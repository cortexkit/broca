//! Wire conformance for the serve adapter — drives `serve_connection` over an in-memory
//! duplex with a fake-executor registry (NO live daemon, NO live LLM). Proves:
//!  - HELLO registers the manifest (correct module_id + ManagementSurface shape);
//!  - route.bind → RouteBindAck;
//!  - session.send / run.status dispatch to the coordinator and return Response frames;
//!  - session.subscribe opens a StreamData* → StreamEnd stream on the held corr;
//!  - egress-closed (client drops the read half mid-stream) tears the subscriber down
//!    cleanly — the serve loop ends without panic/hang.

use crate::store_exec;
use broca_session_store::record::{Placement, Record};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[path = "../common/address_inventory.rs"]
mod address_inventory;
use address_inventory::test_registry;

use async_trait::async_trait;
use broca_core::{ChargeBasis, FileLeaseStore, OutputEvent, Provider, StubProvider, StubToolPlane};
use broca_module::executor::RunExecutor;
use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse, ServeError,
    ServeHandler, SessionImportRequest, SessionImportResponse, SessionReadRequest,
    SessionReadResponse, StatusResponse, SubscribeFrom,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, serve_connection_with_catalog, ServeEnd};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    BindIdentity, FinishReason, MessageId, Usage,
};
use broca_wal::WalRecord;
use subc_protocol::session::{
    HealthStatus, ModuleControlRequest, ModuleControlResponse, MODULE_CONTROL_OP_HEALTH_CHECK,
};
use subc_protocol::{
    BindIdentity as SubcBindIdentity, ErrorBody, Flags, Frame, FrameType, ModuleHelloAckBody,
    ModuleHelloBody, Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Notify};

use crate::common;

// Serialize the only integration test that intentionally drops a stale-route frame with
// health probes that compare process-wide drop counters.
static SERVE_FRAME_DROP_COUNTER_TEST_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

// ── A fake executor that writes a RunStarted then emits a control unit + finishes, so the
//    subscribe stream has events to forward. ──

struct FakeExec {
    started: Arc<AtomicUsize>,
    /// Parks the run between its admission ack and its terminal, so a test can observe a run
    /// that is genuinely IN FLIGHT rather than one that has already ended.
    ///
    /// Without this the executor acks and writes RunFinished microseconds later, so any
    /// assertion about in-flight state is a race against the actor processing the terminal --
    /// and one that passes on a fast platform and fails on a slow one. `Notify` rather than a
    /// channel because a permit sent before the wait is STORED: releasing early cannot lose the
    /// wakeup and hang the run.
    hold: Option<Arc<tokio::sync::Notify>>,
}

/// A tiny real-core executor for the send-id wire test: it runs the actual loop with a
/// one-step stub provider so the second same-send-id request hits the core's same-run-id
/// replay path instead of a fake executor's hand-written outcome.
struct CoreExec {
    reply_text: String,
}

fn one_step_text_reply(text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(11),
                output_tokens: Some(4),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

#[async_trait]
impl RunExecutor for FakeExec {
    /// Every session is kept in the new storage: its run answers "ok" through the new loop,
    /// parked on `hold`, when set, between its admission and its answer.
    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        self.started.fetch_add(1, Ordering::SeqCst);
        let provider = GatedProvider {
            stub: StubProvider::new(vec![one_step_text_reply("ok")]),
            gate: self.hold.clone(),
        };
        store_exec::run_on_store(job, &out, &provider, &StubToolPlane::new(), None).await
    }
}

#[async_trait]
impl RunExecutor for CoreExec {
    /// Every session is kept in the new storage: its run answers `reply_text` through the new
    /// loop.
    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        let provider = StubProvider::new(vec![one_step_text_reply(&self.reply_text)]);
        store_exec::run_on_store(job, &out, &provider, &StubToolPlane::new(), None).await
    }
}

/// Rig whose executor PARKS each run between its ack and its terminal until `hold` fires.
///
/// Exists because asserting on in-flight state against the default rig is a race: the executor
/// acks and finishes microseconds later, so a probe observes a run only if it wins that window.
/// It won on macOS and Linux for weeks and lost on Windows, which is the failure mode of every
/// assertion that depends on losing a race being unlikely rather than impossible.
async fn registry_with_hold(dir: &Path, hold: Arc<tokio::sync::Notify>) -> Arc<LineageRegistry> {
    registry_and_wal_inner(dir, Some(hold)).await.0
}

async fn registry_and_wal(dir: &Path) -> (Arc<LineageRegistry>, Sessions) {
    registry_and_wal_inner(dir, None).await
}

/// The sessions of a test registry, as the new storage keeps them.
struct Sessions(Arc<broca_module::store_open::StoreRuntime>);

/// One record of a session's log.
struct StoredRecord {
    record: Record,
}

/// What each run `session` started was admitted with, and its frozen config, read back from
/// each run's start record and the setup parts it names, in the order the runs started.
async fn starts_in(
    runtime: &broca_module::store_open::StoreRuntime,
    session: &BindIdentity,
) -> Vec<(broca_session_loop::run::Admission, broca_wal::RunConfig)> {
    let Some(class) = runtime.class_of(session).unwrap() else {
        return Vec::new();
    };
    let parts = runtime.parts_for(class, runtime.new_sessions());
    store_exec::records(runtime, session)
        .await
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some((
                broca_session_loop::run::admission_of_start(start, parts).unwrap(),
                broca_session_loop::run::run_config_of_start(start, parts).unwrap(),
            )),
            _ => None,
        })
        .collect()
}

impl Sessions {
    /// What each run `session` started was admitted with, and its frozen config.
    async fn starts(
        &self,
        session: &BindIdentity,
    ) -> Vec<(broca_session_loop::run::Admission, broca_wal::RunConfig)> {
        starts_in(&self.0, session).await
    }

    /// `session`'s records in log order; empty while the session doesn't exist.
    async fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<StoredRecord>, std::convert::Infallible> {
        Ok(store_exec::records(&self.0, session)
            .await
            .into_iter()
            .map(|record| StoredRecord { record })
            .collect())
    }
}

async fn registry_and_wal_inner(
    dir: &Path,
    hold: Option<Arc<tokio::sync::Notify>>,
) -> (Arc<LineageRegistry>, Sessions) {
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(FakeExec {
        started: Arc::new(AtomicUsize::new(0)),
        hold: hold.clone(),
    });
    let runtime = store_exec::store_runtime(dir, exec.clone()).await;
    let opener = DefaultOpener::new(lease, exec)
        .with_store_runtime(runtime.clone())
        .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1)));
    broca_store::session_refs::migrate(
        &dir.join("store.db"),
        &broca_store::session_refs::StoredSessions::new(dir),
    )
    .unwrap();
    let reg = Arc::new(broca_module::LineageRegistry::new(Arc::new(opener)));
    reg.set_session_address_inventory(store);
    (reg, Sessions(runtime))
}

async fn registry(dir: &Path) -> Arc<LineageRegistry> {
    registry_and_wal(dir).await.0
}

async fn registry_with_export(dir: &Path) -> (Arc<LineageRegistry>, Arc<broca_store::SqliteStore>) {
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let exec = Arc::new(FakeExec {
        started: Arc::new(AtomicUsize::new(0)),
        hold: None,
    });
    let exec_opener = exec.clone();
    let resolver = Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1));
    // As the module does: each finished run's spend facts go into the run index's export table.
    let runtime = Arc::new(
        broca_module::store_open::StoreRuntime::open(
            dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            exec as Arc<dyn RunExecutor>,
            Duration::from_millis(100),
        )
        .await
        .unwrap()
        .with_billing(broca_module::store_billing::StoreBilling {
            index: store.clone() as Arc<dyn broca_core::ExportIndex>,
            resolver: resolver.clone(),
        }),
    );
    let opener = DefaultOpener::new(lease, exec_opener)
        .with_store_runtime(runtime)
        .with_charge_basis_resolver(resolver);
    broca_store::session_refs::migrate(
        &dir.join("store.db"),
        &broca_store::session_refs::StoredSessions::new(dir),
    )
    .unwrap();
    let reg = Arc::new(broca_module::LineageRegistry::new(Arc::new(opener)));
    reg.set_session_address_inventory(store.clone());
    (reg, store)
}

/// True when the session holds no records: it doesn't exist (the usual result of a refusal,
/// which writes nothing) or holds none.
async fn no_durable_records(wal: &Sessions, session: &BindIdentity) -> bool {
    wal.read_records(session).await.unwrap().is_empty()
}

struct BlockingSendHandler {
    slow_send_started: Notify,
    release_slow_send: Notify,
    ordered_calls: AtomicUsize,
    first_ordered_send_started: Notify,
    second_ordered_send_started: Notify,
    release_first_ordered_send: Notify,
}

impl BlockingSendHandler {
    fn new() -> Self {
        Self {
            slow_send_started: Notify::new(),
            release_slow_send: Notify::new(),
            ordered_calls: AtomicUsize::new(0),
            first_ordered_send_started: Notify::new(),
            second_ordered_send_started: Notify::new(),
            release_first_ordered_send: Notify::new(),
        }
    }
}

#[async_trait]
impl ServeHandler for BlockingSendHandler {
    async fn send(&self, req: SendRequest) -> Result<SendResponse, ServeError> {
        if req.session.session == "slow" {
            self.slow_send_started.notify_one();
            self.release_slow_send.notified().await;
        } else if req.session.session == "ordered" {
            match self.ordered_calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    self.first_ordered_send_started.notify_one();
                    self.release_first_ordered_send.notified().await;
                }
                1 => self.second_ordered_send_started.notify_one(),
                _ => {}
            }
        }
        Ok(SendResponse::Active {
            run_id: req.run_id.0.clone(),
            delivered: None,
        })
    }

    async fn import(
        &self,
        _req: SessionImportRequest,
    ) -> Result<SessionImportResponse, ServeError> {
        Err(ServeError::Import("test handler does not import".into()))
    }

    async fn retract(
        &self,
        _session: &BindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        Ok(RetractResponse::NotPending)
    }

    async fn subscribe(
        &self,
        _session: &BindIdentity,
        _from: SubscribeFrom,
        _sink: mpsc::Sender<broca_module::Delivery>,
    ) -> Result<(), ServeError> {
        Err(ServeError::Open(
            "test handler does not subscribe".into(),
            None,
        ))
    }

    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        Err(ServeError::Open("test handler does not read".into(), None))
    }

    async fn cancel(
        &self,
        _session: &BindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        Ok(CancelResponse::NotActive)
    }

    async fn status(&self, _session: &BindIdentity) -> Result<StatusResponse, ServeError> {
        Ok(StatusResponse::Idle)
    }

    async fn run_status(
        &self,
        _session: &BindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        Ok(RunStatusResponse::Unknown)
    }
}

// ── A tiny client over the duplex: write request frames, read response frames. ──

async fn write_req<W: AsyncWrite + Unpin>(
    w: &mut W,
    channel: u16,
    corr: u64,
    body: serde_json::Value,
) {
    write_req_epoch(w, channel, u32::from(channel != 0), corr, body).await;
}

async fn write_req_epoch<W: AsyncWrite + Unpin>(
    w: &mut W,
    channel: u16,
    epoch: u32,
    corr: u64,
    body: serde_json::Value,
) {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&body).unwrap(),
    )
    .unwrap();
    write_frame(w, &frame).await.unwrap();
}

async fn read_one<R: AsyncRead + Unpin>(r: &mut R) -> Frame {
    read_frame(r).await.unwrap().expect("a frame")
}

/// A fresh scratch directory for one fixture, never shared with another call.
///
/// The name carries a per-call sequence number, not just the tag, because a helper that
/// several tests use passes the same tag for all of them. With a tag-only name, those tests
/// running in parallel got one directory: each setup deleted it and rebuilt it under the
/// others, so a test failed opening its store, appending to its WAL, or read another test's
/// session markers. The scratch root is already private to this process, so a unique name
/// needs no clean-up of a previous directory before use.
fn tmpdir(tag: &str) -> std::path::PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let call = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-serve-{}-{tag}-{call}", std::process::id()));
    std::fs::create_dir_all(dir.join("proj")).unwrap();
    dir
}

/// Answers `vault.audit` with a fixed result and counts the calls.
struct FixedAuditor {
    calls: std::sync::atomic::AtomicUsize,
}

impl broca_module_serve::vault_audit::VaultAuditor for FixedAuditor {
    fn audit(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = broca_module_serve::wire::VaultAuditResponse>
                + Send
                + '_,
        >,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {
            broca_module_serve::wire::VaultAuditResponse::Checked {
                principal: "reserved:broca".into(),
                view: "digest".into(),
                rows: Vec::new(),
                missing: vec![broca_module_serve::wire::VaultAuditMissing {
                    credential_id: "apikey:kimi-for-coding".into(),
                    referenced_by: vec!["apikey:kimi-code-plan-global".into()],
                }],
            }
        })
    }
}

/// `vault.audit` is served by the daemon's auditor, is listed in the manifest as a query, and
/// opens no session.
#[tokio::test]
async fn vault_audit_is_answered_by_the_daemon_auditor_without_opening_a_session() {
    let dir = tmpdir("vault-audit");
    let (reg, _store) = registry_with_export(&dir).await;
    let auditor = Arc::new(FixedAuditor {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve_auditor: Arc<dyn broca_module_serve::vault_audit::VaultAuditor> = auditor.clone();
    let serve = tokio::spawn(async move {
        let _ = broca_module_serve::serve_connection_configured(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
            None,
            broca_module_serve::consumer_routes::ConsumerRoutes::default(),
            Some(serve_auditor),
            None,
            None,
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_text = String::from_utf8_lossy(&hello.body).to_string();
    assert!(
        hello_text.contains("\"vault.audit\""),
        "the HELLO manifest declares vault.audit"
    );
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "audit"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        11,
        serde_json::json!({"method": "vault.audit"}),
    )
    .await;
    let audit = read_one(&mut creader).await;
    assert_eq!(audit.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(body["result"]["status"], "checked");
    assert_eq!(
        body["result"]["missing"][0]["credential_id"],
        "apikey:kimi-for-coding"
    );
    assert_eq!(auditor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(reg.slot_counts().open, 0, "vault.audit opens no session");

    // An option the op does not have is refused, not ignored.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({"method": "vault.audit", "params": {"verbose": true}}),
    )
    .await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_eq!(auditor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `memory.report` is listed in the manifest as a query, answers the operator's own connection
/// with the registry's report, refuses any other principal, refuses an option it doesn't have,
/// and opens no session.
#[tokio::test]
async fn memory_report_answers_only_the_operator_and_opens_no_session() {
    let dir = tmpdir("memory-report");
    let (reg, _store) = registry_with_export(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });
    let hello = read_one(&mut creader).await;
    assert!(
        String::from_utf8_lossy(&hello.body).contains("\"memory.report\""),
        "the HELLO manifest declares memory.report"
    );
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();
    for (channel, principal) in [(7, None), (8, Some(subc_protocol::Principal::Direct))] {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 2,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", "memory"),
            principal,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        };
        write_req(
            &mut cwriter,
            0,
            10 + u64::from(channel),
            serde_json::to_value(bind).unwrap(),
        )
        .await;
        assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);
    }
    // A connection that isn't the operator's is refused.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        21,
        serde_json::json!({"method": "memory.report"}),
    )
    .await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert!(String::from_utf8_lossy(&refused.body).contains("operator_required"));
    // The operator's is answered with the registry's report: nothing open here.
    write_req_epoch(
        &mut cwriter,
        8,
        2,
        22,
        serde_json::json!({"method": "memory.report"}),
    )
    .await;
    let answered = read_one(&mut creader).await;
    assert_eq!(answered.header.ty, FrameType::Response);
    let body: serde_json::Value = serde_json::from_slice(&answered.body).unwrap();
    assert_eq!(body["result"]["sessions"], serde_json::json!([]));
    assert_eq!(body["result"]["legacy_sessions"], 0);
    assert!(body["result"]["settled_runs"]["max_entries"].is_u64());
    // An option the op does not have is refused, not ignored.
    write_req_epoch(
        &mut cwriter,
        8,
        2,
        23,
        serde_json::json!({"method": "memory.report", "params": {"verbose": true}}),
    )
    .await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Error);
    assert_eq!(reg.slot_counts().open, 0, "memory.report opens no session");
    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn retired_usage_export_is_unknown_and_terminal_facts_remain() {
    let dir = tmpdir("usage-export");
    let (reg, store) = registry_with_export(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "export");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    // The retired method follows the ordinary unknown-method refusal.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        11,
        serde_json::json!({"method": "usage.export", "params": {"limit": 10}}),
    )
    .await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(body["code"], "unknown_method");
    assert_eq!(body["message"], "unknown method 'usage.export'");

    // A contract op of a group broca doesn't declare is known, so it is refused `unsupported`:
    // promotion needs `send_promote`, which broca doesn't declare.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({
            "method": "session.promote",
            "params": {"send_id": "missing", "request_id": "unsupported"},
        }),
    )
    .await;
    let promote = read_one(&mut creader).await;
    assert_eq!(promote.header.ty, FrameType::Error);
    let promote: serde_json::Value = serde_json::from_slice(&promote.body).unwrap();
    assert_eq!(promote["code"], "unsupported", "{promote}");

    // A broca without the vault still answers vault.audit, and says it could not check,
    // rather than failing the request or answering an empty (clean-looking) audit.
    write_req_epoch(
        &mut cwriter,
        7,
        2,
        15,
        serde_json::json!({"method": "vault.audit", "params": {}}),
    )
    .await;
    let audit = read_one(&mut creader).await;
    assert_eq!(audit.header.ty, FrameType::Response);
    let audit_body: serde_json::Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(audit_body["result"]["status"], "unanswered");
    assert_eq!(reg.slot_counts().open, 0, "vault.audit is daemon-scoped");

    write_req_epoch(
        &mut cwriter,
        7,
        2,
        12,
        serde_json::json!({
            "method": "session.send",
            "params": {"prompt": "hi", "model": {"provider": "fake", "model": "fake"}}
        }),
    )
    .await;
    let send = read_one(&mut creader).await;
    assert_eq!(send.header.ty, FrameType::Response);

    let mut page = None;
    broca_test_support::wait_for(
        "retired_usage_export_is_unknown_and_terminal_facts_remain",
        async {
            loop {
                let candidate = store.read_export_page(None, 10).unwrap();
                if !candidate.facts.is_empty() {
                    page = Some(candidate);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "export_facts={:?}",
                store.read_export_page(None, 10).map(|p| p.facts.len())
            )
        },
    )
    .await;
    let page = page.expect("terminal emission should publish one usage fact");
    assert_eq!(page.facts.len(), 1);
    assert_eq!(
        page.facts[0].segment.as_session().unwrap().charge_basis,
        ChargeBasis::Unknown
    );
    assert_eq!(page.facts[0].segment.as_session().unwrap().session, session);
    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `cap.install` and `spend.delta` were removed: any caller the daemon admitted could call them,
/// and nothing recorded who installed a cap, so any local module could remove a cap or report
/// spend against one. A call to either name, with the params the operations used to accept, must
/// get the same unknown-method refusal as any other name broca does not serve, open no session
/// actor, write nothing to the session's WAL, and create no `caps.db`.
#[tokio::test]
async fn removed_spend_cap_operations_are_unknown_methods_that_write_nothing() {
    let dir = tmpdir("removed-cap-ops");
    let (reg, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_reg = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_reg.clone(),
            serve_reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "removed-cap-ops");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    // Params in the exact shape each operation used to accept, so the refusal cannot come from
    // a params decoder rejecting a malformed body.
    let watermark = |seq: u64| serde_json::json!({"source_instance_id": "source-a", "source_epoch": 1, "consumed_export_seq": seq});
    let calls = [
        (
            "cap.install",
            serde_json::json!({
                "cap_id": "cap-org-x",
                "cap_version": 1,
                "op": "remove",
                "scope_key": "runner",
                "project_root": session.project_root.display().to_string(),
                "constraint_dim": "spend_dollars",
                "period": "calendar_month",
                "period_tz": "Europe/Madrid",
                "hard_cap_nanos": 10,
                "max_staleness_ms": 60000,
                "replaces_cap_id": null,
                "baseline": {
                    "spend_nanos": 0,
                    "unresolved_count": 0,
                    "watermark": watermark(0),
                    "period_bucket": "2026-07",
                    "as_of_ms": 1789000000000i64
                }
            }),
        ),
        (
            "spend.delta",
            serde_json::json!({
                "cap_id": "cap-org-x",
                "cap_version": 1,
                "from_watermark": watermark(0),
                "to_watermark": watermark(1),
                "priced_delta_nanos": 5,
                "unresolved_delta": 0,
                "period_bucket": "2026-07"
            }),
        ),
    ];
    for (corr, (method, params)) in (11..).zip(calls) {
        write_req_epoch(
            &mut cwriter,
            7,
            2,
            corr,
            serde_json::json!({"method": method, "params": params}),
        )
        .await;
        let refused = read_one(&mut creader).await;
        assert_eq!(refused.header.ty, FrameType::Error, "{method}");
        assert_eq!(refused.header.corr, corr, "{method}");
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "unknown_method", "{method}");
        assert_eq!(
            body["message"],
            format!("unknown method '{method}'"),
            "{method}"
        );
    }

    assert_eq!(reg.slot_counts().open, 0, "a refused call opens no session");
    assert!(
        no_durable_records(&wal, &session).await,
        "a refused call writes nothing to the session's WAL"
    );
    assert!(
        !dir.join("caps.db").exists(),
        "nothing creates the old cap table"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn bind_ack_precedes_route_egress_and_stale_epoch_is_silent() {
    let _drop_counter_guard = SERVE_FRAME_DROP_COUNTER_TEST_LOCK.lock().await;
    let dir = tmpdir("bind-order-epoch");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "ordered");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness, session.session),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    let bind_ack = read_one(&mut creader).await;
    assert_eq!(bind_ack.header.ty, FrameType::Response);
    assert_eq!((bind_ack.header.channel, bind_ack.header.epoch), (0, 0));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), read_one(&mut creader))
            .await
            .is_err(),
        "route.bind must not emit route traffic before or after its ack"
    );

    write_req_epoch(
        &mut cwriter,
        7,
        1,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "stale", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), read_one(&mut creader))
            .await
            .is_err(),
        "a stale route generation must not receive a response"
    );
    assert_eq!(handler.ordered_calls.load(Ordering::SeqCst), 0);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn separator_collision_bind_is_rejected_before_any_durable_state() {
    let dir = tmpdir("bind-separator-collision");
    let registry = registry(&dir).await;
    let colliding = BindIdentity::new("broca-unscoped-v1\u{1f}runner", "a", "b");
    let invalid = BindIdentity::new("broca-unscoped-v1", "runner\u{1f}a", "b");
    let address = broca_core::session_addr(&invalid);
    assert_ne!(colliding, invalid);
    assert_eq!(broca_core::session_addr(&colliding), address);

    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(invalid.harness.clone(), invalid.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    let bind_reply = read_one(&mut creader).await;

    // Keep the test non-vacuous: without the serve-side validation, the bind is acknowledged;
    // this status request then opens the actor and creates the colliding WAL before the failure.
    if bind_reply.header.ty == FrameType::Response {
        write_req_epoch(
            &mut cwriter,
            7,
            2,
            11,
            serde_json::json!({"method": "run.status", "params": {}}),
        )
        .await;
        let _ = read_one(&mut creader).await;
        panic!("ambiguous bind was accepted and opened colliding durable state at {address}");
    }

    assert_eq!(bind_reply.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&bind_reply.body).unwrap();
    assert_eq!(error.code, "invalid_bind_identity");
    assert!(error.message.contains("U+001F"));
    assert!(
        broca_wal::session_storage_class(&dir, &address)
            .unwrap()
            .is_none(),
        "rejected bind must not create a session"
    );
    let counts = registry.slot_counts();
    assert_eq!((counts.open, counts.opening, counts.closing), (0, 0, 0));
    let lease_root = dir.join("leases");
    assert!(
        !lease_root.exists() || std::fs::read_dir(&lease_root).unwrap().next().is_none(),
        "rejected bind must not create a lease"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

async fn wait_for_run_state<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    channel: u16,
    run_id: &str,
    expected: &str,
    first_corr: u64,
) -> serde_json::Value {
    // Bounded by elapsed time, not by a poll count. The run's progress is real file and SQLite
    // work, and on a loaded host a run that needs a second or two can need many times that; a
    // fixed 40 polls failed healthy runs there. The last state seen goes into the failure, so a
    // run that is stuck reads differently from one that is merely slow.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut offset = 0;
    let mut last = serde_json::Value::Null;
    while tokio::time::Instant::now() < deadline {
        write_req(
            writer,
            channel,
            first_corr + offset,
            serde_json::json!({
                "method": "run.status",
                "params": { "run_id": run_id }
            }),
        )
        .await;
        offset += 1;
        let resp = read_one(reader).await;
        assert_eq!(
            resp.header.ty,
            FrameType::Response,
            "run.status reply: {}",
            String::from_utf8_lossy(&resp.body)
        );
        let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        if v["result"]["state"] == expected {
            return v["result"].clone();
        }
        last = v["result"].clone();
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run {run_id} never reached {expected} status within 30 s; last status: {last}");
}

async fn wait_for_completed_run<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    channel: u16,
    run_id: &str,
    first_corr: u64,
) -> serde_json::Value {
    wait_for_run_state(reader, writer, channel, run_id, "completed", first_corr).await
}

/// A terminal run can remain active until the actor processes executor completion. A fresh
/// send that must start immediately needs the session's idle status, not only that terminal.
async fn wait_for_session_idle<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    channel: u16,
    first_corr: u64,
) {
    let transport = tokio::sync::Mutex::new((reader, writer, first_corr));
    broca_test_support::wait_until("session is idle with no active run", || async {
        let mut transport = transport.lock().await;
        let (reader, writer, corr) = &mut *transport;
        write_req(
            writer,
            channel,
            *corr,
            serde_json::json!({
                "method": "run.status", "params": {}
            }),
        )
        .await;
        *corr += 1;
        let response = read_one(reader).await;
        assert_eq!(response.header.ty, FrameType::Response);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        if body["result"]["state"] == "idle" && body["result"].get("run_id").is_none() {
            Ok(())
        } else {
            Err(format!("session status: {body}"))
        }
    })
    .await;
}

/// HELLO + route.bind + session.send with a system prompt writes a RunStarted record whose
/// input leads with Role::System and preserves the exact text bytes.
#[tokio::test]
async fn hello_bind_send_with_system_writes_leading_system_message() {
    let dir = tmpdir("system");
    let (reg, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let system = "  system α\nline δύο 🧪  ";
    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "hi",
                "system": system,
                "model": { "provider": "fake", "model": "fake" }
            }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(v["result"]["state"], "active");

    // The run's input as its records keep it: the send's system text leads, then its prompt.
    let run_started: Vec<Message> = wal
        .read_records(&session)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|stored| match stored.record {
            Record::Input(input) if input.placement == Placement::RunInput => {
                Some((*input.message).clone())
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        run_started,
        vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: system.into(),
                }],
            ),
            Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }],),
        ]
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// `seed_from` names a future lineage-fork operation, so v1 must reject it instead of silently
/// starting an unrelated fresh episode.
#[tokio::test]
async fn session_send_rejects_reserved_seed_from_before_starting_a_run() {
    let dir = tmpdir("seed-from-rejected");
    let (registry, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "seed-from");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "fork from the earlier lineage",
                "model": { "provider": "fake", "model": "fake" },
                "seed_from": {
                    "source_lineage": "runner:source",
                    "at_seq": 42
                }
            }
        }),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(response.header.ty, FrameType::Error);
    let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(error.code, "unsupported_seed_from");
    assert!(no_durable_records(&wal, &session).await);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// An unknown `context_management` value is refused on the wire before anything runs.
///
/// `SendParams` tolerates unknown FIELDS, so this is the one place a too-lenient field VALUE
/// would slip through: reading an unknown value as `"none"` would run the session unmanaged
/// against a window its caller believed was managed.
#[tokio::test]
async fn session_send_refuses_an_unknown_context_management_value_before_starting_a_run() {
    let dir = tmpdir("context-management-unknown");
    let (registry, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "context-management");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("summarize")),
        (12, serde_json::json!({"reduce": "magic-context"})),
        (13, serde_json::json!({"transform": 7})),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "manage my context",
                    "model": { "provider": "fake", "model": "fake" },
                    "context_management": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert!(
            error.message.contains("context_management must be")
                && error.message.contains(&value.to_string()),
            "the refusal names the field and the value: {error:?}"
        );
    }
    assert!(
        no_durable_records(&wal, &session).await,
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// An unknown `service_tier` value, `"fast"` included, is refused on the wire before anything
/// runs, naming the three accepted tiers. `fast` is OpenAI's marketing name for priority and is
/// deliberately not an alias.
#[tokio::test]
async fn session_send_refuses_an_unknown_service_tier_before_starting_a_run() {
    let dir = tmpdir("service-tier-unknown");
    let (registry, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "service-tier");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("fast")),
        (12, serde_json::json!("auto")),
        (13, serde_json::json!("PRIORITY")),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "go fast",
                    "model": { "provider": "fake", "model": "fake" },
                    "service_tier": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert!(
            error
                .message
                .contains("service_tier must be \"flex\", \"default\" or \"priority\"",)
                && error.message.contains(&value.to_string()),
            "the refusal names the field and the value: {error:?}"
        );
    }
    assert!(
        no_durable_records(&wal, &session).await,
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// Health reports lifetime projection failures, including failures from earlier
// tests in this binary. Re-execute probes of a healthy process with fresh metrics
// instead of resetting production counters or relaxing their exact assertions.
async fn isolated_health_probe(test: &str) -> bool {
    const CHILD_TEST: &str = "BROCA_ISOLATED_HEALTH_TEST";
    if std::env::var(CHILD_TEST).as_deref() == Ok(test) {
        return true;
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(CHILD_TEST, test)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("isolated health probe must finish within 30s")
    .expect("spawn isolated health probe");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "isolated health probe {test} failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

mod keepalive_health;

/// A channel-0 health.check returns registry counts and dependency states from memory.
#[tokio::test]
async fn health_check_reports_slot_counts_and_stale_lkg_without_degrading() {
    if !isolated_health_probe(
        "wire::health_check_reports_slot_counts_and_stale_lkg_without_degrading",
    )
    .await
    {
        return;
    }
    let _drop_counter_guard = SERVE_FRAME_DROP_COUNTER_TEST_LOCK.lock().await;
    // The serve binary publishes the table it loaded before serving; this harness has no
    // config, so it publishes the shipped table the same way.
    broca_module_serve::ChargeBasisTable::shipped_defaults().publish_for_health();
    let dir = tmpdir("health");
    // The run is HELD open across the health probe rather than left to finish on its own, so
    // runs_in_flight is observed while a run genuinely is in flight.
    let hold = Arc::new(tokio::sync::Notify::new());
    let reg = registry_with_hold(&dir, hold.clone()).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let resolved_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 5_000;
    let catalog_cache =
        common::snapshot_cache(resolved_at_ms, broca_catalog::CatalogSource::LastKnownGood);

    let serve = tokio::spawn(async move {
        let _ = serve_connection_with_catalog(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
            Some(catalog_cache),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    // Capture the process-wide values before this rig sends any requests. A sibling test
    // may already have dropped a frame, so this test compares its two health readings rather
    // than assuming a fresh process starts at zero.
    write_req(
        &mut cwriter,
        0,
        9,
        serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let initial_health = read_one(&mut creader).await;
    assert_eq!(initial_health.header.ty, FrameType::Response);
    let initial_health: ModuleControlResponse =
        serde_json::from_slice(&initial_health.body).unwrap();
    let ModuleControlResponse::HealthCheck {
        metrics: Some(initial_metrics),
        ..
    } = initial_health
    else {
        panic!("expected initial health.check metrics");
    };
    let initial_metrics = initial_metrics
        .as_object()
        .expect("initial health.check metrics are a JSON object");
    let unbound_drops_before = initial_metrics["serve_frames_dropped_unbound_channel"]
        .as_u64()
        .expect("initial health.check includes the unbound-channel counter");
    let stale_drops_before = initial_metrics["serve_frames_dropped_stale_epoch"]
        .as_u64()
        .expect("initial health.check includes the stale-epoch counter");
    let discovery_before = initial_metrics["tool_keepalive_discovery"].clone();
    keepalive_health::record_failed_lookup().await;

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);

    write_req(
        &mut cwriter,
        0,
        12,
        serde_json::to_value(&ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let health: ModuleControlResponse = serde_json::from_slice(&resp.body).unwrap();
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = health
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_ne!(status, HealthStatus::Degraded);
    assert_eq!(detail, None);
    let metrics = metrics.expect("health.check includes metrics");
    let metrics = metrics
        .as_object()
        .expect("health.check metrics are a JSON object");
    // EXACT SET, not a contains-check, and the direction it adds is the load-bearing one.
    //
    // A `contains_key` list catches a field being REMOVED and cannot catch one being ADDED
    // unpinned -- which is the failure that actually happens here. The key list gets written
    // when the first field lands; later fields arrive in commits focused on their own logic,
    // and nothing sends the author back to this test. So the fields added LAST are the fields
    // nobody pins, and those are disproportionately the incident-reading ones, because
    // observability fields are added late by nature. Both catalog refusal gauges reached the
    // wire that way and were unasserted until an unrelated review found them.
    //
    // With an exact set, adding a metric fails here by name and forces the author to decide
    // what it should read on a healthy daemon at the moment of adding it.
    let expected: std::collections::BTreeSet<&str> = [
        // Identity of the serving process, for "did the process holding my work restart".
        // Pinned in the exact set like every other key: a health field the fleet reads during
        // an incident must not appear or vanish without a test saying so.
        "launch_id",
        "active_sessions",
        "opening",
        "closing",
        "vault",
        // This library-only rig publishes no new-storage slot or deployment key. The metric
        // remains present as null, without degrading the healthy process.
        "store",
        // This rig's daemon doesn't manage broca's settings (broca reads its own file), so
        // `settings` is present as null.
        "settings",
        // No converter engine is published by this rig; its health remains present and idle.
        "catalog_age_ms",
        "catalog_source",
        // Runs executing right now, as opposed to open session slots. Shipped in v0.3.27 for
        // SUBC's restart gate and unpinned until the exact-set fence below found it -- the same
        // added-late-so-nobody-pins-it path as the refusal gauges.
        "runs_in_flight",
        // Present at zero for the same reason as the drop counters: absent-versus-zero is
        // the distinction an operator needs, and a field that appears only on failure
        // cannot say "this process has projected cleanly since boot".
        // The current-state counterpart: unlike the lifetime counter, successful projection
        // removes only the recovered session's entry and leaves other sessions behind.
        // The ck-bus run-event publisher: off by default, so a healthy default daemon reads
        // `enabled: false` with every counter at zero; `enabled` is what separates that from an
        // enabled publisher that has simply had nothing to send.
        "bus_events",
        // Process-local lifetime operation counts. The isolated operation_counters target proves
        // their zero and non-zero readings; this test keeps their presence in the total key set.
        "stream_chunks",
        "display_index_resets",
        "session_reads",
        "session_readers",
        "actor_opens",
        // Views of closed sessions too large for the view cache, counted from zero.
        "views_over_cap",
        "management_ops",
        // The subscriber fan-out's counters, pinned in the exact set. The fan-out had
        // NO counters before -- not "only failure counters", which is what I first
        // wrote; frames_dropped_* above belong to broca-subc's client socket.
        // display_gaps is the loss signal, kept separate so deliveries cannot rise
        // when the lane drops frames.
        "control_deliveries",
        "display_deliveries",
        "display_gaps",
        // Present even at zero. A drop counter that appears only when non-zero cannot tell a
        // connection losing nothing from a gauge that was never wired, so the healthy reading
        // would be the one indistinguishable from a missing instrument.
        "frames_dropped_stale_epoch",
        "frames_dropped_unmatched_terminal",
        "reverse_requests_refused",
        "tool_keepalive_discovery",
        // The preceding stale-epoch field belongs to broca-subc's client-side outbound routes;
        // these two are the serve loop's process-side inbound frame drops.
        "serve_frames_dropped_unbound_channel",
        "serve_frames_dropped_stale_epoch",
        // BOTH refusal fields, and the pair is the point. The streak is a current-state gauge
        // that resets on the first success; the total survives recovery. Streak zero with a
        // non-zero total is "occurred and recovered", which neither field can say alone.
        //
        // Listed together because a check on one half of a pair is not a check on the other,
        // and these two fail in different vocabularies: a missing total reads as absent, while
        // a total that wrongly resets reads as zero. FUSI hit exactly that -- their fence caught
        // one half of a two-field adoption and was blind to the other.
        "catalog_refusals",
        "catalog_refusals_total",
        // Present rows of the served catalog that could not be used and were left out or kept
        // at their previous model. A healthy catalog reads 0 with a null reason; a bad row
        // shows here rather than as a refusal, because the rest of the catalog still refreshed.
        "catalog_rows_excluded",
        "catalog_rows_excluded_reason",
        // The charge-basis table new facts are resolved against, as the facts carry it, and
        // how many local override rows make it up. A deployment with no override reads
        // "none" and 0; an override swap check compares these instead of scraping logs.
        "charge_basis_overrides",
        "charge_basis_override_rows",
        // Entries providers listed as changes they made to a request's input (for Anthropic,
        // thinking blocks dropped because an earlier part of the conversation changed). Present
        // at zero so a daemon that stopped counting cannot pass for a healthy one.
        "input_transformations",
        // Attempts sent without asking the provider for that report (every Claude Code persona
        // request), so their missing report means "not measured" rather than zero. Present at
        // zero, so the pair can tell "nothing transformed" from "nothing asked".
        "input_transformations_unmeasured_attempts",
        // How each new run's provider account was chosen and how its reports to routing
        // fared. Null, not an empty object, in a process that asks routing nothing (no vault
        // credential source, as here); its counters are read through a real daemon in
        // account_selection_real_daemon.
        "account_selection",
        // Refusals ended because their computed reset/retry deadline was already before
        // broca's pause time. This healthy daemon has seen none; assert zero below.
        "account_limit_stale_resets",
        "account_limit_pause_declined",
        "account_limit_hold_cap_unknown",
        "refusal_schedule_unrecorded_replays",
        "stall_watchdog",
        // Retained sessions whose expiry could not be confirmed because their log is unreadable
        // and the index is not provably current, so deletion keeps them. Present at zero.
        "retention_unreadable_sessions",
        "agent_data_ops",
        // Decision-model batches currently calling a provider. It is one of the gauges the
        // daemon's restart drain waits on, so an idle daemon must read 0 here or every restart
        // would wait out its drain ceiling.
        "system_one_batches_in_flight",
        "system_one",
        // Private-content verdicts and marked-call outcomes for the llm_classify route. Null
        // here because this rig serves no model-tool route; the counters themselves are
        // asserted through the route in llm_tools_consent.
        "llm_tools_consent",
        // Scoped sessions counted by whether the daemon-stamped workdir matches the resolved
        // storage root. Every route.bind request in this probe omits the scope stamp;
        // assert all four counters and no dedupe-set overflow below.
        "scoped_session_workdirs",
        // No scoped provider route opens in this healthy probe, so no refusal codes were counted.
        "scoped_role_route_refusals",
        // A complete inventory and valid session identities refuse no binds in this isolated
        // healthy process. Pin all reason counters at zero below, not just their presence.
        "session_address_refusals",
        // Provider-held calls: whether the late-results intake still needs its verified rebuild,
        // what that rebuild has scanned, and tool.withdraw outcomes. Asserted below: a root that
        // never held a call needs no rebuild, scans nothing, and has no failed or pending
        // withdrawal.
        "provider_held",
        // This process's WAL fsyncs by cause. Asserted below: its four counters exist, and a
        // writer that lost its channel without a shutdown (`disconnect`) never happens here.
        // Cached tool routes and the route Goodbyes sent when sessions close. Asserted below:
        // no route outlives its session, and no Goodbye failed, was dropped or is still owed.
        "tool_routes",
        // Quota readings forwarded to insula. This probe makes no subscription model call, so
        // asserted below: nothing sent, failed or dropped, and no reading insula ignored.
        "usage_observe",
    ]
    .into_iter()
    .collect();
    let actual: std::collections::BTreeSet<&str> = metrics.keys().map(String::as_str).collect();
    let discovery = &metrics["tool_keepalive_discovery"];
    assert_eq!(
        discovery["probes"].as_u64().unwrap(),
        discovery_before["probes"].as_u64().unwrap() + 1
    );
    assert_eq!(
        discovery["lookup_failed"].as_u64().unwrap(),
        discovery_before["lookup_failed"].as_u64().unwrap() + 1
    );
    assert_eq!(
        discovery["lookup_unfinished"],
        discovery_before["lookup_unfinished"]
    );
    assert_eq!(*discovery, broca_fetch_plan::keepalive_discovery_health());
    assert_eq!(
        actual, expected,
        "health.check metric keys drifted from what this test pins. A NEW key here is not a \
         failure to fix by adding it to the list -- decide what it must read on a healthy \
         daemon and assert that below, because an unasserted metric is one the fleet reads \
         during an incident and nothing defends."
    );
    assert_eq!(
        metrics["scoped_role_route_refusals"],
        serde_json::json!({}),
        "a process with no refused scoped role opens has an empty refusal-code map"
    );
    assert_eq!(
        metrics["store"],
        serde_json::Value::Null,
        "a process without a published new-storage slot reports no deployment-key state"
    );
    assert_eq!(
        metrics["settings"],
        serde_json::Value::Null,
        "a process serving without the settings gate reports no daemon settings view"
    );
    assert_eq!(
        metrics["usage_observe"],
        serde_json::json!({
            "rows_sent": 0,
            "batches_sent": 0,
            "transport_failures": 0,
            "rows_dropped": 0,
            "ignored": {},
        }),
        "a process that made no subscription call forwards no quota reading and loses none"
    );
    let readers = metrics["session_readers"]
        .as_object()
        .expect("reader census is present");
    assert!(readers["since_ms"].as_i64().is_some_and(|ms| ms > 0));
    assert!(readers["overflow"].is_u64());
    for entry in readers["entries"]
        .as_array()
        .expect("reader census entries")
    {
        assert!(entry["count"].is_u64());
        assert!(entry["last_ms"].is_i64());
        assert!(entry["principal"].is_string());
        assert!(entry["op"].is_string());
        assert!(entry["owner"].is_boolean() || entry["owner"].is_null());
    }
    assert_eq!(
        metrics["account_limit_stale_resets"],
        serde_json::json!(0),
        "the isolated healthy daemon has not ended a refusal for stale account timing"
    );
    assert_eq!(
        metrics["account_limit_hold_cap_unknown"],
        serde_json::json!(0)
    );
    assert_eq!(
        metrics["account_limit_pause_declined"],
        serde_json::json!({
            "disabled":0, "no_accepted_response":0, "cache_expires_first":0, "beyond_cap":0
        })
    );
    assert_eq!(
        metrics["refusal_schedule_unrecorded_replays"],
        serde_json::json!(0),
        "the isolated healthy daemon has not reconstructed an old refusal seed"
    );
    // This in-process adapter has no daemon bootstrap or watchdog file. The real
    // binary enables the witness during boot; a library-only connection says so.
    assert_eq!(
        metrics["stall_watchdog"],
        serde_json::json!({"enabled": false})
    );
    // Asserted as a NON-EMPTY STRING rather than a literal: the value is per-launch, so
    // pinning it would pin the pid of whichever process ran the test. What must hold is that
    // it is present, non-empty, and carries the two components a caller compares.
    let launch = metrics["launch_id"]
        .as_str()
        .expect("launch_id is a string");
    assert!(
        launch.contains('-') && !launch.starts_with('-') && !launch.ends_with('-'),
        "launch_id must carry both nonce components, got {launch:?}"
    );
    let routes = metrics["tool_routes"]
        .as_object()
        .expect("tool_routes is an object");
    assert_eq!(
        routes.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "cached_routes",
            "cached_routes_per_provider",
            "goodbye_failures_since_boot",
            "goodbye_giveups_since_boot",
            "goodbye_retry_overflows_since_boot",
            "oldest_route_idle_secs",
            "open_actor_reasons",
            "open_session_actors",
            "pending_goodbyes",
            "routes_for_closed_sessions",
        ],
        "tool_routes carries the route and Goodbye counters"
    );
    // A route still cached after its session closed is the leak these counters exist to
    // expose, and on a healthy daemon every Goodbye was delivered: none failed, none overflowed
    // the retry queue, none was given up, and none is still waiting.
    for healthy_zero in [
        "routes_for_closed_sessions",
        "goodbye_failures_since_boot",
        "goodbye_retry_overflows_since_boot",
        "goodbye_giveups_since_boot",
        "pending_goodbyes",
    ] {
        assert_eq!(
            routes[healthy_zero],
            serde_json::json!(0),
            "tool_routes.{healthy_zero} on a healthy daemon: {routes:?}"
        );
    }
    assert_eq!(
        metrics["session_address_refusals"],
        serde_json::json!({"incomplete_inventory":0,"gap_marker":0,"store_error":0,"wait_expired":0,"migration_failed":0,"ambiguous":0,"invalid_identity":0,"session_log_missing":0,"repairs":{"started":0,"completed":0,"failed":0}}),
        "a healthy inventory and valid identities must not refuse a session address"
    );
    let workdirs = metrics["scoped_session_workdirs"]
        .as_object()
        .expect("scoped_session_workdirs is an object");
    assert_eq!(
        workdirs.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "sessions_differ",
            "sessions_same",
            "sessions_untracked",
            "sessions_without_workdir"
        ],
        "scoped_session_workdirs carries one counter per outcome"
    );
    // This rig binds without a daemon stamp, so no session was compared. A non-zero
    // `sessions_untracked` on any daemon means the dedupe set overflowed and counts are lost.
    assert_eq!(
        metrics["scoped_session_workdirs"],
        serde_json::json!({
            "sessions_differ": 0,
            "sessions_same": 0,
            "sessions_untracked": 0,
            "sessions_without_workdir": 0,
        })
    );
    // `rebuild_required: true` would mean late results may be missing from the intake, and a
    // failed withdrawal needs attention. Both degrade health; neither can happen on a root
    // that never held a call. A scope-gone refusal is reported without degrading health, and
    // its counter must also be zero on this healthy, never-held root. A root that never
    // held a call also has no boot withdrawal sweep (nothing running, no rows left to recover)
    // and no late-results entry pinned past the five-minute warning.
    assert_eq!(
        metrics["provider_held"],
        serde_json::json!({
            "rebuild_required": false,
            "fence": null,
            "scanned": 0,
            "decoded": 0,
            "registered": 0,
            "unreadable": 0,
            "withdrawals": {
                "failed": 0,
                "pending": 0,
                "scope_gone": 0,
                "sweep_running": false,
                "rows_remaining": 0,
            },
            "overdue_pins": 0,
            "late_results_delivery_stuck": [],
        })
    );
    assert_eq!(metrics["active_sessions"], serde_json::json!(1));
    assert_eq!(metrics["opening"], serde_json::json!(0));
    assert_eq!(metrics["closing"], serde_json::json!(0));
    assert_eq!(
        metrics["system_one_batches_in_flight"],
        serde_json::json!(0)
    );
    assert_eq!(metrics["vault"], serde_json::json!("disconnected"));
    assert_eq!(
        metrics["bus_events"],
        serde_json::json!({
            "enabled": false,
            "bus_events_published": 0,
            "bus_outbox_depth": 0,
            "compaction_notice_outbox_pending": 0,
            "bus_publish_failures": 0,
            "bus_events_oversize": 0,
            "bus_outbox_dropped": 0,
            // The liveness readings an enabled publisher moves. This rig leaves
            // `bus_run_events_enabled` off, so no publisher loop runs and the pass age is null
            // rather than a number that could look healthy, and no session's run events reach
            // the outbox.
            "bus_account_known": false,
            "bus_store_batches_recorded": 0,
            "bus_store_batches_waiting": 0,
            "bus_store_batches_dropped": 0,
            "bus_loop_pass_age_ms": null,
        })
    );
    assert!(metrics["catalog_age_ms"].as_i64().unwrap() >= 5_000);
    assert_eq!(metrics["catalog_source"], serde_json::json!("lkg"));
    assert_eq!(metrics["catalog_refusals"], serde_json::json!(0));
    assert_eq!(metrics["catalog_refusals_total"], serde_json::json!(0));
    assert_eq!(metrics["catalog_rows_excluded"], serde_json::json!(0));
    assert_eq!(
        metrics["catalog_rows_excluded_reason"],
        serde_json::Value::Null
    );
    assert_eq!(metrics["charge_basis_overrides"], serde_json::json!("none"));
    assert_eq!(metrics["charge_basis_override_rows"], serde_json::json!(0));
    // Zero is the healthy reading: this rig's provider reports no transformations, and any
    // other number means some request was not an append-only extension of the one before.
    assert_eq!(metrics["input_transformations"], serde_json::json!(0));
    assert_eq!(
        metrics["input_transformations_unmeasured_attempts"],
        serde_json::json!(0)
    );
    assert_eq!(metrics["account_selection"], serde_json::Value::Null);
    assert_eq!(metrics["llm_tools_consent"], serde_json::Value::Null);
    // Zero on a healthy daemon: any other number is a retained session kept past its expiry.
    // The count belongs to this registry's retention service, so other tests cannot raise it.
    assert_eq!(
        metrics["retention_unreadable_sessions"],
        serde_json::json!(0)
    );
    assert_eq!(
        metrics["agent_data_ops"],
        serde_json::json!({
            "describe":0,"busy":0,"diverged":0,"prepare":0,"export":0,
            "import":0,"release":0,"activate":0,"abort":0,"status":0
        })
    );
    assert!(metrics.get("agent_data_inventory").is_none());
    // The counter lock keeps the stale-frame sibling out during this interval. Equal readings
    // show that this rig's health probe and valid route request did not drop a frame, while the
    // unsigned values above ensure each metric remains present when its process count is zero.
    assert_eq!(
        metrics["serve_frames_dropped_unbound_channel"].as_u64(),
        Some(unbound_drops_before),
        "this rig did not submit an unbound-channel frame"
    );
    assert_eq!(
        metrics["serve_frames_dropped_stale_epoch"].as_u64(),
        Some(stale_drops_before),
        "this rig did not submit a stale-epoch frame"
    );
    // ONE, not zero: this rig holds a run at the point health is probed. I wrote zero first,
    // assuming the rig drove no runs, and the fence corrected me -- which is the better
    // assertion by luck. A gauge hardcoded to zero would satisfy `== 0` here and would also
    // approve every restart, because SUBC's gate reads this to decide whether a bounce would
    // cut work in flight. Asserting the NON-zero reading is what proves it counts at all.
    assert_eq!(metrics["runs_in_flight"], serde_json::json!(1));

    // Release the parked run so the rig can shut down cleanly.
    hold.notify_one();

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A vanished project root refuses the ops that use it and admits the ops that end a run.
///
/// The second half is the one worth having. A run whose root disappears can only be ended
/// through `run.cancel`/`run.status`, so a check applied to every op would leave it with no
/// exit at all -- turning a directory that went missing into a run nobody can finish.
#[tokio::test]
async fn a_vanished_project_root_refuses_execute_ops_but_still_admits_run_ops() {
    let dir = tmpdir("vanished-root");
    let reg = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let root = dir.join("proj");
    let session = BindIdentity::new(root.clone(), "runner", "vanished");
    let inventory = broca_store::SqliteStore::open(dir.join("store.db")).unwrap();
    broca_core::SessionRefIndex::record_session(
        &inventory,
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(bind).unwrap()).await;
    assert_eq!(read_one(&mut creader).await.header.ty, FrameType::Response);

    // CONTROL: the same send succeeds while the root is present, so a later refusal is
    // attributable to the missing directory rather than to anything else in this rig.
    let send = serde_json::json!({
        "method": "session.send",
        "params": {"prompt": "hi", "model": {"provider": "fake", "model": "fake"}}
    });
    write_req_epoch(&mut cwriter, 7, 2, 11, send.clone()).await;
    let admitted = read_one(&mut creader).await;
    assert_eq!(
        admitted.header.ty,
        FrameType::Response,
        "the send must be admitted while the root exists"
    );
    let admitted_body: serde_json::Value = serde_json::from_slice(&admitted.body).unwrap();
    let run_id = admitted_body["result"]["run_id"]
        .as_str()
        .expect("an admitted send returns its run_id")
        .to_string();

    std::fs::remove_dir_all(&root).unwrap();
    assert!(
        !root.exists(),
        "the root must be gone for this test to mean anything"
    );

    // A send USES the root: tools resolve paths beneath it, so this must fail at admission
    // rather than several steps later at the first tool call.
    write_req_epoch(&mut cwriter, 7, 2, 12, send).await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let refused_body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(refused_body["code"], "invalid_project_root");
    assert_eq!(
        refused_body["message"],
        format!("project root does not exist: {}", root.display()),
        "the refusal must name the missing directory so the operator knows what to repair"
    );

    let import = serde_json::json!({
        "method": "session.import",
        "params": {
            "messages": [],
            "import_id": "vanished-import",
            "origin": {"kind": "import", "source_ref": "fixture"}
        }
    });
    write_req_epoch(&mut cwriter, 7, 2, 13, import).await;
    let refused = read_one(&mut creader).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    let refused_body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(refused_body["code"], "invalid_project_root");
    assert_eq!(
        refused_body["message"],
        format!("project root does not exist: {}", root.display()),
        "an import refusal must name the same missing directory"
    );

    // THE HALF THAT MATTERS: every op that reads or ends a run must still be reachable.
    // run.cancel is named explicitly because it is the ONLY exit from a paused run -- a test
    // that checked run.status alone would pass even with the check wrongly applied to cancel,
    // which is precisely the mistake that strands a run forever.
    for (corr, method) in [(14u64, "run.status"), (15, "run.cancel")] {
        write_req_epoch(
            &mut cwriter,
            7,
            2,
            corr,
            serde_json::json!({"method": method, "params": {"run_id": run_id}}),
        )
        .await;
        let reply = read_one(&mut creader).await;
        if reply.header.ty == FrameType::Error {
            let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
            assert_ne!(
                body["code"], "invalid_project_root",
                "{method} must not be refused for a vanished root, or a run in this state has no exit"
            );
        }
    }

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Vault health distinguishes never-connected, connected, broken, and cached-fallback states.
#[tokio::test]
async fn health_check_vault_gauge_flips_on_stamped_outcomes() {
    if !isolated_health_probe("wire::health_check_vault_gauge_flips_on_stamped_outcomes").await {
        return;
    }
    let dir = tmpdir("health-gauge");
    let reg = registry(&dir).await;
    let gauge = broca_module_serve::serve::VaultGauge::disconnected();
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_gauge = gauge.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(sreader, swriter, "broca", reg.clone(), reg, serve_gauge).await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let probe = || serde_json::to_value(&ModuleControlRequest::HealthCheck {}).unwrap();

    write_req(&mut cwriter, 0, 20, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert_eq!(
        detail.as_deref(),
        Some("vault: credentials module not catalog-live")
    );
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("disconnected"));
    assert!(
        metrics.get("vault_ok_age_s").is_none(),
        "a gauge that has never completed a call must not report an age at all"
    );

    gauge.stamp_ok();
    write_req(&mut cwriter, 0, 21, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Ok);
    assert_eq!(detail, None);
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("connected"));
    // The healthy reading has to be ageable too. Before a call completes the field is ABSENT
    // rather than zero, so "registered but never dialled" cannot be read as a fresh success.
    assert_eq!(
        metrics["vault_ok_age_s"].as_i64(),
        Some(0),
        "a just-stamped success ages to zero seconds"
    );

    gauge.stamp_error();
    write_req(&mut cwriter, 0, 22, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert!(
        detail.unwrap().contains("credential vault unreachable"),
        "broken state names the unreachable vault"
    );
    let metrics = metrics.unwrap();
    assert_eq!(metrics["vault"], serde_json::json!("broken"));
    assert!(metrics["last_vault_error_ms"].as_i64().unwrap() > 0);

    gauge.stamp_serving_cached();
    write_req(&mut cwriter, 0, 23, probe()).await;
    let resp = read_one(&mut creader).await;
    let ModuleControlResponse::HealthCheck {
        status,
        detail,
        metrics,
    } = serde_json::from_slice(&resp.body).unwrap()
    else {
        panic!("expected a health.check response");
    };
    assert_eq!(status, HealthStatus::Degraded);
    assert_eq!(
        detail.as_deref(),
        Some("vault unreachable; serving cached credentials")
    );
    assert_eq!(metrics.unwrap()["vault"], serde_json::json!("broken"));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// HELLO carries the broca manifest with the ManagementSurface shape; route.bind acks;
/// session.send + run.status dispatch and return Response frames.
#[tokio::test]
async fn hello_bind_send_status_roundtrip() {
    let dir = tmpdir("rt");
    let reg = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    // 1. HELLO from the module.
    let hello = read_one(&mut creader).await;
    assert_eq!(hello.header.ty, FrameType::Hello);
    let body: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
    assert_eq!(body.manifest.module_id, "broca");
    let slot = body
        .manifest
        .role_declarations
        .get(cortexkit_role_routing::ROLE_DECLARATIONS_KEY)
        .expect("HELLO publishes the routing runner declaration");
    let declarations = cortexkit_role_routing::declarations::parse_consumer_declarations(
        &serde_json::Value::Object(slot.clone()),
    )
    .expect("the HELLO routing slot validates as a whole");
    let runner = declarations
        .runner
        .expect("HELLO declares broca as a runner");
    assert!(runner.follows_account_choice);
    assert!(!runner.requires_wire_api());
    assert_eq!(runner.pairs.len(), 13);
    assert_eq!(
        runner
            .pairs
            .iter()
            .filter(|pair| runner.requires_wire_api_for_pair(pair))
            .count(),
        9
    );
    assert!(runner
        .pairs
        .iter()
        .any(|pair| pair.wire_api == "google.code_assist" && pair.auth_method == "antigravity"));
    assert_eq!(
        body.control_ops,
        Some(vec![MODULE_CONTROL_OP_HEALTH_CHECK.to_string()])
    );
    // Send HELLO_ACK.
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    // 2. route.bind on channel 7 for a session.
    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let ack = read_one(&mut creader).await;
    assert_eq!(ack.header.ty, FrameType::Response);
    let _: ModuleControlResponse = serde_json::from_slice(&ack.body).unwrap();

    // 3. session.send on the bound route.
    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(
        v["result"]["state"], "active",
        "send returns active for an idle lineage"
    );

    // 4. run.status on the bound route.
    write_req(
        &mut cwriter,
        7,
        12,
        serde_json::json!({ "method": "run.status", "params": {} }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert!(v["result"]["state"].is_string(), "status returns a state");

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// This test verifies that route requests are spawned instead of being handled directly in
/// the reader loop. If the reader waits for one slow send to finish before reading the next
/// frame, another session can time out, so spawning request tasks keeps the connection
/// draining and prevents one blocked request from delaying other sessions.
#[tokio::test]
async fn slow_request_on_one_session_does_not_block_another_session() {
    let dir = tmpdir("hol");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let slow_session = BindIdentity::new("broca-unscoped-v1", "runner", "slow");
    let slow_bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(slow_session.harness.clone(), slow_session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(
        &mut cwriter,
        0,
        10,
        serde_json::to_value(&slow_bind).unwrap(),
    )
    .await;
    let _ = read_one(&mut creader).await;

    let fast_session = BindIdentity::new("broca-unscoped-v1", "runner", "fast");
    let fast_bind = ModuleControlRequest::RouteBind {
        route_channel: 8,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(fast_session.harness.clone(), fast_session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(
        &mut cwriter,
        0,
        11,
        serde_json::to_value(&fast_bind).unwrap(),
    )
    .await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "slow", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    handler.slow_send_started.notified().await;

    write_req(
        &mut cwriter,
        8,
        21,
        serde_json::json!({ "method": "run.status", "params": {} }),
    )
    .await;

    let fast_resp = tokio::time::timeout(Duration::from_millis(250), read_one(&mut creader))
        .await
        .expect("the fast session must not wait for the slow session's send");
    assert_eq!(fast_resp.header.channel, 8);
    assert_eq!(fast_resp.header.corr, 21);
    assert_eq!(fast_resp.header.ty, FrameType::Response);
    let fast_body: serde_json::Value = serde_json::from_slice(&fast_resp.body).unwrap();
    assert_eq!(fast_body["result"]["state"], "idle");

    handler.release_slow_send.notify_one();
    let slow_resp = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("the slow send should finish once the test releases it");
    assert_eq!(slow_resp.header.channel, 7);
    assert_eq!(slow_resp.header.corr, 20);
    assert_eq!(slow_resp.header.ty, FrameType::Response);
    let slow_body: serde_json::Value = serde_json::from_slice(&slow_resp.body).unwrap();
    assert_eq!(slow_body["result"]["state"], "active");

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pipelined_requests_for_one_session_reach_the_handler_in_wire_order() {
    let dir = tmpdir("same-session-fifo");
    let handler = Arc::new(BlockingSendHandler::new());
    let health_registry = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_handler = handler.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_handler,
            health_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "ordered");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "first", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    handler.first_ordered_send_started.notified().await;

    write_req(
        &mut cwriter,
        7,
        21,
        serde_json::json!({
            "method": "session.send",
            "params": { "prompt": "second", "model": { "provider": "fake", "model": "fake" } }
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            handler.second_ordered_send_started.notified()
        )
        .await
        .is_err(),
        "the second request must not reach the actor-bound handler while the first is parked"
    );

    handler.release_first_ordered_send.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("first request completes after release");
    assert_eq!((first.header.channel, first.header.corr), (7, 20));
    handler.second_ordered_send_started.notified().await;
    let second = tokio::time::timeout(Duration::from_secs(2), read_one(&mut creader))
        .await
        .expect("second request runs after the first");
    assert_eq!((second.header.channel, second.header.corr), (7, 21));

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A one-shot send's public `session.read` returns its complete transcript.
#[tokio::test]
async fn read_route_manifest_and_page_roundtrip() {
    let dir = tmpdir("read");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let exec = Arc::new(CoreExec {
        reply_text: "one-shot answer".into(),
    });
    let opener = Arc::new(
        DefaultOpener::new(lease, exec.clone())
            .with_store_runtime(store_exec::store_runtime(&dir, exec).await)
            .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1))),
    );
    let reg = Arc::new(test_registry(LineageRegistry::new(opener)));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let body: ModuleHelloBody = serde_json::from_slice(&hello.body).unwrap();
    let read_op = body
        .manifest
        .provides
        .iter()
        .find_map(|role| match role {
            subc_protocol::manifest::ProviderRole::ManagementSurface { operations, .. } => {
                operations.iter().find(|op| op.name == "session.read")
            }
            _ => None,
        })
        .expect("manifest includes session.read");
    assert_eq!(
        read_op.kind,
        subc_protocol::manifest::ManagementOperationKind::Query
    );
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    write_req(
        &mut cwriter,
        7,
        11,
        serde_json::json!({
            "method": "session.send",
            "params": {
                "prompt": "hi",
                "model": { "provider": "fake", "model": "fake" },
                "one_shot": true
            }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let run_id = v["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 100).await;

    write_req(
        &mut cwriter,
        7,
        12,
        serde_json::json!({
            "method": "session.read",
            "params": { "from_ordinal": 0, "limit": 10 }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let result: broca_module_serve::wire::ReadResult =
        serde_json::from_value(v["result"].clone()).unwrap();
    assert_eq!(result.messages.len(), 2);
    // llm-runner 0.8 ordinals start at 1.
    assert_eq!(result.messages[0].ordinal, 1);
    assert_eq!(result.messages[0].message.role, Role::User);
    assert_eq!(result.messages[1].ordinal, 2);
    assert_eq!(result.messages[1].message.role, Role::Assistant);
    assert_eq!(result.lineage_state.state, "completed");
    assert_eq!(result.lineage_state.reason.as_deref(), Some("completed"));
    assert_eq!(result.tools, None, "tools are opt-in");
    assert_eq!(result.tools_run_id, None);

    // `include_tools` must travel from the wire params to the handler. This send declared no
    // tools, so the episode's frozen list is present and empty -- not absent, which would
    // mean the session has no episode.
    write_req(
        &mut cwriter,
        7,
        13,
        serde_json::json!({
            "method": "session.read",
            "params": { "from_ordinal": 0, "limit": 10, "include_tools": true }
        }),
    )
    .await;
    let resp = read_one(&mut creader).await;
    assert_eq!(resp.header.ty, FrameType::Response);
    let v: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let with_tools: broca_module_serve::wire::ReadResult =
        serde_json::from_value(v["result"].clone()).unwrap();
    assert_eq!(with_tools.tools, Some(Vec::new()));
    assert_eq!(with_tools.tools_run_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(with_tools.messages, result.messages);

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two omitted-tool `session.send` requests with the same `send_id` route to the same
/// durable episode. Reusing that id with an explicit empty surface is rejected.
#[tokio::test]
async fn same_send_id_same_prompt_reuses_one_episode() {
    let dir = tmpdir("send-id");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let exec = Arc::new(CoreExec {
        reply_text: "answer".into(),
    });
    let _store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
    let runtime = store_exec::store_runtime(&dir, exec.clone()).await;
    let wal_sessions = Sessions(runtime.clone());
    let reg = Arc::new(test_registry(LineageRegistry::new(Arc::new(
        DefaultOpener::new(lease, exec)
            .with_idle_ttl(Duration::from_millis(50))
            .with_store_runtime(runtime)
            .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1))),
    ))));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = reg.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let send_body = serde_json::json!({
        "method": "session.send",
        "params": {
            "prompt": "hi",
            "send_id": "logical-send-1",
            "model": { "provider": "fake", "model": "fake" }
        }
    });

    write_req(&mut cwriter, 7, 11, send_body.clone()).await;
    let resp1 = read_one(&mut creader).await;
    assert_eq!(resp1.header.ty, FrameType::Response);
    let v1: serde_json::Value = serde_json::from_slice(&resp1.body).unwrap();
    let run_id = v1["result"]["run_id"]
        .as_str()
        .expect("first send returns a run_id")
        .to_string();
    let terminal = wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 100).await;
    assert_eq!(terminal["final_step_finish_reason"], "stop");
    assert_eq!(terminal["usage"]["input_tokens"], 11);
    assert_eq!(terminal["usage"]["output_tokens"], 4);

    broca_test_support::wait_for(
        "same_send_id_same_prompt_reuses_one_episode",
        async {
            loop {
                if reg.slot_state(&session).is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
        || format!("slot={:?}", reg.slot_state(&session)),
    )
    .await;
    assert_eq!(
        reg.slot_state(&session),
        None,
        "the mismatch must exercise durable comparison after actor recreation"
    );

    let mut explicit_empty = send_body.clone();
    explicit_empty["params"]["tools"] = serde_json::json!([]);
    write_req(&mut cwriter, 7, 12, explicit_empty).await;
    let mismatch = read_one(&mut creader).await;
    assert_eq!(mismatch.header.ty, FrameType::Error);
    let mismatch_body: serde_json::Value = serde_json::from_slice(&mismatch.body).unwrap();
    assert_eq!(mismatch_body["code"], "send_id_reuse");
    assert!(
        mismatch_body["message"]
            .as_str()
            .is_some_and(|message| message.contains("tools")),
        "the recreated actor must preserve the field-specific tools mismatch: {mismatch_body}"
    );

    write_req(&mut cwriter, 7, 13, send_body).await;
    let resp2 = read_one(&mut creader).await;
    assert_eq!(resp2.header.ty, FrameType::Response);
    let v2: serde_json::Value = serde_json::from_slice(&resp2.body).unwrap();
    assert_eq!(
        v2["result"]["run_id"].as_str(),
        Some(run_id.as_str()),
        "the byte-identical retry still resolves the same derived run_id"
    );
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &run_id, 140).await;

    let run_starts = wal_sessions
        .read_records(&session)
        .await
        .unwrap()
        .into_iter()
        .filter(|f| matches!(f.record, Record::RunStart(_)))
        .count();
    assert_eq!(
        run_starts, 1,
        "two identical same-send-id sends write one episode"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// session.subscribe opens a StreamData* → StreamEnd stream; dropping the client mid-stream
/// (egress-closed) tears the subscriber down cleanly (the serve loop ends, no hang/panic).
#[tokio::test]
async fn subscribe_streams_then_egress_close_tears_down_cleanly() {
    let dir = tmpdir("sub");
    let reg = registry(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    // HELLO/ACK.
    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    // bind + send a run (so there's a RunStarted + terminal to stream).
    let session = BindIdentity::new("broca-unscoped-v1", "runner", "s1");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 3,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 1, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await; // bind ack

    // Bind a SECOND, dedicated route channel (4) for the subscribe stream — subscribe holds
    // its route's credit until StreamEnd, so it must not share the ops channel.
    let bind4 = ModuleControlRequest::RouteBind {
        route_channel: 4,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 5, serde_json::to_value(&bind4).unwrap()).await;
    let _ = read_one(&mut creader).await; // bind4 ack

    write_req(
        &mut cwriter,
        3,
        2,
        serde_json::json!({ "method": "session.send", "params": { "prompt": "hi", "model": { "provider": "fake", "model": "fake" } } }),
    )
    .await;
    let _ = read_one(&mut creader).await; // send response

    // subscribe from start on the DEDICATED route channel (4), corr 99.
    write_req(
        &mut cwriter,
        4,
        99,
        serde_json::json!({ "method": "session.subscribe", "params": { "from": "start" } }),
    )
    .await;

    // Read at least one StreamData frame on (channel=4, corr=99). A session subscription is
    // LINEAGE-scoped: it does NOT emit StreamEnd when the run finishes — it stays open
    // across turns. So we expect StreamData (the RunStarted etc. control units) and the
    // stream stays live (no StreamEnd until unsubscribe/teardown).
    let mut saw_stream_data = false;
    for _ in 0..20 {
        let f = match tokio::time::timeout(Duration::from_secs(2), read_frame(&mut creader)).await {
            Ok(Ok(Some(f))) => f,
            _ => break,
        };
        if f.header.channel == 4 && f.header.corr == 99 && f.header.ty == FrameType::StreamData {
            saw_stream_data = true;
            break;
        }
    }
    assert!(
        saw_stream_data,
        "the subscribe stream emitted at least one StreamData frame (the run's control units)"
    );

    // EGRESS CLOSE: drop the client entirely while the lineage-scoped subscription is still
    // LIVE (its forward task is parked waiting for the next turn's events). The connection
    // teardown must cancel that parked forward task so it drops its egress sender, the
    // writer channel closes, and `serve_connection` ends — WITHOUT hanging. (Before the
    // cancel-on-teardown fix, the parked forward task held the egress open forever.)
    drop(cwriter);
    drop(creader);
    let result = tokio::time::timeout(Duration::from_secs(3), serve).await;
    assert!(
        result.is_ok(),
        "the serve loop must end cleanly after egress close even with a LIVE subscription \
         (the parked forward task must be cancelled on teardown, not leak/hang)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The production seal hangs off `serve_connection` RETURNING when the daemon's side of the
/// connection closes (main.rs: `result = &mut serving => { registry.stop_and_seal() ... }`).
/// Every test in this file exercises that property by dropping the client halves at the end
/// and awaiting the serve task -- and a serve loop that RECONNECTED instead of returning would
/// make all of them hang, which reads as a slow runner rather than a regression. Covered
/// everywhere, failure mode named nowhere. This arm is bounded and names the consequence.
///
/// Measured why it matters: the 2026-09-18 daemon cut sent no GOODBYE (the daemon has no
/// signal handler), so EOF was the ONLY trigger the seal had, and one run in flight was sealed
/// correctly because serve returned. A refactor that made serve reconnect on EOF would leave
/// the seal's own tests green (they call stop_and_seal directly) while the production seal
/// never fired.
#[tokio::test]
async fn serve_returns_on_daemon_eof_so_the_seal_can_run() {
    let dir = tmpdir("serve-eof");
    let (registry, _wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    // Bring the connection fully up first, so EOF lands on a SERVING loop and not on a
    // handshake that would have refused anyway.
    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    // The daemon dies: both halves of its side go away. No GOODBYE, no route.closing.
    drop(cwriter);
    drop(creader);

    let returned = tokio::time::timeout(Duration::from_secs(5), serve).await;
    assert!(
        returned.is_ok(),
        "serve_connection must RETURN when the daemon's connection closes; it did not within 5s. \
         If it reconnects or waits instead, the production seal (main.rs, the `serving` arm) \
         never runs and every in-flight run is left to WAL recovery on the next open."
    );
    let end = returned
        .unwrap()
        .expect("serve task panicked")
        .expect("a clean EOF ends serve Ok");
    assert_eq!(
        end,
        ServeEnd::Eof,
        "a close with no GOODBYE first must be reported as EOF on the seal line"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A channel-0 GOODBYE is the daemon's planned stop, and serve must return on it promptly and
/// say so: the seal line in main.rs appends `(cause: goodbye)` from this value, which is how a
/// placement tells a planned stop from the connection merely ending. The client halves stay
/// open until serve has returned, so EOF cannot be what ended it.
#[tokio::test]
async fn serve_returns_on_channel0_goodbye_with_cause_goodbye() {
    let dir = tmpdir("serve-goodbye");
    let (registry, _wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let goodbye = Frame::build(
        FrameType::Goodbye,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        9,
        Vec::new(),
    )
    .unwrap();
    write_frame(&mut cwriter, &goodbye).await.unwrap();

    let end = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve_connection must return promptly on a channel-0 GOODBYE")
        .expect("serve task panicked")
        .expect("a GOODBYE ends serve Ok");
    assert_eq!(
        end,
        ServeEnd::Goodbye,
        "a channel-0 GOODBYE must be reported as the cause, not EOF"
    );
    drop(cwriter);
    drop(creader);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A daemon cut RESETS the socket rather than closing it, and that has to end serve with `Ok`
/// too, or main exits 1 after a clean seal and the supervisor records a crash.
///
/// Measured on the 2026-09-24 16:25Z cut: broca logged "shutdown seal complete on serve
/// return", then main returned `ServeIoError("frame I/O error: Connection reset by peer (os
/// error 54)")`, exited 1, and `ck module` recorded exit_kind=Crash for a clean stop. The EOF
/// test above cannot see this: a dropped in-memory duplex reads as a clean EOF, never a reset.
/// So this arm uses a REAL TCP pair and closes the daemon's side with SO_LINGER 0, which makes
/// the kernel send RST -- the same thing the cut produced.
#[tokio::test]
async fn serve_ends_ok_when_the_daemon_resets_the_socket() {
    let dir = tmpdir("serve-reset");
    let (registry, _wal) = registry_and_wal(&dir).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let module_side = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (daemon_side, _) = listener.accept().await.unwrap();

    let (sreader, swriter) = tokio::io::split(module_side);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let (mut creader, mut cwriter) = tokio::io::split(daemon_side);
    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    // WAIT FOR THE SERVING LOOP BEFORE RESETTING. A reset discards data the receiver hasn't
    // read yet, and on Windows that includes the HELLO_ACK just written: the first version of
    // this test reset straight after it, so on Windows CI serve failed inside the handshake
    // (os error 10054) instead of in the serving loop this test is about. A channel-0 Ping is
    // answered only by the serving loop, so reading its Pong proves serve consumed HELLO_ACK
    // and is serving, with nothing left unread for the reset to throw away.
    let ping = Frame::build(
        FrameType::Ping,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        7,
        Vec::new(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ping).await.unwrap();
    let pong = read_one(&mut creader).await;
    assert_eq!(
        pong.header.ty,
        FrameType::Pong,
        "the serving loop answers a channel-0 Ping"
    );

    // The daemon dies with the socket still open: linger 0 turns the close into a reset.
    let daemon_side = creader.unsplit(cwriter);
    daemon_side.set_zero_linger().unwrap();
    drop(daemon_side);

    let returned = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve_connection must return when the daemon resets the socket")
        .expect("serve task panicked");
    assert!(
        returned.is_ok(),
        "a reset from the daemon is the daemon going away, not a fault: serve must end Ok so \
         main exits 0 after the seal. Got {returned:?}"
    );
    assert_eq!(
        returned.unwrap(),
        ServeEnd::PeerReset,
        "a reset must be reported as its own cause on the seal line, not as EOF"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stray_bind_project_members_do_not_change_send_address_or_attribution() {
    project_observation_wire(false).await;
}

#[tokio::test]
async fn stray_bind_project_members_do_not_change_import_address_or_attribution() {
    project_observation_wire(true).await;
}

async fn project_observation_wire(import: bool) {
    let dir = tmpdir(if import {
        "project-import"
    } else {
        "project-observation"
    });
    let exec = Arc::new(CoreExec {
        reply_text: "answer".into(),
    });
    let runtime = store_exec::store_runtime(&dir, exec.clone()).await;
    let opener = Arc::new(
        DefaultOpener::new(Arc::new(FileLeaseStore::new(dir.join("leases"))), exec)
            .with_store_runtime(runtime.clone())
            .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1))),
    );
    let reg = Arc::new(test_registry(LineageRegistry::new(opener)));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut reader, mut writer) = tokio::io::split(client);
    let (sr, sw) = tokio::io::split(server);
    let serve = tokio::spawn(async move {
        serve_connection(
            sr,
            sw,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });
    let hello = read_one(&mut reader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut writer, &ack).await.unwrap();
    let session = BindIdentity::new("broca-unscoped-v1", "runner", "observed");
    let address = broca_core::session_addr(&session);
    let mut previous: Vec<String> = Vec::new();
    for (index, project_id) in [Some("pj-wire"), None].into_iter().enumerate() {
        let mut bind = serde_json::to_value(ModuleControlRequest::RouteBind {
            route_channel: 7,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", "observed"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: None,
            call_mode: None,
        })
        .unwrap();
        if let Some(project_id) = project_id {
            bind["identity"]["project_root"] = serde_json::json!("/stray/legacy/folder");
            bind["identity"]["project_id"] = serde_json::json!(project_id);
        }
        let _: ModuleControlRequest = serde_json::from_value(bind.clone()).unwrap();
        write_req(&mut writer, 0, 10, bind).await;
        assert_eq!(read_one(&mut reader).await.header.ty, FrameType::Response);
        let request = if !import || index > 0 {
            serde_json::json!({"method":"session.send", "params": {
                "prompt":"hi", "send_id":format!("turn-{index}"),
                "model":{"provider":"fake","model":"fake"}
            }})
        } else {
            serde_json::json!({"method":"session.import", "params": {
                "import_id":"import-observed", "origin":{"kind":"import","source_ref":"fixture:observed"},
                "messages":[{"role":"user","content":[{"type":"text","text":"imported"}]}]
            }})
        };
        wait_for_session_idle(&mut reader, &mut writer, 7, 1_000).await;
        write_req(&mut writer, 7, 11, request).await;
        let response = read_one(&mut reader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let run_id = response["result"]["run_id"]
            .as_str()
            .unwrap_or_else(|| panic!("send reply has no run id: {response}"));
        if !import || index > 0 {
            wait_for_completed_run(&mut reader, &mut writer, 7, run_id, 100).await;
        }
        // The session is stored under the original triple's address, and nowhere else.
        assert!(
            broca_wal::session_storage_class(&dir, &address)
                .unwrap()
                .is_some(),
            "the original triple must address the session"
        );
        let records: Vec<String> = store_exec::records(&runtime, &session)
            .await
            .iter()
            .map(|record| format!("{record:?}"))
            .collect();
        assert!(records.starts_with(&previous));
        let episodes = store_exec::records(&runtime, &session)
            .await
            .iter()
            .filter(|record| matches!(record, Record::RunStart(_)))
            .count();
        assert_eq!(episodes, index + 1);
        previous = records;
        let heads: usize = broca_wal::StorageClass::ALL
            .into_iter()
            .map(|class| {
                std::fs::read_dir(class.store_root(&dir).join("heads"))
                    .map(|entries| entries.count())
                    .unwrap_or(0)
            })
            .sum();
        assert_eq!(heads, 1, "one session is stored");
        write_req(
            &mut writer,
            7,
            200,
            serde_json::json!({"method":"session.read", "params":{}}),
        )
        .await;
        let page = read_one(&mut reader).await;
        assert_eq!(page.header.ty, FrameType::Response);
        let page: serde_json::Value = serde_json::from_slice(&page.body).unwrap();
        assert!(!page.to_string().contains("project_id"));
    }
    drop(writer);
    drop(reader);
    tokio::time::timeout(Duration::from_secs(2), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Detached actor and projector tasks may still hold store handles inside `dir` for a
    // moment after serve returns; Windows refuses to remove a tree with an open file in it
    // where unix unlinks silently. Every other test in this file ignores the result for the
    // same reason. Nothing under test lives past this line.
    let _ = std::fs::remove_dir_all(dir);
}

// ── The bind principal: recorded on RunStarted and Queued through the real serve path ──

/// A stub provider whose step can be held, so a run stays genuinely active (admitted, its
/// `RunStarted` durable) while the test queues other sends behind it.
struct GatedProvider {
    stub: StubProvider,
    gate: Option<Arc<Notify>>,
}

#[async_trait]
impl Provider for GatedProvider {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        if let Some(gate) = &self.gate {
            gate.notified().await;
        }
        self.stub.step(step_id, opts).await
    }
}

/// The real core loop, so `RunStarted` is written by core from the request the serve layer
/// built, never by the test. Only the first run it executes waits on `gate`.
struct GatedCoreExec {
    gate: Arc<Notify>,
    runs: AtomicUsize,
    completion_gate: Option<Arc<CompletionGate>>,
}

/// The executor seam holds the first completion after core has synced and emitted its terminal.
/// The actor cannot process completion until the executor returns, regardless of scheduling.
struct CompletionGate {
    held: Notify,
    release: Notify,
}

#[async_trait]
impl RunExecutor for GatedCoreExec {
    /// Every session is kept in the new storage: the first run waits on `gate` before its model
    /// step, and its completion is held on `completion_gate` once the run has ended.
    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        let first = self.runs.fetch_add(1, Ordering::SeqCst) == 0;
        let provider = GatedProvider {
            stub: StubProvider::new(vec![one_step_text_reply("answer")]),
            gate: first.then(|| self.gate.clone()),
        };
        let end = store_exec::run_on_store(job, &out, &provider, &StubToolPlane::new(), None).await;
        if first {
            if let Some(gate) = &self.completion_gate {
                gate.held.notify_one();
                gate.release.notified().await;
            }
        }
        end
    }
}

/// One serve connection over a real registry and core loop, with helpers to bind channels
/// under a principal and to read what reached the WAL.
struct PrincipalRig {
    dir: std::path::PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    runtime: Arc<broca_module::store_open::StoreRuntime>,
    session: BindIdentity,
    gate: Arc<Notify>,
    corr: u64,
}

fn prefrontal_core() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn other_agent() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "other-agent".into(),
    }
}

impl PrincipalRig {
    async fn start(tag: &str) -> Self {
        Self::start_with_completion_gate(tag, None).await
    }

    async fn start_with_completion_gate(
        tag: &str,
        completion_gate: Option<Arc<CompletionGate>>,
    ) -> Self {
        let dir = tmpdir(tag);
        let gate = Arc::new(Notify::new());
        let exec = Arc::new(GatedCoreExec {
            gate: gate.clone(),
            runs: AtomicUsize::new(0),
            completion_gate,
        });
        let runtime = store_exec::store_runtime(&dir, exec.clone()).await;
        let opener = Arc::new(
            DefaultOpener::new(Arc::new(FileLeaseStore::new(dir.join("leases"))), exec)
                .with_store_runtime(runtime.clone())
                .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1))),
        );
        let reg = Arc::new(test_registry(LineageRegistry::new(opener)));
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (sr, sw) = tokio::io::split(server);
        let serve = tokio::spawn(async move {
            serve_connection(
                sr,
                sw,
                "broca",
                reg.clone(),
                reg,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_one(&mut reader).await;
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        let session = BindIdentity::new("broca-unscoped-v1", "runner", "bound");
        Self {
            dir,
            reader,
            writer,
            serve,
            runtime,
            session,
            gate,
            corr: 1,
        }
    }

    fn next_corr(&mut self) -> u64 {
        self.corr += 1;
        self.corr
    }

    /// Bind `channel` to the rig's session the way the daemon relays it, stamped with
    /// `principal` (or with none, as a daemon that predates principal stamping sends).
    async fn bind(&mut self, channel: u16, principal: Option<subc_protocol::Principal>) {
        let corr = self.next_corr();
        write_req(
            &mut self.writer,
            0,
            corr,
            serde_json::to_value(ModuleControlRequest::RouteBind {
                route_channel: channel,
                epoch: 1,
                target: RouteTarget::ManagementSurface {
                    module_id: "broca".into(),
                },
                identity: SubcBindIdentity::new("runner", "bound"),
                principal,
                consumer_capabilities: None,
                admission_facts: None,
                scope: None,
                role_versions: None,
                call_mode: None,
            })
            .unwrap(),
        )
        .await;
        let ack = read_one(&mut self.reader).await;
        assert_eq!(
            ack.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&ack.body)
        );
    }

    /// `session.send` on `channel`; returns the response's `result`.
    async fn send(&mut self, channel: u16, send_id: &str) -> serde_json::Value {
        let corr = self.next_corr();
        write_req(
            &mut self.writer,
            channel,
            corr,
            serde_json::json!({"method":"session.send", "params": {
                "prompt":"hi", "send_id":send_id,
                "model":{"provider":"fake","model":"fake"}
            }}),
        )
        .await;
        let response = read_one(&mut self.reader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        body["result"].clone()
    }

    /// The rig's session's records in the new storage, in log order.
    async fn records(&self) -> Vec<Record> {
        store_exec::records(&self.runtime, &self.session).await
    }

    /// The frozen config of each run the rig's session started, in the order they started.
    async fn run_configs(&self) -> Vec<broca_wal::RunConfig> {
        starts_in(&self.runtime, &self.session)
            .await
            .into_iter()
            .map(|(_, config)| config)
            .collect()
    }

    /// Whether the new storage holds the rig's session at all.
    fn exists(&self) -> bool {
        broca_wal::session_storage_class(
            self.runtime.state_root(),
            &broca_core::session_addr(&self.session),
        )
        .unwrap()
        .is_some()
    }

    /// Wait until `n` episodes have a durable terminal. Read from the log, so a queued send
    /// is waited for without asking the wire about a run that may not have started yet.
    async fn wait_for_finished(&self, n: usize) -> Vec<Record> {
        broca_test_support::wait_for(
            "wait_for_finished",
            async {
                loop {
                    let records = self.records().await;
                    let finished = records
                        .iter()
                        .filter(|record| matches!(record, Record::RunEnd(_)))
                        .count();
                    if finished >= n {
                        return records;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || "see the session's records".to_owned(),
        )
        .await
    }

    async fn finish(self) {
        let Self {
            dir,
            reader,
            writer,
            serve,
            ..
        } = self;
        drop(writer);
        drop(reader);
        tokio::time::timeout(Duration::from_secs(2), serve)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Detached actor tasks may keep store handles open for a moment after serve returns,
        // and Windows refuses to remove a directory with an open file, so ignore the result.
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[tokio::test]
async fn a_fresh_send_after_terminal_status_is_pending_until_completion_is_processed() {
    let completion = Arc::new(CompletionGate {
        held: Notify::new(),
        release: Notify::new(),
    });
    let mut rig =
        PrincipalRig::start_with_completion_gate("terminal-before-idle", Some(completion.clone()))
            .await;
    rig.bind(7, None).await;
    let first = rig.send(7, "first").await;
    let first_run = first["run_id"]
        .as_str()
        .expect("first send starts immediately");
    rig.gate.notify_one();
    broca_test_support::wait_for(
        "executor holds after its durable terminal",
        completion.held.notified(),
        || format!("first run: {first_run}"),
    )
    .await;
    let terminal =
        wait_for_completed_run(&mut rig.reader, &mut rig.writer, 7, first_run, 100).await;
    assert_eq!(terminal["state"], "completed");

    let corr = rig.next_corr();
    write_req(
        &mut rig.writer,
        7,
        corr,
        serde_json::json!({
            "method":"run.status", "params":{}
        }),
    )
    .await;
    let status = read_one(&mut rig.reader).await;
    assert_eq!(status.header.ty, FrameType::Response);
    let status: serde_json::Value = serde_json::from_slice(&status.body).unwrap();
    assert_eq!(status["result"]["state"], "active");
    assert_eq!(status["result"]["run_id"], first_run);

    let corr = rig.next_corr();
    write_req(
        &mut rig.writer,
        7,
        corr,
        serde_json::json!({
            "method":"session.send", "params":{
                "prompt":"Continue.", "model":{"provider":"fake", "model":"fake"}
            }
        }),
    )
    .await;
    let response = read_one(&mut rig.reader).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let response: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let pending = &response["result"];
    assert_eq!(pending["state"], "pending");
    assert!(pending["submission_id"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert!(pending.get("run_id").is_none(), "{response}");
    println!("terminal={terminal}; session={status}; second_send={response}");

    completion.release.notify_one();
    wait_for_session_idle(&mut rig.reader, &mut rig.writer, 7, 200).await;
    assert_eq!(run_started_principals(&rig.records().await).len(), 2);
    rig.finish().await;
}

/// A principal a record keeps as JSON, in its comparison form.
fn principal_of(value: Option<serde_json::Value>) -> Option<String> {
    let principal: Option<broca_protocol::BindPrincipal> =
        serde_json::from_value(value.unwrap_or(serde_json::Value::Null)).unwrap();
    principal.map(|principal| principal.as_owner_string())
}

fn run_started_principals(records: &[Record]) -> Vec<Option<String>> {
    records
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some(principal_of(start.principal.clone())),
            _ => None,
        })
        .collect()
}

fn queued_principals(records: &[Record]) -> Vec<Option<String>> {
    records
        .iter()
        .filter_map(|record| match record {
            Record::QueuedSend(queued) => Some(principal_of(
                queued
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.parse()["principal"].clone())
                    .filter(|principal| !principal.is_null()),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_send_over_a_reserved_bind_records_that_principal_on_run_started() {
    let mut rig = PrincipalRig::start("principal-reserved").await;
    rig.bind(7, Some(prefrontal_core())).await;
    rig.gate.notify_one();
    let result = rig.send(7, "turn-1").await;
    assert_eq!(result["state"], "active", "{result}");
    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        run_started_principals(&records),
        [Some("reserved:prefrontal-core".to_owned())]
    );
    // The run's start keeps the principal as the daemon's structured value; the
    // `reserved:prefrontal-core` string compared above is only its comparison form.
    let start = rig
        .records()
        .await
        .into_iter()
        .find_map(|record| match record {
            Record::RunStart(start) => start.principal.clone(),
            _ => None,
        })
        .expect("the run start keeps the principal");
    assert_eq!(
        start,
        serde_json::json!({"kind":"reserved","module_id":"prefrontal-core"}),
        "the run start must carry the stamped principal verbatim"
    );
    rig.finish().await;
}

/// A bind that carried no principal must record none: no member at all, never a default or an
/// empty value. Absence is what an older daemon sends, and it must never read as a principal.
#[tokio::test]
async fn a_bind_without_a_principal_writes_no_principal_bytes() {
    let mut rig = PrincipalRig::start("principal-absent").await;
    rig.bind(7, None).await;
    rig.gate.notify_one();
    let result = rig.send(7, "turn-1").await;
    assert_eq!(result["state"], "active", "{result}");
    let records = rig.wait_for_finished(1).await;
    assert_eq!(run_started_principals(&records), [None]);
    // An absent principal is absent on the run's start, never a default or an empty value.
    assert!(
        rig.records().await.iter().any(|record| matches!(
            record,
            Record::RunStart(start) if start.principal.is_none()
        )),
        "an absent principal is absent on the run's start"
    );
    rig.finish().await;
}

/// A send queued behind another caller's run records the principal that QUEUED it when it is
/// drained, not the principal of the run whose end drained it. Channel 9 starts a held run over
/// a bind with no principal; channel 7 queues, then channel 8 queues; releasing the held run
/// drains both in order.
///
/// The held run records no principal because an unscoped session accepts later sends only from
/// the principal of its first send, when that first send recorded one. A first episode without
/// a principal (a pre-attestation daemon's, as here) leaves the session open to any principal,
/// which is what lets two different principals queue into it.
#[tokio::test]
async fn a_drained_queued_send_records_the_principal_that_queued_it() {
    let mut rig = PrincipalRig::start("principal-queued").await;
    rig.bind(9, None).await;
    rig.bind(8, Some(other_agent())).await;
    rig.bind(7, Some(prefrontal_core())).await;

    let held = rig.send(9, "turn-1").await;
    assert_eq!(held["state"], "active", "{held}");
    let queued = rig.send(7, "turn-2").await;
    assert_eq!(queued["state"], "pending", "{queued}");
    let queued_after = rig.send(8, "turn-3").await;
    assert_eq!(queued_after["state"], "pending", "{queued_after}");

    rig.gate.notify_one();
    let records = rig.wait_for_finished(3).await;
    assert_eq!(
        run_started_principals(&records),
        [
            None,
            Some("reserved:prefrontal-core".to_owned()),
            Some("reserved:other-agent".to_owned()),
        ],
        "each drained run must record the principal that queued it"
    );
    assert_eq!(
        queued_principals(&records),
        [
            Some("reserved:prefrontal-core".to_owned()),
            Some("reserved:other-agent".to_owned()),
        ]
    );
    // The second RunStarted consumed the submission channel 7 queued.
    let consumed: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some(start.submission_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(consumed[0], None);
    assert_eq!(consumed[1].as_deref(), queued["submission_id"].as_str());
    rig.finish().await;
}

/// The principal is recorded, never part of what makes two sends the same send: the same
/// send id and payload from a second principal is the first send, not a reuse error, and it
/// starts nothing new.
///
/// The first send's bind carries no principal. An unscoped session whose first send recorded
/// a principal accepts later sends only from that principal (the owner check refuses anyone
/// else before the send key is even compared); one whose first send recorded none accepts any
/// principal, which is the case where two principals can present the same send at all.
#[tokio::test]
async fn the_same_send_from_two_principals_is_one_send() {
    let mut rig = PrincipalRig::start("principal-identity").await;
    rig.bind(7, None).await;
    rig.bind(8, Some(subc_protocol::Principal::Direct)).await;
    rig.gate.notify_one();

    let first = rig.send(7, "same-send").await;
    assert_eq!(first["state"], "active", "{first}");
    rig.wait_for_finished(1).await;
    let second = rig.send(8, "same-send").await;
    assert_eq!(second["run_id"], first["run_id"], "{second}");
    // `active` when the actor has not yet folded the durable terminal into its state; either
    // way it is the original run answering, not a refusal and not a new one.
    assert!(
        matches!(second["state"].as_str(), Some("active" | "finished")),
        "{second}"
    );

    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        run_started_principals(&records),
        [None],
        "the repeat must not start a second episode"
    );
    rig.finish().await;
}

/// The pinned refusal bodies are valid subc error bodies: decoded as subc's `ErrorBody` and
/// re-encoded, they keep exactly their bytes, so the refusal can be sent as an ordinary error
/// with a typed detail.
#[test]
fn scope_adoption_refused_golden_bodies_are_subc_error_bodies() {
    let golden = Path::new(env!("CARGO_MANIFEST_DIR")).join("../broca-wire/tests/golden");
    for file in [
        "scope_adoption_refused.no_recorded_principal.json",
        "scope_adoption_refused.principal_mismatch.json",
    ] {
        let bytes = std::fs::read(golden.join(file)).unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.code, broca_wire::SCOPE_ADOPTION_REFUSED);
        assert!(body.detail.is_some());
        assert_eq!(serde_json::to_vec(&body).unwrap(), bytes, "{file}");
    }
}

// ── The send key: recorded on the WAL record that carries the message ──

#[tokio::test]
async fn undecodable_control_request_errors_and_keeps_connection_alive() {
    let dir = tmpdir("invalid-control-request");
    let (registry, _store) = registry_with_export(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });

    let hello = read_one(&mut creader).await;
    let hello_ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &hello_ack).await.unwrap();

    let mut invalid_bind = serde_json::to_value(ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "invalid-control-request"),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    })
    .unwrap();
    invalid_bind["scope"] = serde_json::json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "scope_ref": "head-1",
        "scope_epoch": 1,
        "kind": "head",
        "parent": null,
        "parent_state": null,
        "attributes": {"agent_id": "agent-7", "delegates": true, "future_attribute": "value"},
        "owner_authorized": true
    });
    write_req(&mut cwriter, 0, 41, invalid_bind).await;
    let rejected = read_one(&mut creader).await;
    assert_eq!(rejected.header.ty, FrameType::Error);
    assert_eq!(
        rejected.header.corr, 41,
        "error must identify the rejected request"
    );
    let error: ErrorBody = serde_json::from_slice(&rejected.body).unwrap();
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("future_attribute"));

    write_req(
        &mut cwriter,
        0,
        42,
        serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
    )
    .await;
    let health = read_one(&mut creader).await;
    assert_eq!(health.header.ty, FrameType::Response);
    assert_eq!(health.header.corr, 42);
    assert!(matches!(
        serde_json::from_slice::<ModuleControlResponse>(&health.body).unwrap(),
        ModuleControlResponse::HealthCheck { .. }
    ));

    drop(cwriter);
    drop(creader);
    let _ = serve.await;
}

#[tokio::test]
async fn session_send_invalid_on_restart_matches_golden_and_writes_nothing() {
    let dir = tmpdir("restart-policy-invalid");
    let (registry, wal) = registry_and_wal(&dir).await;
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);

    let serve_registry = registry.clone();
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            serve_registry.clone(),
            serve_registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });

    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();

    let session = BindIdentity::new("broca-unscoped-v1", "runner", "restart-policy");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    for (corr, value) in [
        (11, serde_json::json!("always")),
        (12, serde_json::json!(true)),
        (13, serde_json::json!(null)),
    ] {
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({
                "method": "session.send",
                "params": {
                    "prompt": "manage my context",
                    "model": { "provider": "fake", "model": "fake" },
                    "on_restart": value,
                }
            }),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{value} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{value}: {error:?}");
        assert_eq!(error.detail.as_ref().unwrap()["field"], "on_restart");
        if corr == 11 {
            assert_eq!(
                response.body.as_slice(),
                include_bytes!("../../../broca-wire/tests/golden/invalid_params.on_restart.json")
            );
        }
    }
    assert!(
        no_durable_records(&wal, &session).await,
        "a refused value writes nothing durable"
    );

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A tool declared on `session.send` with `reply: {max_ms}`, over the real serve path: every
/// malformed `reply` is refused `invalid_params` naming its member before anything is written;
/// the golden send freezes the deadline into its `RunStarted` tool entry (byte for byte the
/// entry pinned in `run_started.tool_reply.json`, in a frame requiring `tool-reply/v1`); and a
/// later send that omits `tools` inherits the deadline with the rest of the tool.
#[tokio::test]
async fn send_declared_tool_reply_is_validated_frozen_and_inherited_over_the_serve_path() {
    let dir = tmpdir("tool-reply");
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let exec = Arc::new(CoreExec {
        reply_text: "researched".into(),
    });
    let runtime = store_exec::store_runtime(&dir, exec.clone()).await;
    let opener = Arc::new(
        DefaultOpener::new(lease, exec.clone())
            .with_store_runtime(runtime.clone())
            .with_charge_basis_resolver(Arc::new(|_: &str, _: &str| (ChargeBasis::Unknown, 1))),
    );
    let reg = Arc::new(test_registry(LineageRegistry::new(opener)));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut creader, mut cwriter) = tokio::io::split(client);
    let (sreader, swriter) = tokio::io::split(server);
    let serve = tokio::spawn(async move {
        let _ = serve_connection(
            sreader,
            swriter,
            "broca",
            reg.clone(),
            reg,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await;
    });
    let hello = read_one(&mut creader).await;
    let ack = Frame::build(
        FrameType::HelloAck,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        hello.header.corr,
        serde_json::to_vec(&ModuleHelloAckBody {
            negotiated_ver: PROTOCOL_VERSION,
            subc_ops: vec![],
            subc_capabilities: vec![],
            storage: None,
            machine_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    write_frame(&mut cwriter, &ack).await.unwrap();
    let session = BindIdentity::new("broca-unscoped-v1", "runner", "lead");
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 1,
        target: RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new(session.harness.clone(), session.session.clone()),
        principal: None,
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_req(&mut cwriter, 0, 10, serde_json::to_value(&bind).unwrap()).await;
    let _ = read_one(&mut creader).await;

    let mut golden: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../broca-wire/tests/golden/send_params.tool_reply.json"
    ))
    .unwrap();
    golden["model"] = serde_json::json!({ "provider": "fake", "model": "fake" });

    for (corr, reply, field) in [
        (11, serde_json::json!({}), "tools[0].reply.max_ms"),
        (
            12,
            serde_json::json!({"max_ms": 0}),
            "tools[0].reply.max_ms",
        ),
        (
            13,
            serde_json::json!({"max_ms": "1800000"}),
            "tools[0].reply.max_ms",
        ),
        (
            14,
            serde_json::json!({"max_ms": 1000, "grace_ms": 5}),
            "tools[0].reply.grace_ms",
        ),
        (15, serde_json::json!(1_800_000), "tools[0].reply"),
    ] {
        let mut params = golden.clone();
        params["tools"][0]["reply"] = reply.clone();
        write_req(
            &mut cwriter,
            7,
            corr,
            serde_json::json!({"method": "session.send", "params": params}),
        )
        .await;
        let response = read_one(&mut creader).await;
        assert_eq!(
            response.header.ty,
            FrameType::Error,
            "{reply} must be refused"
        );
        let error: ErrorBody = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error.code, "invalid_params", "{reply}: {error:?}");
        assert_eq!(error.detail.as_ref().unwrap()["field"], field, "{reply}");
        assert!(
            error.message.starts_with(field),
            "{reply}: {}",
            error.message
        );
    }
    assert!(
        no_durable_records(&Sessions(runtime.clone()), &session).await,
        "a refused reply writes nothing durable"
    );

    write_req(
        &mut cwriter,
        7,
        20,
        serde_json::json!({"method": "session.send", "params": golden}),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(
        response.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let first: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let first_run = first["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &first_run, 100).await;
    wait_for_session_idle(&mut creader, &mut cwriter, 7, 1_000).await;

    // A later send that omits `tools` inherits the frozen surface, deadline included.
    write_req(
        &mut cwriter,
        7,
        30,
        serde_json::json!({"method": "session.send", "params": {
            "prompt": "Continue.",
            "model": { "provider": "fake", "model": "fake" }
        }}),
    )
    .await;
    let response = read_one(&mut creader).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let second: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    let second_run = second["result"]["run_id"].as_str().unwrap().to_string();
    wait_for_completed_run(&mut creader, &mut cwriter, 7, &second_run, 200).await;

    let pinned: WalRecord = serde_json::from_str(include_str!(
        "../../../broca-wal/tests/golden/run_started.tool_reply.json"
    ))
    .unwrap();
    let WalRecord::RunStarted { config: pinned, .. } = pinned else {
        unreachable!()
    };
    let pinned_entry = serde_json::to_string(&pinned.tools[0]).unwrap();
    let starts = Sessions(runtime.clone()).starts(&session).await;
    assert_eq!(starts.len(), 2);
    for (_, config) in &starts {
        assert_eq!(config.tools.len(), 1);
        assert_eq!(
            serde_json::to_string(&config.tools[0]).unwrap(),
            pinned_entry
        );
    }

    drop(cwriter);
    drop(creader);
    let _ = tokio::time::timeout(Duration::from_secs(2), serve).await;
    let _ = std::fs::remove_dir_all(&dir);
}

async fn account_route_send(
    rig: &mut PrincipalRig,
    params: serde_json::Value,
) -> (FrameType, serde_json::Value) {
    let corr = rig.next_corr();
    write_req(
        &mut rig.writer,
        7,
        corr,
        serde_json::json!({"method":"session.send","params":params}),
    )
    .await;
    let reply = read_one(&mut rig.reader).await;
    (
        reply.header.ty,
        serde_json::from_slice(&reply.body).unwrap(),
    )
}

#[tokio::test]
async fn account_route_wire_refusals_write_nothing() {
    let mut rig = PrincipalRig::start("account-route-refusals").await;
    rig.bind(7, None).await;
    for bad in [
        serde_json::Value::Null,
        serde_json::json!("route"),
        serde_json::json!({}),
        serde_json::json!({"provider":"p"}),
        serde_json::json!({"provider":"","model":"m"}),
        serde_json::json!({"provider":"p","model":""}),
        serde_json::json!({"provider":5,"model":"m"}),
        serde_json::json!({"provider":"p","model":"m","extra":true}),
    ] {
        let reply = account_route_send(
            &mut rig,
            serde_json::json!({
                "send_id":"bad-route","prompt":"hello","model":{"provider":"fake","model":"fake"},
                "auth":{"method":"oauth"},"account_route":bad
            }),
        )
        .await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "invalid_params", "{reply:?}");
        assert_eq!(reply.1["detail"]["field"], "account_route", "{reply:?}");
        assert!(!rig.exists(), "invalid account_route created the session");
    }
    // Every known sign-in method takes account_route; a method broca doesn't know is refused.
    for method in ["unknown"] {
        let reply = account_route_send(
            &mut rig,
            serde_json::json!({
                "prompt":"hello","model":{"provider":"fake","model":"fake"},
                "auth":{"method":method},"account_route":{"provider":"p","model":"m"}
            }),
        )
        .await;
        assert_eq!(reply.0, FrameType::Error);
        assert_eq!(reply.1["code"], "invalid_params");
        assert_eq!(reply.1["detail"]["field"], "account_route");
        assert!(!rig.exists());
    }
    // broca declares message_marks, so speaks_for and notice are admitted when valid. Each of
    // these is a value the contract refuses, naming the field it names: speaks_for that is not
    // an object has no valid agent id, this unscoped session has no owner to speak for, and
    // notice must be a boolean.
    for (field, value, refused) in [
        ("speaks_for", serde_json::json!(true), "speaks_for.agent_id"),
        (
            "speaks_for",
            serde_json::json!({"agent_id":"Ada"}),
            "speaks_for",
        ),
        ("notice", serde_json::json!("true"), "notice"),
    ] {
        let mut params =
            serde_json::json!({"prompt":"hello","model":{"provider":"fake","model":"fake"}});
        params[field] = value;
        let reply = account_route_send(&mut rig, params).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "invalid_params", "{reply:?}");
        assert_eq!(reply.1["detail"]["field"], refused, "{reply:?}");
        assert!(!rig.exists());
    }
    rig.finish().await;
}

#[tokio::test]
async fn account_route_wire_joins_inherit_and_queue_freezes_its_own_route() {
    let mut rig = PrincipalRig::start("account-route-joining").await;
    rig.bind(7, None).await;
    let route = serde_json::json!({"provider":"routing-provider","model":"routing-model"});
    let base = serde_json::json!({"prompt":"hello","model":{"provider":"fake","model":"fake"}});
    let mut first = base.clone();
    first["send_id"] = "first-route".into();
    first["account_route"] = route.clone();
    let reply = account_route_send(&mut rig, first).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    let before = rig.records().await;
    assert!(rig
        .run_configs()
        .await
        .iter()
        .any(|config| serde_json::to_value(&config.account_route).unwrap() == route));
    for different in [
        serde_json::json!({"provider":"different","model":"routing-model"}),
        serde_json::json!({"provider":"routing-provider","model":"different"}),
    ] {
        let mut joining = base.clone();
        joining["send_id"] = "different-route".into();
        joining["delivery"] = "steer".into();
        joining["account_route"] = different;
        let reply = account_route_send(&mut rig, joining).await;
        assert_eq!(reply.0, FrameType::Error, "{reply:?}");
        assert_eq!(reply.1["code"], "invalid_params");
        assert_eq!(reply.1["detail"]["field"], "account_route");
        assert_eq!(rig.records().await, before);
    }
    for (send_id, explicit) in [("equal-route", true), ("absent-route", false)] {
        let mut joining = base.clone();
        joining["send_id"] = send_id.into();
        joining["delivery"] = "steer".into();
        if explicit {
            joining["account_route"] = route.clone();
        }
        let reply = account_route_send(&mut rig, joining).await;
        assert_eq!(reply.0, FrameType::Response, "{reply:?}");
        assert_eq!(reply.1["result"]["state"], "active");
    }
    let own_route = serde_json::json!({"provider":"queued-provider","model":"queued-model"});
    let mut queued = base;
    queued["send_id"] = "queued-route".into();
    queued["account_route"] = own_route.clone();
    let reply = account_route_send(&mut rig, queued).await;
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    assert_eq!(reply.1["result"]["state"], "pending");
    // Steering adds a follow-up model call; the queued run then makes its own call.
    for steps in [2, 3] {
        rig.gate.notify_one();
        broca_test_support::wait_until("account-route next model gate", || async {
            let actual = rig
                .records()
                .await
                .iter()
                .filter(|record| matches!(record, Record::StepStart(_)))
                .count();
            if actual >= steps {
                Ok(())
            } else {
                Err(format!("step starts={actual}, wanted={steps}"))
            }
        })
        .await;
    }
    rig.gate.notify_one();
    rig.wait_for_finished(2).await;
    let routes: Vec<_> = rig
        .run_configs()
        .await
        .iter()
        .map(|config| serde_json::to_value(&config.account_route).unwrap())
        .collect();
    assert_eq!(routes, vec![route, own_route]);
    rig.finish().await;
}
