//! The scope carrier's inbound half, through the real serve path: the daemon's scope stamp on
//! `route.bind`, the scope frozen on `RunStarted`, the owner check on every send, owner-matched
//! adoption (refused on the send reply and on the bind), and the busy refusal for a send that
//! would freeze a scope while the session is busy.
//!
//! Every run goes through the real core loop, so each `RunStarted` is written by core from the
//! request the serve layer built. The daemon side is played by the test: it sends the HELLO_ACK
//! (with or without `scopes/v1`) and stamps each bind the way the daemon does.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, Provider,
    RunError, RunSummary, StartMode, StubProvider, StubToolPlane,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock},
    error::{ErrorClass, ProviderError},
    provider::{NormalizedEvent, ProviderControl},
    BindIdentity, FinishReason, MessageId, Usage,
};
use broca_wal::WalRecord;
use subc_protocol::scope::{ScopeAttributes, ScopeKind, ScopeStamp, CAP_SCOPES_V1};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::{mpsc, Notify};

fn core() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn other() -> Principal {
    Principal::Reserved {
        module_id: "other-agent".into(),
    }
}

/// The stamp the daemon puts on a bind admitted under `owner`'s scope `head-1`.
fn stamp(owner: Principal, epoch: u64) -> ScopeStamp {
    ScopeStamp::new(owner, "head-1", epoch, ScopeKind::Head)
        // Admission ignores agent_id and delegates: they do not authorize sends to broca.
        // This test uses an ordinary scope, with no flow id.
        .with_attributes(
            ScopeAttributes::new()
                .with_agent_id(Some("agent-7".into()))
                .with_delegates(true),
        )
        .with_owner_authorized(true)
}

fn auth_required_error() -> ProviderError {
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Auth,
        message: "credential needs re-authentication".into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some("needs_reauth".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn one_step_text_reply() -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "answer".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A stub provider whose step waits on a gate, so a run stays active while the test sends.
/// `held` is signalled as the step reaches the gate: from then until the gate opens, the run
/// writes nothing more to its WAL. Every step records how many messages its prompt held.
struct GatedProvider {
    stub: StubProvider,
    gate: Option<(Arc<Notify>, Arc<Notify>)>,
    prompts: Arc<std::sync::Mutex<Vec<usize>>>,
}

#[async_trait]
impl Provider for GatedProvider {
    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.prompts.lock().unwrap().push(opts.prompt.len());
        if let Some((gate, held)) = &self.gate {
            held.notify_one();
            gate.notified().await;
        }
        self.stub.step(step_id, opts).await
    }
}

/// The real core loop. Only the first run waits on `gate`; with `pause_first`, the first run
/// instead pauses for auth at its start, as a run whose credential needs re-authentication does.
struct CoreExec {
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    gate: Arc<Notify>,
    held: Arc<Notify>,
    pause_first: bool,
    runs: AtomicUsize,
    prompts: Arc<std::sync::Mutex<Vec<usize>>>,
}

#[async_trait]
impl RunExecutor for CoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let first = self.runs.fetch_add(1, Ordering::SeqCst) == 0;
        let provider = GatedProvider {
            stub: StubProvider::new(vec![one_step_text_reply()]),
            gate: (first && !self.pause_first).then(|| (self.gate.clone(), self.held.clone())),
            prompts: self.prompts.clone(),
        };
        let materialized_store =
            job.materialized_store(self.store.clone() as Arc<dyn broca_core::SessionStore>);
        let mut control = job.control;
        if first && self.pause_first {
            control.pause_for_auth_at_start = Some(auth_required_error());
        }
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &StubToolPlane::new(),
            &AllowAll,
            self.wal_store.as_ref(),
            materialized_store,
            control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

struct Rig {
    dir: PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    session: BindIdentity,
    wal_path: PathBuf,
    gate: Arc<Notify>,
    held: Arc<Notify>,
    /// The number of messages in each model step's prompt, in call order.
    prompts: Arc<std::sync::Mutex<Vec<usize>>>,
    registry: Arc<LineageRegistry>,
    corr: u64,
}

impl Rig {
    /// Serve a fresh state root. `scopes` decides whether the daemon's HELLO_ACK advertises
    /// `scopes/v1`.
    async fn start(tag: &str, scopes: bool, pause_first: bool) -> Self {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-scope-carrier-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("proj")).unwrap();
        Self::open(dir, scopes, pause_first).await
    }

    /// Serve the state root at `dir` as it is on disk.
    async fn open(dir: PathBuf, scopes: bool, pause_first: bool) -> Self {
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        let gate = Arc::new(Notify::new());
        let held = Arc::new(Notify::new());
        let prompts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let exec = Arc::new(CoreExec {
            wal_store: wal.clone(),
            store: store.clone(),
            gate: gate.clone(),
            held: held.clone(),
            pause_first,
            runs: AtomicUsize::new(0),
            prompts: prompts.clone(),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                exec,
            )
            .with_run_index(store.clone())
            .with_materialized_store(store.clone()),
        );
        let registry = Arc::new(LineageRegistry::new_with_reader(
            opener,
            wal.clone(),
            Some(store.clone()),
        ));
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_core::ReadOnlyWalReader::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let serve_registry = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                serve_registry.clone(),
                serve_registry,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_frame(&mut reader).await.unwrap().unwrap();
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: if scopes {
                    vec![CAP_SCOPES_V1.to_owned()]
                } else {
                    vec![]
                },
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        let session = BindIdentity::new(dir.join("proj"), "runner", "scoped");
        let wal_path = wal.wal_path(&session);
        Self {
            dir,
            reader,
            writer,
            serve,
            session,
            wal_path,
            gate,
            held,
            prompts,
            registry,
            corr: 1,
        }
    }

    /// Stop serving as a module stopping gracefully does, so every session's writer lease is
    /// released, and return the state root, left on disk for [`Rig::open`].
    async fn close(self) -> PathBuf {
        let Self {
            dir,
            reader,
            writer,
            serve,
            registry,
            ..
        } = self;
        drop(writer);
        drop(reader);
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        assert!(
            registry.stop_and_seal().await,
            "the stop sealed every session"
        );
        drop(registry);
        dir
    }

    async fn request(&mut self, channel: u16, body: serde_json::Value) -> Frame {
        self.corr += 1;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            self.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), read_frame(&mut self.reader))
            .await
            .expect("a reply")
            .unwrap()
            .unwrap()
    }

    /// Bind `channel` to the rig's session as the daemon relays it: stamped with `principal`,
    /// and with `scope` when the route was admitted under one. Returns the daemon-facing reply.
    async fn bind(
        &mut self,
        channel: u16,
        principal: Option<Principal>,
        scope: Option<ScopeStamp>,
    ) -> Frame {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(self.session.project_root.clone(), "runner", "scoped"),
            principal,
            consumer_capabilities: None,
            admission_facts: None,
            scope,
            role_versions: None,
            call_mode: None,
        };
        self.request(0, serde_json::to_value(bind).unwrap()).await
    }

    async fn bind_ok(
        &mut self,
        channel: u16,
        principal: Option<Principal>,
        scope: Option<ScopeStamp>,
    ) {
        let reply = self.bind(channel, principal, scope).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }

    async fn send(&mut self, channel: u16, send_id: &str) -> Frame {
        self.send_as(channel, send_id, None).await
    }

    /// A send with `delivery` (`queue`, `steer` or `interrupt`) when it is `Some`.
    async fn send_as(&mut self, channel: u16, send_id: &str, delivery: Option<&str>) -> Frame {
        let mut params = serde_json::json!({
            "prompt":"hi", "send_id":send_id,
            "model":{"provider":"fake","model":"fake"}
        });
        if let Some(delivery) = delivery {
            params["delivery"] = delivery.into();
        }
        self.request(
            channel,
            serde_json::json!({"method":"session.send", "params": params}),
        )
        .await
    }

    /// A send that must be admitted; returns its `result`.
    async fn send_ok(&mut self, channel: u16, send_id: &str) -> serde_json::Value {
        let reply = self.send(channel, send_id).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap()["result"].clone()
    }

    /// A send that freezes or adopts a scope, retried while the session is still busy: the
    /// actor learns a run ended a moment after its terminal is durable, and `scope_freeze_busy`
    /// is retryable by contract. Any other refusal fails the test.
    async fn send_ok_once_idle(&mut self, channel: u16, send_id: &str) -> serde_json::Value {
        let reply = self.send_once_idle(channel, send_id).await;
        serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap()["result"].clone()
    }

    /// [`Rig::send_ok_once_idle`], returning the admitted reply's frame.
    async fn send_once_idle(&mut self, channel: u16, send_id: &str) -> Frame {
        let wal_path = self.wal_path.clone();
        broca_test_support::wait_for(
            "scope freeze admits send after idle",
            async {
                loop {
                    let reply = self.send(channel, send_id).await;
                    if reply.header.ty == FrameType::Error {
                        let (code, body) = error_body(&reply);
                        assert_eq!(code, "scope_freeze_busy", "{body}");
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    return reply;
                }
            },
            || {
                format!(
                    "WAL bytes={:?}",
                    std::fs::metadata(&wal_path).map(|m| m.len())
                )
            },
        )
        .await
    }

    /// Wait until the gated first run's provider has reached its gate.
    async fn wait_until_held(&self) {
        broca_test_support::event_timeout(
            "first provider reaches held gate",
            self.held.notified(),
            || broca_test_support::record_kinds(self.records()),
        )
        .await
        .expect("the first run's provider reaches its gate");
    }

    /// Wait until the WAL on disk holds the first run's step start.
    async fn wait_for_step_started(&self) {
        broca_test_support::wait_for(
            "durable StepStarted",
            async {
                loop {
                    if self
                        .records()
                        .iter()
                        .any(|record| matches!(record, WalRecord::StepStarted { .. }))
                    {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    fn wal_bytes(&self) -> Vec<u8> {
        std::fs::read(&self.wal_path).unwrap_or_default()
    }

    fn records(&self) -> Vec<WalRecord> {
        broca_wal::replay_file(&self.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
            .unwrap()
            .into_iter()
            .map(|frame| frame.record)
            .collect()
    }

    async fn wait_for_finished(&self, n: usize) -> Vec<WalRecord> {
        broca_test_support::wait_for(
            "durable RunFinished or RunPaused",
            async {
                loop {
                    let records = self.records();
                    let finished = records
                        .iter()
                        .filter(|record| {
                            matches!(
                                record,
                                WalRecord::RunFinished { .. } | WalRecord::RunPaused { .. }
                            )
                        })
                        .count();
                    if finished >= n {
                        return records;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
            || broca_test_support::record_kinds(self.records()),
        )
        .await
    }

    fn floor(&self) -> Option<String> {
        std::fs::read_to_string(self.dir.join(broca_wal::FEATURE_FLOOR_FILENAME)).ok()
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
        let _ = tokio::time::timeout(Duration::from_secs(5), serve).await;
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// The scopes each `RunStarted` froze, as `(owner, ref, epoch)`.
fn frozen_scopes(records: &[WalRecord]) -> Vec<Option<(String, String, u64)>> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { scope, .. } => Some(scope.as_ref().map(|scope| {
                (
                    scope.owner.as_owner_string(),
                    scope.scope_ref.clone(),
                    scope.scope_epoch,
                )
            })),
            _ => None,
        })
        .collect()
}

/// How many `RunStarted` records sit in gated frames requiring `scope/v1`, found by their
/// payload bytes: such a payload opens with the envelope, so an ordinary frame never matches.
fn gated_run_started_count(bytes: &[u8]) -> usize {
    // Newly admitted episodes carry computed tags and a durable input time beside the scope.
    let needle = br#"{"requires":["message-times/v1","runner-tags/v2","scope/v1"],"record":{"type":"run_started""#;
    bytes
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

fn error_body(frame: &Frame) -> (String, serde_json::Value) {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "expected a refusal, got {}",
        String::from_utf8_lossy(&frame.body)
    );
    let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
    (body["code"].as_str().unwrap().to_owned(), body)
}

fn golden(file: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../broca-wire/tests/golden")
            .join(file),
    )
    .unwrap()
}

/// The owner-mismatch refusal. The first stamped send freezes the owner's scope on a gated
/// `RunStarted` (after the WAL feature floor lists `scope/v1`); afterwards a send from another
/// principal under the same stamp, from the owner without a stamp, from no principal at all,
/// and from the owner under an earlier epoch of the same ref are each refused
/// `scope_owner_mismatch` by name, and none of them writes anything. (A later epoch of the same
/// ref reopens the session instead; see `a_later_epoch_of_the_owners_scope_reopens_an_idle_session`.)
#[tokio::test]
async fn a_scoped_session_refuses_a_send_under_a_different_identity_or_none() {
    let mut rig = Rig::start("owner-mismatch", true, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 3))).await;
    rig.gate.notify_one();
    let first = rig.send_ok(7, "turn-1").await;
    assert_eq!(first["state"], "active", "{first}");
    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        frozen_scopes(&records),
        [Some((
            "reserved:prefrontal-core".into(),
            "head-1".into(),
            3
        ))]
    );
    assert_eq!(gated_run_started_count(&rig.wal_bytes()), 1);
    assert_eq!(
        rig.floor().as_deref(),
        Some("message-times/v1\nrunner-tags/v2\nscope/v1\n")
    );
    let before = rig.wal_bytes();

    rig.bind_ok(8, Some(other()), Some(stamp(core(), 3))).await;
    rig.bind_ok(9, Some(core()), None).await;
    rig.bind_ok(10, None, None).await;
    rig.bind_ok(11, Some(core()), Some(stamp(core(), 2))).await;
    for (channel, presented) in [
        (8, serde_json::json!("reserved:other-agent")),
        (9, serde_json::json!("reserved:prefrontal-core")),
        (10, serde_json::Value::Null),
        (11, serde_json::json!("reserved:prefrontal-core")),
    ] {
        let reply = rig.send(channel, &format!("turn-{channel}")).await;
        let (code, body) = error_body(&reply);
        assert_eq!(code, "scope_owner_mismatch", "channel {channel}: {body}");
        assert_eq!(body["detail"]["kind"], "scoped", "{body}");
        assert_eq!(body["detail"]["owner"], "reserved:prefrontal-core");
        assert_eq!(body["detail"]["ref"], "head-1");
        assert_eq!(body["detail"]["scope_epoch"], 3);
        assert_eq!(body["detail"]["presented_principal"], presented, "{body}");
    }
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");

    // The owner under the frozen scope is still admitted, and its episode repeats the scope.
    // `pending` when the actor has not yet folded the first run's terminal into its state: the
    // send is then queued behind it and drained, which the next wait covers either way.
    let again = rig.send_ok(7, "turn-2").await;
    assert!(
        matches!(again["state"].as_str(), Some("active" | "pending")),
        "{again}"
    );
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        frozen_scopes(&records),
        [
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3)),
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3)),
        ]
    );
    assert_eq!(gated_run_started_count(&rig.wal_bytes()), 2);
    rig.finish().await;
}

/// A send into a scoped session over a daemon that does not advertise `scopes/v1` cannot carry
/// the owner's stamp at all, so it is refused `scope_unsupported`, not as an owner mismatch.
#[tokio::test]
async fn a_scoped_send_on_a_daemon_without_scopes_is_refused_as_unsupported() {
    let mut rig = Rig::start("unsupported", false, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 3))).await;
    rig.gate.notify_one();
    rig.send_ok(7, "turn-1").await;
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();

    rig.bind_ok(8, Some(core()), None).await;
    let (code, body) = error_body(&rig.send(8, "turn-2").await);
    assert_eq!(code, "scope_unsupported", "{body}");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

/// Owner-matched adoption, on the send reply and on the bind, for the three histories an
/// unscoped session can have. The stamped route is bound while the session is still empty (so
/// the bind is admitted), then an unscoped first send records its principal, then:
/// - every episode recorded the stamp's owner: the stamped send adopts, and its `RunStarted`
///   freezes the scope in a gated frame;
/// - an episode recorded no principal: `scope_adoption_refused {no_recorded_principal}`;
/// - the episodes recorded another principal: `principal_mismatch {recorded}`.
///
/// A refusal carries exactly the pinned golden bytes on the send reply, a new stamped bind
/// is refused with the same bytes, and a refusal writes nothing.
#[tokio::test]
async fn adoption_follows_the_recorded_principals_on_the_send_reply_and_the_bind() {
    // Every episode recorded the stamp's owner.
    let mut rig = Rig::start("adopt-recorded", true, false).await;
    rig.bind_ok(8, Some(core()), Some(stamp(core(), 3))).await;
    rig.bind_ok(7, Some(core()), None).await;
    rig.gate.notify_one();
    rig.send_ok(7, "unscoped-1").await;
    rig.wait_for_finished(1).await;
    let adopted = rig.send_ok_once_idle(8, "scoped-1").await;
    assert_eq!(adopted["state"], "active", "{adopted}");
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        frozen_scopes(&records),
        [
            None,
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3))
        ]
    );
    assert_eq!(gated_run_started_count(&rig.wal_bytes()), 1);
    // Once adopted, the session is scoped: a stamped bind is admitted.
    rig.bind_ok(9, Some(core()), Some(stamp(core(), 3))).await;
    rig.finish().await;

    // An episode recorded no principal, and a different principal was recorded.
    for (tag, first_principal, owner, golden_file) in [
        (
            "adopt-unrecorded",
            None,
            core(),
            "scope_adoption_refused.no_recorded_principal.json",
        ),
        (
            "adopt-mismatch",
            Some(core()),
            other(),
            "scope_adoption_refused.principal_mismatch.json",
        ),
    ] {
        let mut rig = Rig::start(tag, true, false).await;
        rig.bind_ok(8, Some(owner.clone()), Some(stamp(owner.clone(), 3)))
            .await;
        rig.bind_ok(7, first_principal, None).await;
        rig.gate.notify_one();
        rig.send_ok(7, "unscoped-1").await;
        rig.wait_for_finished(1).await;
        let before = rig.wal_bytes();

        let refused = rig.send(8, "scoped-1").await;
        let (code, _) = error_body(&refused);
        assert_eq!(code, "scope_adoption_refused");
        assert_eq!(refused.body, golden(golden_file), "{tag}: the send reply");

        let bind = rig
            .bind(9, Some(owner.clone()), Some(stamp(owner.clone(), 3)))
            .await;
        let (code, _) = error_body(&bind);
        assert_eq!(code, "scope_adoption_refused");
        assert_eq!(bind.body, golden(golden_file), "{tag}: the bind");
        // The daemon decodes an Error frame answering route.bind as subc's ErrorBody and hands
        // that body to the waiting route.open unchanged; a body it cannot decode would make it
        // treat the bind as abandoned instead. So the refusal must be a valid ErrorBody whose
        // code and detail are the pinned ones.
        let decoded: subc_protocol::ErrorBody =
            serde_json::from_slice(&bind.body).expect("the bind refusal is a subc ErrorBody");
        assert_eq!(decoded.code, broca_wire::SCOPE_ADOPTION_REFUSED);
        let pinned: broca_wire::ScopeAdoptionRefused =
            serde_json::from_slice(&golden(golden_file)).unwrap();
        assert_eq!(
            decoded.detail,
            Some(serde_json::to_value(&pinned.detail).unwrap()),
            "{tag}: the bind refusal's detail"
        );
        assert_eq!(
            rig.wal_bytes(),
            before,
            "{tag}: a refused adoption writes nothing"
        );
        // The unscoped episode's runner tags raised the floor; the refusal added no scope.
        assert_eq!(
            rig.floor().as_deref(),
            Some("message-times/v1\nrunner-tags/v2\n"),
            "{tag}: nothing scoped was written"
        );
        rig.finish().await;
    }
}

/// An auth-paused run's resend (the same `send_id`) that meets the adoption refusal gets it
/// again, byte for byte, every time, and never resumes the paused run: the owner check runs
/// before a keyed retry is answered.
#[tokio::test]
async fn an_auth_paused_resend_meets_the_same_refusal_byte_for_byte() {
    let mut rig = Rig::start("paused-resend", true, true).await;
    rig.bind_ok(8, Some(core()), Some(stamp(core(), 3))).await;
    rig.bind_ok(7, None, None).await;
    let paused = rig.send_ok(7, "paused-turn").await;
    assert_eq!(paused["state"], "active", "{paused}");
    let records = rig.wait_for_finished(1).await;
    assert!(
        records
            .iter()
            .any(|record| matches!(record, WalRecord::RunPaused { .. })),
        "the first run must pause for auth: {records:?}"
    );
    let before = rig.wal_bytes();

    let first = rig.send(8, "paused-turn").await;
    let second = rig.send(8, "paused-turn").await;
    assert_eq!(error_body(&first).0, "scope_adoption_refused");
    assert_eq!(
        first.body, second.body,
        "the resend must meet the same bytes"
    );
    assert_eq!(
        first.body,
        golden("scope_adoption_refused.no_recorded_principal.json")
    );
    assert_eq!(
        rig.wal_bytes(),
        before,
        "the refused resend resumed nothing"
    );
    rig.finish().await;
}

/// A send that would adopt its scope while the session's run is active is refused
/// `scope_freeze_busy` with the active run named, in exactly the pinned wire type's bytes, and
/// writes nothing (no `Queued` record). Once the session is idle the same send adopts.
#[tokio::test]
async fn a_send_that_would_freeze_while_a_run_is_active_is_refused_busy() {
    let mut rig = Rig::start("freeze-busy", true, false).await;
    rig.bind_ok(8, Some(core()), Some(stamp(core(), 3))).await;
    rig.bind_ok(7, Some(core()), None).await;
    let held = rig.send_ok(7, "held").await;
    assert_eq!(held["state"], "active", "{held}");
    let run_id = held["run_id"].as_str().unwrap().to_owned();
    // The send reply comes back once the run is admitted, but the run then records its step
    // start before calling the provider. That record goes through the writer's queue, so it can
    // still be on its way to disk when the provider reaches the gate. Snapshot only once the
    // provider is held AND the step start is on disk: from then until the gate opens the run
    // writes nothing, so the comparison below sees only what the refusal writes.
    rig.wait_until_held().await;
    rig.wait_for_step_started().await;
    let before = rig.wal_bytes();

    let busy = rig.send(8, "adopting").await;
    let expected = serde_json::to_vec(&broca_wire::ScopeFreezeBusy::new(
        broca_wire::ScopeFreezeBusyDetail::RunActive { run_id },
    ))
    .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&busy.body),
        String::from_utf8_lossy(&expected)
    );
    assert_eq!(error_body(&busy).0, "scope_freeze_busy");
    assert_eq!(rig.wal_bytes(), before, "a busy refusal writes nothing");
    assert!(!rig
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::Queued { .. })));

    rig.gate.notify_one();
    rig.wait_for_finished(1).await;
    let adopted = rig.send_ok_once_idle(8, "adopting").await;
    assert_eq!(adopted["state"], "active", "{adopted}");
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        frozen_scopes(&records),
        [
            None,
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3))
        ]
    );
    rig.finish().await;
}

/// In a session that is already scoped, the owner's send while a run is active is queued and
/// its drained episode carries the frozen scope; a send under a different identity is refused
/// when it would be queued, not at drain, and leaves no `Queued` record.
#[tokio::test]
async fn queued_sends_in_a_scoped_session_are_checked_when_queued_and_drain_scoped() {
    let mut rig = Rig::start("queued-scoped", true, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 3))).await;
    rig.bind_ok(8, Some(other()), Some(stamp(core(), 3))).await;
    let held = rig.send_ok(7, "held").await;
    assert_eq!(held["state"], "active", "{held}");
    let queued = rig.send_ok(7, "queued").await;
    assert_eq!(queued["state"], "pending", "{queued}");
    let (code, _) = error_body(&rig.send(8, "intruder").await);
    assert_eq!(code, "scope_owner_mismatch");
    let queued_records = rig
        .records()
        .into_iter()
        .filter(|record| matches!(record, WalRecord::Queued { .. }))
        .count();
    assert_eq!(queued_records, 1, "only the owner's send was queued");

    rig.gate.notify_one();
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        frozen_scopes(&records),
        [
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3)),
            Some(("reserved:prefrontal-core".into(), "head-1".into(), 3)),
        ],
        "the drained episode carries the frozen scope"
    );
    assert_eq!(gated_run_started_count(&rig.wal_bytes()), 2);
    rig.finish().await;
}

/// An unscoped session accepts later sends only from the principal of its first send: another
/// principal, or none, is refused by name. The first send's principal is still admitted.
#[tokio::test]
async fn an_unscoped_session_accepts_later_sends_only_from_its_first_principal() {
    let mut rig = Rig::start("first-principal", true, false).await;
    rig.bind_ok(7, Some(core()), None).await;
    rig.bind_ok(8, Some(other()), None).await;
    rig.bind_ok(9, None, None).await;
    rig.gate.notify_one();
    rig.send_ok(7, "turn-1").await;
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    for (channel, presented) in [
        (8, serde_json::json!("reserved:other-agent")),
        (9, serde_json::Value::Null),
    ] {
        let (code, body) = error_body(&rig.send(channel, "turn-2").await);
        assert_eq!(code, "scope_owner_mismatch", "{body}");
        assert_eq!(body["detail"]["kind"], "first_principal");
        assert_eq!(body["detail"]["first"], "reserved:prefrontal-core");
        assert_eq!(body["detail"]["presented_principal"], presented);
    }
    assert_eq!(rig.wal_bytes(), before);
    let again = rig.send_ok(7, "turn-2").await;
    assert!(
        matches!(again["state"].as_str(), Some("active" | "pending")),
        "{again}"
    );
    rig.finish().await;
}

#[test]
fn scope_stamp_decode_retains_flow_id() {
    let mut value = serde_json::to_value(stamp(core(), 1)).unwrap();
    value["attributes"]["flow_id"] = serde_json::json!("flow-1");

    let decoded: ScopeStamp = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.attributes.flow_id.as_deref(), Some("flow-1"));
}

#[tokio::test]
async fn basal_unscoped_delivery_refusal_preserves_wal_bytes() {
    let mut rig = Rig::start("basal-unscoped", true, false).await;
    rig.bind_ok(1, Some(core()), None).await;
    rig.send_ok(1, "owner-first").await;
    rig.wait_until_held().await;
    // The run hands its step start to the WAL writer before calling the provider, so the
    // provider can reach its gate before that record is on disk. Take the before-snapshot only
    // once it is, or the late step start reads as a refused send writing to the WAL.
    rig.wait_for_step_started().await;
    let before = rig.wal_bytes();
    rig.bind_ok(
        2,
        Some(Principal::Reserved {
            module_id: "basal".into(),
        }),
        None,
    )
    .await;
    for delivery in ["queue", "steer", "interrupt"] {
        let reply = rig
            .request(
                2,
                serde_json::json!({"method":"session.send", "params": {
                    "prompt":"hi", "delivery":delivery, "model":{"provider":"fake", "model":"fake"}
                }}),
            )
            .await;
        let (code, body) = error_body(&reply);
        assert_eq!(code, "flow_scope_required", "{body}");
        assert_eq!(
            rig.wal_bytes(),
            before,
            "refused {delivery} changed the WAL"
        );
    }
    rig.gate.notify_one();
    rig.wait_for_finished(1).await;
    rig.finish().await;
}

/// A stamp under `owner`'s scope with every identity member given: the ref, the epoch and the
/// flow identifier.
fn stamp_of(owner: Principal, scope_ref: &str, epoch: u64, flow_id: Option<&str>) -> ScopeStamp {
    let mut stamp = stamp(owner, epoch);
    stamp.scope_ref = scope_ref.into();
    stamp.attributes.flow_id = flow_id.map(Into::into);
    stamp
}

/// Compare a reply's bytes with the golden `file`, with `run_id` (when given) standing as
/// `@RUN_ID@`: a run id is derived from the session's per-process scratch path, so it cannot be
/// pinned. `sha256` pins the golden file itself. `BROCA_UPDATE_GOLDEN=1` rewrites the file from
/// this reply, which came through the real serve path.
pub(crate) fn assert_reply_golden(frame: &Frame, file: &str, run_id: Option<&str>, sha256: &str) {
    use sha2::{Digest, Sha256};
    let mut body = String::from_utf8(frame.body.clone()).unwrap();
    if let Some(run_id) = run_id {
        body = body.replace(run_id, "@RUN_ID@");
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(file);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &body).unwrap();
    }
    let pinned = std::fs::read(&path)
        .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
    let digest = format!("{:x}", Sha256::digest(&pinned));
    println!("golden {file} sha256 {digest}");
    assert_eq!(
        body,
        String::from_utf8(pinned).unwrap(),
        "the reply bytes pinned in {file}"
    );
    assert_eq!(digest, sha256, "golden file {file} changed");
}

const REOPENED_REPLY_SHA256: &str =
    "b41d0ec62f90d1db8bff4562da0e2c31fa58dd111ca9cb31a6bd63b7a9bf9470";
const REOPEN_STEER_REFUSAL_SHA256: &str =
    "eec2329c3c4d4145408534724d9c1de36248e5974d591d68bff5952f63efde4f";
const REOPEN_BUSY_SHA256: &str = "b8ee3557caff3e0b6e5d105b96a7c6b8d05f3a5b7c02338a22a901b43ae36450";

fn run_id_of(frame: &Frame) -> String {
    serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn head(epoch: u64) -> Option<(String, String, u64)> {
    Some(("reserved:prefrontal-core".into(), "head-1".into(), epoch))
}

/// The specimen: the owner reopened its scope at the next epoch after idle expiry ended the
/// first, and sends into the idle session under it. The send is admitted in the SAME session:
/// its episode's `RunStarted` records the new epoch and its prompt holds the first turn. From
/// then on the new epoch is the frozen one, so the ended epoch's route is refused and the new
/// one keeps sending.
#[tokio::test]
async fn a_later_epoch_of_the_owners_scope_reopens_an_idle_session() {
    let mut rig = Rig::start("reopen", true, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    rig.gate.notify_one();
    let first = rig.send_ok(7, "turn-1").await;
    assert_eq!(first["state"], "active", "{first}");
    rig.wait_for_finished(1).await;

    rig.bind_ok(8, Some(core()), Some(stamp(core(), 2))).await;
    let reopened = rig.send_once_idle(8, "turn-2").await;
    let run_id = run_id_of(&reopened);
    assert_reply_golden(
        &reopened,
        "send_reply.scope_reopened.json",
        Some(&run_id),
        REOPENED_REPLY_SHA256,
    );
    let records = rig.wait_for_finished(2).await;
    assert_eq!(frozen_scopes(&records), [head(1), head(2)]);
    assert_eq!(gated_run_started_count(&rig.wal_bytes()), 2);
    let prompts = rig.prompts.lock().unwrap().clone();
    assert_eq!(prompts.len(), 2, "one model step per episode: {prompts:?}");
    assert_eq!(
        prompts[1],
        prompts[0] + 2,
        "the reopened episode's prompt holds the first turn's question and answer: {prompts:?}"
    );

    let before = rig.wal_bytes();
    let (code, body) = error_body(&rig.send(7, "turn-3").await);
    assert_eq!(code, "scope_owner_mismatch", "{body}");
    assert_eq!(body["detail"]["scope_epoch"], 2, "{body}");
    assert_eq!(
        body["detail"]["presented_scope"]["scope_epoch"], 1,
        "{body}"
    );
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");

    let again = rig.send_ok_once_idle(8, "turn-3").await;
    assert!(
        matches!(again["state"].as_str(), Some("active" | "pending")),
        "{again}"
    );
    let records = rig.wait_for_finished(3).await;
    assert_eq!(frozen_scopes(&records), [head(1), head(2), head(2)]);
    rig.finish().await;
}

/// Only a strictly later epoch with the same owner, ref and flow identifier reopens. An equal
/// epoch under another ref, an earlier epoch, another owner, an added flow identifier, and a
/// later epoch from someone other than the owner are each refused `scope_owner_mismatch` naming
/// the frozen scope, writing nothing. On a flow session a later epoch under another flow
/// identifier keeps its typed flow refusal, and the same flow under a later epoch reopens.
#[tokio::test]
async fn an_equal_or_lower_epoch_or_another_scope_identity_is_still_refused() {
    let mut rig = Rig::start("reopen-refused", true, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 3))).await;
    rig.gate.notify_one();
    rig.send_ok(7, "turn-1").await;
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();

    let refused = [
        (8, Some(core()), stamp_of(core(), "head-2", 3, None)),
        (9, Some(core()), stamp(core(), 2)),
        (10, Some(core()), stamp(other(), 4)),
        (
            11,
            Some(core()),
            stamp_of(core(), "head-1", 4, Some("flow-1")),
        ),
        (12, Some(other()), stamp(core(), 4)),
    ];
    for (channel, principal, stamp) in refused.clone() {
        rig.bind_ok(channel, principal, Some(stamp)).await;
    }
    for (channel, _, _) in refused {
        let (code, body) = error_body(&rig.send(channel, &format!("turn-{channel}")).await);
        assert_eq!(code, "scope_owner_mismatch", "channel {channel}: {body}");
        assert_eq!(body["detail"]["kind"], "scoped", "{body}");
        assert_eq!(body["detail"]["ref"], "head-1", "{body}");
        assert_eq!(body["detail"]["scope_epoch"], 3, "{body}");
    }
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");
    rig.finish().await;

    let mut rig = Rig::start("reopen-flow", true, false).await;
    rig.bind_ok(
        7,
        Some(other()),
        Some(stamp_of(core(), "head-1", 3, Some("flow-1"))),
    )
    .await;
    rig.gate.notify_one();
    rig.send_ok(7, "flow-1").await;
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    rig.bind_ok(
        8,
        Some(other()),
        Some(stamp_of(core(), "head-1", 4, Some("flow-2"))),
    )
    .await;
    let (code, body) = error_body(&rig.send(8, "flow-2").await);
    assert_eq!(code, "scope_flow_id_mismatch", "{body}");
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");
    rig.bind_ok(
        9,
        Some(other()),
        Some(stamp_of(core(), "head-1", 4, Some("flow-1"))),
    )
    .await;
    let reopened = rig.send_ok_once_idle(9, "flow-reopened").await;
    assert_eq!(reopened["state"], "active", "{reopened}");
    let records = rig.wait_for_finished(2).await;
    let flows: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted {
                scope: Some(scope), ..
            } => Some((
                scope.scope_epoch,
                scope.flow_id.clone(),
                scope.sender.as_ref().map(|sender| sender.as_owner_string()),
            )),
            _ => None,
        })
        .collect();
    let flow = |epoch| {
        (
            epoch,
            Some("flow-1".to_owned()),
            Some("reserved:other-agent".to_owned()),
        )
    };
    assert_eq!(flows, [flow(3), flow(4)]);
    rig.finish().await;
}

/// A reopening send must open its own episode at once, because only that episode's
/// `RunStarted` can record the new epoch. With the old epoch's run active:
/// - a steer, which would join that run (its tool routes belong to the ended epoch), and a
///   retry of that run's own send are refused `scope_owner_mismatch`, as before;
/// - a queued send and an interrupt would wait in the queue, where nothing records their
///   scope, so both are refused `scope_freeze_busy` naming the run; the interrupt cancels
///   nothing.
///
/// None of them writes anything. Once the run ends, the same route reopens the session.
#[tokio::test]
async fn a_reopen_that_cannot_open_its_own_episode_now_is_refused_and_writes_nothing() {
    let mut rig = Rig::start("reopen-busy", true, false).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    let held = rig.send_ok(7, "held").await;
    assert_eq!(held["state"], "active", "{held}");
    let run_id = held["run_id"].as_str().unwrap().to_owned();
    // Snapshot only once the provider is held and the step start is on disk; see
    // `a_send_that_would_freeze_while_a_run_is_active_is_refused_busy`.
    rig.wait_until_held().await;
    rig.wait_for_step_started().await;
    let before = rig.wal_bytes();

    rig.bind_ok(8, Some(core()), Some(stamp(core(), 2))).await;
    let steer = rig.send_as(8, "steer-2", Some("steer")).await;
    let (code, body) = error_body(&steer);
    assert_eq!(code, "scope_owner_mismatch", "{body}");
    assert_eq!(body["detail"]["scope_epoch"], 1, "{body}");
    assert_eq!(
        body["detail"]["presented_scope"]["scope_epoch"], 2,
        "{body}"
    );
    assert_reply_golden(
        &steer,
        "scope_owner_mismatch.reopen_steer.json",
        None,
        REOPEN_STEER_REFUSAL_SHA256,
    );
    let (code, body) = error_body(&rig.send(8, "held").await);
    assert_eq!(code, "scope_owner_mismatch", "the run's own retry: {body}");

    let queued = rig.send_as(8, "queued-2", Some("queue")).await;
    assert_eq!(error_body(&queued).0, "scope_freeze_busy");
    assert_reply_golden(
        &queued,
        "scope_freeze_busy.reopen_run_active.json",
        Some(&run_id),
        REOPEN_BUSY_SHA256,
    );
    let interrupt = rig.send_as(8, "interrupt-2", Some("interrupt")).await;
    assert_eq!(
        String::from_utf8_lossy(&interrupt.body),
        String::from_utf8_lossy(&queued.body),
        "an interrupt is refused busy like a queued send"
    );
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");

    rig.gate.notify_one();
    let records = rig.wait_for_finished(1).await;
    assert!(
        records.iter().any(|record| matches!(
            record,
            WalRecord::RunFinished {
                reason: broca_protocol::caller::RunFinishReason::Completed,
                ..
            }
        )),
        "the refused interrupt cancelled nothing: {records:?}"
    );
    let reopened = rig.send_ok_once_idle(8, "after").await;
    assert_eq!(reopened["state"], "active", "{reopened}");
    let records = rig.wait_for_finished(2).await;
    assert_eq!(frozen_scopes(&records), [head(1), head(2)]);
    rig.finish().await;
}

/// Turn the auth pause that ends the WAL at `wal_path` into a restart pause, as a graceful stop
/// leaves a run it paused at a boundary: the last frame is rewritten with the same sequence and
/// fence.
fn rewrite_last_pause_as_restart(wal_path: &Path) {
    let bytes = std::fs::read(wal_path).unwrap();
    let mut offset = 0;
    let mut last = None;
    while offset < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                last = Some((offset, framed));
                offset += consumed;
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            _ => panic!("the WAL decodes to its end"),
        }
    }
    let (start, framed) = last.expect("a record");
    let WalRecord::RunPaused {
        run_id,
        ts_ms,
        error,
        ..
    } = framed.record
    else {
        panic!("the WAL ends with a pause: {:?}", framed.record);
    };
    let restart = WalRecord::RunPaused {
        run_id,
        ts_ms,
        reason: broca_protocol::PauseReason::Restart,
        error,
    };
    let mut rewritten = bytes[..start].to_vec();
    rewritten.extend(broca_wal::framing::encode(framed.seq, framed.fence, &restart).unwrap());
    std::fs::write(wal_path, rewritten).unwrap();
}

/// Behind a restart-paused run of the old epoch, a reopening queued send or interrupt would wait
/// in the queue, so each is refused `scope_freeze_busy` naming the paused run, and a steer
/// would join that run, so it is refused `scope_owner_mismatch`. None writes anything.
#[tokio::test]
async fn a_reopen_behind_a_restart_paused_run_is_refused_and_writes_nothing() {
    let mut rig = Rig::start("reopen-restart-paused", true, true).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    let paused = rig.send_ok(7, "paused-turn").await;
    let run_id = paused["run_id"].as_str().unwrap().to_owned();
    let records = rig.wait_for_finished(1).await;
    assert!(
        matches!(records.last(), Some(WalRecord::RunPaused { .. })),
        "the first run pauses: {records:?}"
    );
    let wal_path = rig.wal_path.clone();
    let dir = rig.close().await;
    rewrite_last_pause_as_restart(&wal_path);
    // The store's admission checkpoint still names the auth pause at the same sequence; without
    // it the session is read from the rewritten WAL alone.
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        if entry.file_name().to_string_lossy().starts_with("store.db") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    let mut rig = Rig::open(dir, true, false).await;
    rig.gate.notify_one();
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    let status = rig
        .request(
            7,
            serde_json::json!({"method": "run.status", "params": {"run_id": run_id}}),
        )
        .await;
    let status: serde_json::Value = serde_json::from_slice(&status.body).unwrap();
    assert_eq!(status["result"]["pause_reason"], "restart", "{status}");
    let before = rig.wal_bytes();

    rig.bind_ok(8, Some(core()), Some(stamp(core(), 2))).await;
    for delivery in ["queue", "interrupt"] {
        let (code, body) = error_body(&rig.send_as(8, delivery, Some(delivery)).await);
        assert_eq!(code, "scope_freeze_busy", "{delivery}: {body}");
        assert_eq!(
            body["detail"]["run_id"],
            run_id.as_str(),
            "{delivery}: {body}"
        );
    }
    let (code, body) = error_body(&rig.send_as(8, "steer", Some("steer")).await);
    assert_eq!(code, "scope_owner_mismatch", "{body}");
    assert_eq!(rig.wal_bytes(), before, "a refused send must write nothing");
    rig.finish().await;
}

#[tokio::test]
async fn role_describe_without_optional_handler_writers_matches_build_switches() {
    #[derive(serde::Deserialize)]
    struct RawReply<'a> {
        #[serde(borrow)]
        result: &'a serde_json::value::RawValue,
    }
    let mut rig = Rig::start("reader-describe", false, false).await;
    rig.bind_ok(7, None, None).await;
    let reply = rig
        .request(7, serde_json::json!({"method":"role.describe","params":{}}))
        .await;
    assert_eq!(reply.header.ty, FrameType::Response);
    let raw: RawReply<'_> = serde_json::from_slice(&reply.body).unwrap();
    let value: serde_json::Value = serde_json::from_str(raw.result.get()).unwrap();
    let describe = cortexkit_role_llm_runner::describe::check_describe(&value).unwrap();
    for group in ["run_status", "run_cancel", "retract"] {
        assert!(describe.declares(group), "{group}");
    }
    assert!(!describe.declares("call-modes/v1"));
    assert!(!describe.declares("compaction"));
    assert!(!describe.declares("ordered_hook_phases"));
    assert_eq!(
        describe.declares("retention"),
        broca_module_serve::RETENTION_WRITER_ENABLED
    );
    // The reader golden represents a build with the retention writer disabled.
    // A writer build checks discovery above but must not overwrite that reader answer.
    if broca_module_serve::RETENTION_WRITER_ENABLED {
        assert!(
            std::env::var_os("BROCA_REGENERATE_ROLE_DESCRIBE_GOLDEN").is_none(),
            "disable the retention writer before regenerating its reader golden"
        );
        rig.finish().await;
        return;
    }
    let version = serde_json::to_string(value["implementation_version"].as_str().unwrap()).unwrap();
    let old = format!("\"implementation_version\":{version}");
    assert_eq!(raw.result.get().matches(&old).count(), 1);
    // Keep served member order; the build version alone is not part of the contract.
    let served = raw.result.get().replacen(
        &old,
        "\"implementation_version\":\"0.0.0-reader-golden\"",
        1,
    );
    let golden = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/role_describe_reader.json");
    if std::env::var_os("BROCA_REGENERATE_ROLE_DESCRIBE_GOLDEN").is_some() {
        std::fs::write(&golden, served.as_bytes()).unwrap();
    }
    assert_eq!(served.as_bytes(), std::fs::read(golden).unwrap());
    rig.finish().await;
}

#[tokio::test]
async fn unknown_run_status_and_paused_cancel_match_the_role_contract() {
    let mut rig = Rig::start("cancel-paused-role", true, true).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    let unknown = rig
        .request(
            7,
            serde_json::json!({"method":"run.status","params":{"run_id":"absent"}}),
        )
        .await;
    assert_eq!(unknown.header.ty, FrameType::Response);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unknown.body).unwrap()["result"],
        serde_json::json!({"state":"unknown"})
    );
    let active = rig.send_ok(7, "paused-turn").await;
    let run_id = active["run_id"].as_str().unwrap();
    let records = rig.wait_for_finished(1).await;
    assert!(records
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));
    let cancel = rig
        .request(
            7,
            serde_json::json!({"method":"run.cancel","params":{"run_id":run_id}}),
        )
        .await;
    assert_eq!(cancel.header.ty, FrameType::Response);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&cancel.body).unwrap()["result"],
        serde_json::json!("ack")
    );
    rig.finish().await;
}

#[tokio::test]
async fn unlisted_cancel_points_refuse_at_without_changing_the_run() {
    let mut rig = Rig::start("cancel-unlisted", true, true).await;
    rig.bind_ok(7, Some(core()), Some(stamp(core(), 1))).await;
    let before = rig.wal_bytes();
    for at in [
        serde_json::json!("later"),
        serde_json::json!(0),
        serde_json::Value::Null,
    ] {
        let reply = rig
            .request(
                7,
                serde_json::json!({"method":"run.cancel","params":{"run_id":"absent","at":at}}),
            )
            .await;
        assert_eq!(reply.header.ty, FrameType::Error);
        let error: subc_protocol::ErrorBody = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error.code, "invalid_params");
        assert_eq!(error.detail.unwrap()["field"], "at");
        assert_eq!(
            rig.wal_bytes(),
            before,
            "refused cancellation must write nothing"
        );
    }
    let head = rig
        .request(7, serde_json::json!({"method":"session.head","params":{}}))
        .await;
    assert_eq!(head.header.ty, FrameType::Response);
    rig.finish().await;
}
