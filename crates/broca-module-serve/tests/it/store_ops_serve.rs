//! Operations a session kept in the new storage serves, reached through the real serve path:
//! each request goes over the wire into `serve_connection`, past its refusal of ops the new
//! storage doesn't serve, into the registry and on to the store. The session is created in the
//! new storage through the same registry the connection serves, by a send the store actor runs
//! over a scripted provider; it has no WAL, so whatever a request reads, it read from the store.
//!
//! The daemon side is played by the test: it sends the HELLO_ACK and binds each route.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary, WalStore,
};
use broca_module::actor::{SendKey, SendOutcome, SendPlan};
use broca_module::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest, Usage,
};
use serde_json::{json, Value};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::mpsc;

/// The compaction provider the planned session's plan names.
const PROVIDER: &str = "ck-mc";

fn core() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn provider() -> Principal {
    Principal::Reserved {
        module_id: PROVIDER.into(),
    }
}

/// A compaction provider that is always ready and never hands over a view.
struct ReadyProvider;

#[async_trait]
impl broca_core::CompactionPlane for ReadyProvider {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => json!({"answer": "nothing"}),
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// Runs a store session's runs through the new loop over a scripted provider, as the module's
/// executor runs them. A run whose plan names a compaction provider freezes the compaction plan
/// version and a context limit, as the module's executor does, so its session is one whose
/// `compaction-runner/v1` route admits that provider.
struct StoreLoop {
    provider: broca_core::provider::StubProvider,
    compaction: Arc<ReadyProvider>,
}

impl StoreLoop {
    fn compacts(control: &broca_core::RunControl) -> bool {
        control
            .manifest
            .as_deref()
            .is_some_and(|manifest| matches!(broca_core::compaction::item(manifest), Ok(Some(_))))
    }
}

#[async_trait]
impl RunExecutor for StoreLoop {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("every session here is kept in the new storage")
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(self.compaction.clone() as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        use broca_session_loop::writer::Writer;
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        if Self::compacts(&control) {
            control.new_log_compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
            control.admitted_context_limit = Some(200_000);
        }
        let result = async {
            let tools = broca_core::StubToolPlane::new().with_canned_text("read", "file contents");
            let now = || 1;
            let seams = broca_session_loop::run::Seams {
                provider: &self.provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: None,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
            };
            match mode {
                StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &self.provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                        .await
                }
                StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                _ => broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await,
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// A send carrying no tools of its own, as a planned session's sends do.
fn request(text: &str) -> SubmissionRequest {
    serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "stop_when": [{"type": "step_count", "n": 3}],
    }))
    .unwrap()
}

/// A plan naming the compaction provider [`PROVIDER`].
fn compacting_plan() -> SendPlan {
    let plan = json!({"compaction_item": {"provider": PROVIDER, "preset": "default"}});
    SendPlan {
        identity: "plan-1".into(),
        manifest: Some(Arc::new(broca_wal::PlanManifest {
            rich_ui: None,
            plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
            plan_identity: "plan-1".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(Vec::new()),
            system_text: "you are a worker".into(),
            system_digest: "system".into(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        })),
        model_check: None,
    }
}

struct Rig {
    dir: PathBuf,
    registry: Arc<LineageRegistry>,
    /// The session the test binds, as the registry resolves the bind's identity.
    key: BindIdentity,
    wal: Arc<FileWalStore>,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    _serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    corr: u64,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    /// A module on a fresh state root whose new storage creates sessions, its runs scripted by
    /// `steps`.
    async fn start(tag: &str, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-store-ops-serve-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor: Arc<dyn RunExecutor> = Arc::new(StoreLoop {
            provider: broca_core::provider::StubProvider::new(steps),
            compaction: Arc::new(ReadyProvider),
        });
        let runtime = broca_module::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            executor.clone(),
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal.clone(),
            executor,
        )
        .with_idle_ttl(Duration::from_millis(100))
        .with_store_runtime(Arc::new(runtime));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None)
            .with_store_new_sessions();
        // A bind resolves the session's address through the deployment's inventory, which
        // today's store keeps.
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_core::ReadOnlyWalReader::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let registry = Arc::new(registry);
        // The identity every bind below names, resolved as the serve path resolves it.
        let key = registry
            .resolve_session_address(&BindIdentity::new("", "runner", "head"), None)
            .await
            .unwrap();
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let served = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                served.clone(),
                served,
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
                subc_capabilities: vec![],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        Self {
            dir,
            registry,
            key,
            wal,
            reader,
            writer,
            _serve: serve,
            corr: 1,
        }
    }

    /// Sends `text` to the session through the registry the connection serves, and waits until
    /// its run has ended.
    async fn send(&self, run_id: &str, text: &str, plan: Option<SendPlan>) {
        let outcome = self
            .registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request(text),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens");
        assert!(
            matches!(
                outcome,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{outcome:?}"
        );
        self.settled(run_id).await;
    }

    async fn settled(&self, run_id: &str) {
        use broca_module::serve::{ServeHandler, SessionReadRequest};
        for _ in 0..2000 {
            let read = self
                .registry
                .read(SessionReadRequest::new(self.key.clone(), None, Some(1)))
                .await
                .expect("the session reads");
            if read.lineage_state.last_run_id.as_deref() == Some(run_id)
                && read.lineage_state.state != "active"
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("run {run_id} never settled");
    }

    /// The session is kept in the new storage, and nothing of it is on today's storage.
    fn assert_store_only(&self) {
        assert!(
            broca_wal::session_storage_head(&self.dir, &broca_core::session_addr(&self.key))
                .exists(),
            "the session's head is in the new storage"
        );
        assert_eq!(self.wal.wal_present(&self.key), Some(false), "no WAL");
    }

    async fn request(&mut self, channel: u16, body: Value) -> Frame {
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
        tokio::time::timeout(Duration::from_secs(20), read_frame(&mut self.reader))
            .await
            .expect("a reply")
            .unwrap()
            .unwrap()
    }

    /// Binds `channel` to the session as the daemon relays it, stamped with `principal`, for the
    /// roles `roles` declares (the session's own route when `None`). Returns the bind's reply.
    async fn bind(
        &mut self,
        channel: u16,
        principal: Principal,
        roles: Option<BTreeMap<String, String>>,
    ) -> Frame {
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", "head"),
            principal: Some(principal),
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: roles,
            call_mode: None,
        };
        self.request(0, serde_json::to_value(bind).unwrap()).await
    }

    /// Binds the session's own route for `principal`.
    async fn bind_session(&mut self, channel: u16, principal: Principal) {
        let reply = self.bind(channel, principal, None).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }

    /// A request's reply body, which must be a response.
    async fn answered(&mut self, channel: u16, method: &str, params: Value) -> Value {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice(&reply.body).unwrap()
    }

    /// The result an op on the session's own route answers with.
    async fn result(&mut self, channel: u16, method: &str, params: Value) -> Value {
        self.answered(channel, method, params).await["result"].clone()
    }

    /// The refusal code `method` answers with.
    async fn refused(&mut self, channel: u16, method: &str, params: Value) -> String {
        let reply = self
            .request(channel, json!({"method": method, "params": params}))
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Error,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice::<Value>(&reply.body).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_owned()
    }
}

/// The text of each message of a raw `session.history` page.
fn history_texts(page: &Value) -> Vec<String> {
    page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["message"]["content"].as_array().unwrap())
        .filter_map(|block| block["text"].as_str().map(str::to_owned))
        .collect()
}

/// `session.history` in its raw form reaches the store through the serve path and pages the
/// session's messages; the display form is still refused by name. A request with no form is
/// the raw form.
#[tokio::test]
async fn raw_history_reaches_the_store_through_serve() {
    let mut rig = Rig::start(
        "history",
        vec![text_step("a1", "hello"), text_step("a2", "again")],
    )
    .await;
    rig.send("r1", "first", None).await;
    rig.send("r2", "second", None).await;
    rig.assert_store_only();
    rig.bind_session(1, core()).await;
    for params in [json!({}), json!({"form": "raw"})] {
        let page = rig.result(1, "session.history", params.clone()).await;
        assert_eq!(
            history_texts(&page),
            ["first", "hello", "second", "again"],
            "{params}: {page}"
        );
    }
    let page = rig
        .result(1, "session.history", json!({"from_ordinal": 2, "limit": 2}))
        .await;
    assert_eq!(history_texts(&page), ["hello", "second"], "{page}");
    assert_eq!(
        rig.refused(1, "session.history", json!({"form": "display"}))
            .await,
        "not_served_on_new_storage"
    );
}

/// Magic Context's `session.entries`, on the `compaction-runner/v1` route, reaches the store
/// through the serve path: the route binds only for the provider the session's plan froze, and
/// pages the session's entries from the store.
#[tokio::test]
async fn compaction_entries_reach_the_store_through_serve() {
    let mut rig = Rig::start(
        "entries",
        vec![text_step("a1", "hello"), text_step("a2", "again")],
    )
    .await;
    rig.send("r1", "first", Some(compacting_plan())).await;
    rig.send(
        "r2",
        "second",
        Some(SendPlan {
            manifest: None,
            ..compacting_plan()
        }),
    )
    .await;
    rig.assert_store_only();
    let compaction = || {
        Some(BTreeMap::from([(
            "compaction-runner".to_owned(),
            "v1".to_owned(),
        )]))
    };
    // Another module is refused at the bind, by the store's check of the frozen provider.
    let refused = rig.bind(2, core(), compaction()).await;
    assert_eq!(
        refused.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.body).unwrap()["detail"]["reason"],
        "provider_mismatch",
        "{}",
        String::from_utf8_lossy(&refused.body)
    );
    let bound = rig.bind(1, provider(), compaction()).await;
    assert_eq!(
        bound.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&bound.body)
    );
    let page = rig
        .answered(
            1,
            "session.entries",
            json!({"select": {"by": "after"}, "detail": "full"}),
        )
        .await;
    let ordinals: Vec<u64> = page["entries"]
        .as_array()
        .unwrap_or_else(|| panic!("{page}"))
        .iter()
        .map(|entry| entry["ordinal"].as_u64().unwrap())
        .collect();
    assert_eq!(ordinals, [1, 2, 3, 4], "{page}");
    let page = rig
        .answered(
            1,
            "session.entries",
            json!({"select": {"by": "after", "after_ordinal": 2, "limit": 1}, "detail": "compact"}),
        )
        .await;
    assert_eq!(
        page["entries"].as_array().unwrap().len(),
        1,
        "a page from ordinal 3 holds one entry: {page}"
    );
    assert_eq!(page["entries"][0]["ordinal"], 3, "{page}");
}
