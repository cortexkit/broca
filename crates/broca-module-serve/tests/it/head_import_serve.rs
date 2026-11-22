//! `session.import` from a file and `session.install_view`, through the real serve path into a
//! module whose new storage creates sessions: the route's attested principal and scope stamp
//! reach the store actor, the reply and refusals come back as the store answers them, and the
//! imported session then reads back from the session store.
//!
//! The daemon side is played by the test, as in the scope carrier tests: it sends the HELLO_ACK
//! advertising `scopes/v1` and stamps each bind.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, OutputEvent};
use broca_module::executor::RunExecutor;
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use broca_session_loop::Writer as _;
use sha2::Digest;
use subc_protocol::scope::{ScopeKind, ScopeStamp, CAP_SCOPES_V1};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::mpsc;

fn core() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn stranger() -> Principal {
    Principal::Reserved {
        module_id: "magic-context".into(),
    }
}

fn stamp(owner: Principal) -> ScopeStamp {
    ScopeStamp::new(owner, "head-1", 1, ScopeKind::Head).with_owner_authorized(true)
}

/// Runs only synthetic provider steps and records the request and compaction Setup.
#[derive(Clone, Default)]
struct HeadRunner {
    calls: Arc<std::sync::Mutex<Vec<broca_protocol::request::CallOptions>>>,
    setups: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

#[async_trait]
impl RunExecutor for HeadRunner {
    fn head_selection_wire(
        &self,
        _: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Option<String> {
        Some("openai".into())
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(Arc::new(self.clone()))
    }

    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        use broca_module::executor::{StoreRunEnd, StoreRunError};
        let broca_module::executor::StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            ..
        } = job;
        control.admitted_context_limit = Some(200_000);
        let provider = broca_core::provider::StubProvider::new(vec![vec![
            broca_protocol::NormalizedEvent::Control(broca_protocol::ProviderControl::Finished {
                usage: Default::default(),
                finish_reason: broca_protocol::content::FinishReason::Stop,
                retries_used: 0,
            }),
        ]]);
        let result = async {
            let admission = broca_session_loop::admit::admission(
                &mut req,
                &control,
                &provider,
                writer.operations(),
                Some(10_000),
            )
            .map_err(StoreRunError::Admission)?;
            let tools = broca_core::stub_plane::StubToolPlane::new();
            let now = || 10_000;
            let seams = broca_session_loop::run::Seams {
                person_steers: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &broca_core::capability::AllowAll,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: Some(self),
                compaction_session: None,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: None,
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                .await
                .map_err(StoreRunError::Loop)
        }
        .await;
        self.calls.lock().unwrap().extend(provider.calls());
        StoreRunEnd { parts, result }
    }
}

#[async_trait]
impl broca_core::CompactionPlane for HeadRunner {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("an imported head uses the current compaction protocol")
    }
    async fn call_v2(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: serde_json::Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => {
                self.setups.lock().unwrap().push(params);
                serde_json::json!({"answer":"ready"})
            }
            "compaction.notice" => serde_json::json!({"answer":"received"}),
            "compaction.ride" => serde_json::json!({"answer":"nothing"}),
            _ => panic!("unexpected method {method}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
    async fn provider_capabilities(&self, _: &str) -> broca_core::ProviderCapabilities {
        broca_core::ProviderCapabilities::Declared(vec!["compaction-provider/v2".into()])
    }
}

#[derive(Default)]
struct HeadPlan(std::sync::atomic::AtomicUsize);

#[async_trait]
impl broca_fetch_plan::PlanAdmitter for HeadPlan {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_records::record::PlanManifest {
                rich_ui: None,
                plan: broca_records::record::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: plan.composition_digest.clone(),
                fetches: vec![],
                tools: Arc::new(vec![]),
                system_text: "Destination system.".into(),
                system_digest: "system".into(),
                join_version: 1,
                role_versions: Default::default(),
                declarations: vec![],
            },
        })
    }
}

fn head_plan() -> serde_json::Value {
    let composition = serde_json::json!({"providers":[{"provider":"synthetic-compactor","tools":[]}],
        "step_transform_items":[], "compaction":{"provider":"synthetic-compactor"}});
    serde_json::json!({"tool_items":[], "system_text_items":[], "step_transform_items":[],
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition":composition, "compaction_item":{"provider":"synthetic-compactor","preset":"default","params":{}},
        "model":{"provider":"stub","model":"m"}})
}

/// An OpenCode 2 export, as Thalamus's `session.export` writes it: a prompt, a reply that
/// reads a file, and a last reply.
fn export() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Look."},
            {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
             "content": [
                {"type": "text", "text": "Reading the file."},
                {"type": "tool", "id": "call_1", "name": "read",
                 "state": {"status": "completed", "input": {"path": "a"},
                           "content": [{"type": "text", "text": "contents"}]}}
             ]},
            {"id": "msg_a2", "type": "assistant", "time": {"created": 4000, "completed": 4500},
             "content": [{"type": "text", "text": "Done."}]}
        ]
    }))
    .unwrap()
}

struct Rig {
    dir: PathBuf,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    _serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    corr: u64,
    runner: HeadRunner,
    admitter: Arc<HeadPlan>,
    /// How long a request waits for its reply.
    reply_within: Duration,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    /// A module on a fresh state root whose new storage creates sessions. With `store` false,
    /// it has no new storage at all.
    async fn start(tag: &str) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-head-import-serve-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let runner = HeadRunner::default();
        let executor: Arc<dyn RunExecutor> = Arc::new(runner.clone());
        let runtime = Arc::new(
            broca_module::store_open::StoreRuntime::open(
                &dir,
                broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                executor.clone(),
                Duration::from_millis(100),
            )
            .await
            .unwrap(),
        );
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
            Arc::new(broca_module::store_open::StoreSlot::loaded(runtime.clone())),
        );
        let registry = LineageRegistry::new(Arc::new(opener));
        registry.set_store_restorer(Arc::new(
            broca_module_serve::store_restore::StoreRestore::new(
                registry.clone(),
                &dir,
                Arc::new(broca_module::store_open::StoreSlot::loaded(runtime.clone())),
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
            ),
        ));
        // A bind resolves the session's address through the session-address inventory, kept in
        // this deployment's SQLite store.
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_store::session_refs::StoredSessions::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let admitter = Arc::new(HeadPlan::default());
        registry.set_plan_admitter(admitter.clone());
        let registry = Arc::new(registry);
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                registry.clone(),
                registry,
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
                subc_capabilities: vec![CAP_SCOPES_V1.to_owned()],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        Self {
            dir,
            reader,
            writer,
            _serve: serve,
            corr: 1,
            runner,
            admitter,
            reply_within: Duration::from_secs(20),
        }
    }

    /// Broca's own import folder under this module's state root.
    fn import_folder(&self) -> PathBuf {
        broca_module::store_import::default_import_folder(&self.dir)
    }

    /// Writes the export into the import folder and returns `session.import`'s params for it.
    fn import_params(&self, import_id: &str) -> serde_json::Value {
        let folder = self.import_folder();
        std::fs::create_dir_all(&folder).unwrap();
        let path = folder.join(format!("{import_id}.json"));
        std::fs::write(&path, export()).unwrap();
        serde_json::json!({
            "import_id": import_id,
            "source": {
                "harness": "opencode2",
                "path": path.display().to_string(),
                "session": "ses_head",
                "sha256": format!("{:x}", sha2::Sha256::digest(export())),
            },
            "origin": {"kind": "import", "source_ref": "thalamus:ses_head"},
            "tag_floor": 40,
        })
    }

    async fn request(&mut self, channel: u16, body: serde_json::Value) -> Frame {
        self.request_captured(channel, body, None).await
    }

    async fn request_captured(
        &mut self,
        channel: u16,
        body: serde_json::Value,
        sample: Option<(&Path, &str)>,
    ) -> Frame {
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
        if let Some((folder, case)) = sample {
            std::fs::write(folder.join(format!("{case}.request.json")), &frame.body).unwrap();
        }
        write_frame(&mut self.writer, &frame).await.unwrap();
        let reply = tokio::time::timeout(self.reply_within, read_frame(&mut self.reader))
            .await
            .expect("a reply")
            .unwrap()
            .unwrap();
        if let Some((folder, case)) = sample {
            let kind = match reply.header.ty {
                FrameType::Response => "reply",
                FrameType::Error => "error",
                other => panic!("unexpected sample frame {other:?}"),
            };
            std::fs::write(folder.join(format!("{case}.{kind}.json")), &reply.body).unwrap();
        }
        reply
    }

    async fn sample(
        &mut self,
        folder: &Path,
        channel: u16,
        method: &str,
        case: &str,
        params: serde_json::Value,
    ) -> Frame {
        self.request_captured(
            channel,
            serde_json::json!({"method":method,"params":params}),
            Some((folder, &format!("{method}.{case}"))),
        )
        .await
    }

    /// Binds `channel` to the head session as the daemon relays it, stamped with `principal`
    /// and, when given, the scope the route was admitted under.
    async fn bind(&mut self, channel: u16, principal: Principal, scope: Option<ScopeStamp>) {
        self.bind_roles(channel, principal, scope, None).await;
    }

    async fn bind_roles(
        &mut self,
        channel: u16,
        principal: Principal,
        scope: Option<ScopeStamp>,
        role_versions: Option<std::collections::BTreeMap<String, String>>,
    ) {
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
            scope,
            role_versions,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
    }

    async fn bind_compactor(&mut self, channel: u16) {
        self.bind_roles(
            channel,
            Principal::Reserved {
                module_id: "synthetic-compactor".into(),
            },
            Some(stamp(core())),
            Some([("compaction-runner".into(), "v1".into())].into()),
        )
        .await;
    }

    async fn result(
        &mut self,
        channel: u16,
        method: &str,
        params: serde_json::Value,
    ) -> serde_json::Value {
        let reply = self
            .request(
                channel,
                serde_json::json!({"method": method, "params": params}),
            )
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        let body = serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap();
        if method == "session.entries" {
            body
        } else {
            body["result"].clone()
        }
    }

    /// The refusal code `method` answers with.
    async fn refused(&mut self, channel: u16, method: &str, params: serde_json::Value) -> String {
        let reply = self
            .request(
                channel,
                serde_json::json!({"method": method, "params": params}),
            )
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Error,
            "{method}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_owned()
    }
}

fn view(version: u64) -> serde_json::Value {
    serde_json::to_value(broca_core::compaction_v2::protocol::view::View::seal(
        version,
        Vec::new(),
    ))
    .unwrap()
}

fn assert_imported_ids(
    imported: &serde_json::Value,
    page: &serde_json::Value,
    collection: &str,
    surface: &str,
) {
    let entries = page[collection]
        .as_array()
        .unwrap_or_else(|| panic!("{surface}: {page}"));
    for moved in imported["moves"]["rows"].as_array().unwrap() {
        if let Some(ordinal) = moved["entry"].as_u64() {
            let served = entries
                .iter()
                .find(|entry| entry["ordinal"].as_u64() == Some(ordinal))
                .unwrap_or_else(|| panic!("{surface} omitted ordinal {ordinal}: {page}"));
            assert_eq!(
                served["mid"], moved["mid"],
                "{surface} changed ordinal {ordinal}'s mid (added={}): {page}",
                moved["added"]
            );
        }
    }
}

fn assert_imported_newest(imported: &serde_json::Value, page: &serde_json::Value) {
    let last = page["entries"].as_array().unwrap().last().unwrap();
    assert_eq!(
        imported["newest"],
        serde_json::json!({"ordinal":last["ordinal"],"mid":last["mid"],"digest":last["digest"]}),
        "import newest must be the exact last imported compaction entry"
    );
}

fn opencode1_source(rig: &Rig, name: &str, count: Option<usize>) -> serde_json::Value {
    opencode1_source_with_text(rig, name, count, 256)
}

/// Like [`opencode1_source`], with each synthetic message's text repeated `repeat` times.
fn opencode1_source_with_text(
    rig: &Rig,
    name: &str,
    count: Option<usize>,
    repeat: usize,
) -> serde_json::Value {
    let db_path = rig.dir.join(format!("{name}-host.db"));
    let mut db = rusqlite::Connection::open(&db_path).unwrap();
    db.execute_batch(include_str!(
        "../../../ck-session-import/tests/fixtures/head/opencode1.sql"
    ))
    .unwrap();
    if let Some(count) = count {
        db.execute_batch(
            "DELETE FROM part; DELETE FROM message; DELETE FROM session WHERE id='ses_other';",
        )
        .unwrap();
        let transaction = db.transaction().unwrap();
        {
            let mut message = transaction
                .prepare("INSERT INTO message VALUES (?1,'ses_synthetic',?2,?3,NULL)")
                .unwrap();
            let mut part = transaction
                .prepare("INSERT INTO part VALUES (?1,?1,'ses_synthetic',?2,?3)")
                .unwrap();
            let text = "Synthetic history. ".repeat(repeat);
            for index in 0..count {
                let mid = format!("msg_synthetic_{index:06}");
                message
                    .execute(rusqlite::params![
                        mid,
                        index as u64,
                        if index % 2 == 0 {
                            r#"{"role":"user"}"#
                        } else {
                            r#"{"role":"assistant"}"#
                        }
                    ])
                    .unwrap();
                part.execute(rusqlite::params![
                    mid,
                    index as u64,
                    serde_json::json!({"type":"text","text":text}).to_string()
                ])
                .unwrap();
            }
        }
        transaction.commit().unwrap();
    }
    drop(db);
    let started = std::time::Instant::now();
    let source = ck_session_import::opencode_extract::extract(
        &db_path,
        "ses_synthetic",
        &rig.import_folder(),
    )
    .unwrap();
    if count.is_some() {
        eprintln!(
            "opencode1 synthetic messages={} bytes={} extract_ms={}",
            source.messages,
            std::fs::metadata(&source.path).unwrap().len(),
            started.elapsed().as_millis()
        );
    }
    serde_json::json!({"import_id":name, "source":{"harness":"opencode1", "path":source.path,
        "session":"ses_synthetic", "sha256":source.sha256},
        "origin":{"kind":"import","source_ref":"opencode1:ses_synthetic"}, "tag_floor":40})
}

#[tokio::test]
async fn opencode1_extract_import_install_view_and_first_owner_send_keep_history() {
    let mut rig = Rig::start("opencode1").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = opencode1_source(&rig, "extract-head", None);
    let imported = rig.result(1, "session.import", params.clone()).await;
    assert_eq!(imported["imported_messages"], 8);
    assert_eq!(imported["dropped_system_messages"], 1);
    assert_eq!(imported["moves"]["shape"], "plugin");
    let rows = imported["moves"]["rows"].as_array().unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row["entry"].as_u64())
            .collect::<Vec<_>>(),
        [
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            None,
            Some(7),
            Some(8),
            None,
            None
        ]
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row["finished_summary"] == true)
            .map(|row| row["mid"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["msg_007", "msg_008"]
    );
    assert!(!std::path::Path::new(params["source"]["path"].as_str().unwrap()).exists());
    let page = rig.result(1, "session.read", serde_json::json!({})).await;
    assert_imported_ids(&imported, &page, "messages", "session.read");
    let stored = page.to_string();
    assert!(stored.contains("image/png"), "{page}");
    assert!(!stored.contains("never-import-this"));
    let raw = rig
        .result(
            1,
            "session.history",
            serde_json::json!({"from_ordinal":1,"limit":100}),
        )
        .await;
    assert_imported_ids(&imported, &raw, "messages", "raw session.history");
    rig.bind(2, stranger(), Some(stamp(core()))).await;
    let send = serde_json::json!({"prompt":"Continue synthetic history.","send_id":"first-owner", "plan":head_plan()});
    let non_owner = rig
        .request(
            2,
            serde_json::json!({"method":"session.send", "params":send.clone()}),
        )
        .await;
    let non_owner: serde_json::Value = serde_json::from_slice(&non_owner.body).unwrap();
    assert_eq!(non_owner["code"], "scope_owner_mismatch", "{non_owner}");
    let missing = rig.request(1, serde_json::json!({"method":"session.send", "params":{"prompt":"Continue", "send_id":"missing-plan"}})).await;
    let missing: serde_json::Value = serde_json::from_slice(&missing.body).unwrap();
    assert_eq!(missing["code"], "invalid_params", "{missing}");
    assert_eq!(missing["detail"]["field"], "plan", "{missing}");
    let fetches_before_owner = rig.admitter.0.load(std::sync::atomic::Ordering::SeqCst);
    rig.result(
        1,
        "session.install_view",
        serde_json::json!({"view":view(1)}),
    )
    .await;
    let sent = rig.result(1, "session.send", send.clone()).await;
    assert_eq!(sent["state"], "active", "{sent}");
    broca_test_support::wait_until("the first imported owner run", || async {
        if rig.runner.calls.lock().unwrap().is_empty() {
            Err("no provider request yet".into())
        } else {
            Ok(())
        }
    })
    .await;
    let calls = rig.runner.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    let request = serde_json::to_string(&calls[0]).unwrap();
    for expected in [
        "Look at this synthetic image.",
        "Synthetic file content",
        "Synthetic summary.",
        "Continue synthetic history.",
        "Destination system.",
    ] {
        assert!(request.contains(expected), "missing {expected}: {request}");
    }
    use broca_provider::families::{
        anthropic_frozen_config, gemini_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, Gemini, OpenAiResponses,
    };
    use broca_provider::wire::WireFamily;
    let families: [(&dyn WireFamily, _); 3] = [
        (
            &AnthropicMessages,
            anthropic_frozen_config("claude-sonnet-4-5"),
        ),
        (&OpenAiResponses, openai_responses_frozen_config("gpt-5")),
        (&Gemini, gemini_frozen_config("gemini-2.5-pro")),
    ];
    for (family, frozen) in families {
        let body = String::from_utf8(family.render(&frozen, &calls[0])).unwrap();
        assert!(body.contains("Synthetic summary."), "{body}");
        assert!(
            !body.contains("Synthetic reasoning") && !body.contains("never-import-this"),
            "finished-run reasoning is stored but never rendered without its payload: {body}"
        );
    }
    assert!(!request.contains("Source system text"));
    let setups = rig.runner.setups.lock().unwrap().clone();
    assert_eq!(setups.len(), 1);
    assert_eq!(setups[0]["applied_view"]["version"], 1, "{:?}", setups[0]);
    rig.bind_compactor(3).await;
    let entries = rig
        .result(
            3,
            "session.entries",
            serde_json::json!({"select":{"by":"after","limit":8},"detail":"full"}),
        )
        .await;
    assert_imported_ids(&imported, &entries, "entries", "session.entries");
    assert_imported_newest(&imported, &entries);
    assert_eq!(
        rig.result(1, "session.import", params).await,
        imported,
        "receipt survives owner admission"
    );
    let baseline = rig
        .result(1, "session.baseline", serde_json::json!({}))
        .await;
    assert!(
        baseline
            .to_string()
            .contains(head_plan()["composition_digest"].as_str().unwrap()),
        "{baseline}"
    );
    let head = rig.result(1, "session.head", serde_json::json!({})).await;
    assert_eq!(head["model"]["provider"], "stub", "{head}");
    let replay = rig.result(1, "session.send", send.clone()).await;
    assert_eq!(replay["state"], "finished");
    assert_eq!(replay["run_id"], sent["run_id"]);
    let mut changed = send.clone();
    changed["model"] = serde_json::json!({"provider":"stub", "model":"different"});
    let refused = rig
        .request(
            1,
            serde_json::json!({"method":"session.send", "params":changed}),
        )
        .await;
    let refused: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
    assert_eq!(refused["code"], "send_id_reuse", "{refused}");
    assert_eq!(refused["detail"]["field"], "model", "{refused}");
    let mut changed = send;
    changed["plan"]["model"]["model"] = serde_json::json!("different");
    assert_eq!(rig.refused(1, "session.send", changed).await, "plan_drift");
    assert_eq!(
        rig.runner.calls.lock().unwrap().len(),
        1,
        "neither retries nor refusals start another run"
    );
    assert_eq!(
        rig.admitter.0.load(std::sync::atomic::Ordering::SeqCst),
        fetches_before_owner + 1,
        "owner admission fetches once; retries never fetch"
    );
    assert_eq!(
        rig.refused(
            1,
            "session.install_view",
            serde_json::json!({"view":view(1)})
        )
        .await,
        "install_view_refused"
    );
}

#[tokio::test]
async fn opencode1_old_open_calls_import_but_last_row_open_calls_keep_the_extract() {
    for last in [false, true] {
        let mut rig = Rig::start(if last {
            "opencode1-open-tail"
        } else {
            "opencode1-open-old"
        })
        .await;
        rig.bind(1, core(), Some(stamp(core()))).await;
        let mut params = opencode1_source(&rig, "open-call", None);
        let path = PathBuf::from(params["source"]["path"].as_str().unwrap());
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute(
        "UPDATE part SET data=json_set(data,'$.state.status','running','$.state.input',json('{\"path\":\"recorded-input\"}')) WHERE id='prt_009'",
        [],
    )
    .unwrap();
        if last {
            db.execute(
                "UPDATE part SET message_id='msg_009' WHERE id='prt_009'",
                [],
            )
            .unwrap();
        }
        drop(db);
        params["source"]["sha256"] =
            serde_json::json!(ck_session_import::opencode_extract::sha256_file(&path).unwrap());
        if last {
            assert_eq!(
                rig.refused(1, "session.import", params).await,
                "import_open_call"
            );
            assert!(path.exists(), "a refused import never deletes its source");
        } else {
            let imported = rig.result(1, "session.import", params).await;
            assert_eq!(imported["imported_messages"], 8);
            assert!(!path.exists());
            let page = rig
                .result(1, "session.read", serde_json::json!({}))
                .await
                .to_string();
            assert!(
                page.contains("[Tool execution was interrupted]")
                    && page.contains("recorded-input")
                    && page.contains("reused-2"),
                "{page}"
            );
        }
        assert!(rig.runner.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
#[ignore = "synthetic 25k-message extract/import timing, run explicitly"]
async fn opencode1_extract_import_25k_timing() {
    let mut rig = Rig::start("opencode1-25k").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = opencode1_source(&rig, "timing-25k", Some(25_000));
    let started = std::time::Instant::now();
    let imported = rig.result(1, "session.import", params).await;
    eprintln!(
        "opencode1 synthetic messages=25000 import_ms={}",
        started.elapsed().as_millis()
    );
    assert_eq!(imported["imported_messages"], 25_000);
}

/// A move list too large for one reply frame: asked for whole, the reply isn't served; asked for
/// in pages, every page fits a frame and the pages hold every row once, in source order, each
/// row naming the entry its message became. With `BROCA_IMPORT_MOVES_SAMPLES` naming a folder,
/// the requests and replies are kept there as wire samples.
#[tokio::test]
async fn a_move_list_too_large_for_one_frame_is_served_in_pages_through_serve() {
    let samples = std::env::var_os("BROCA_IMPORT_MOVES_SAMPLES").map(PathBuf::from);
    let mut rig = Rig::start("opencode1-paged-moves").await;
    // The first page imports 100,000 messages, which an unoptimized build on a shared CI runner
    // takes longer than the rig's usual 20 s to do.
    rig.reply_within = Duration::from_secs(180);
    rig.bind(1, core(), Some(stamp(core()))).await;
    let count = 100_000;
    let params = opencode1_source_with_text(&rig, "paged-moves", Some(count), 1);
    let mut pages = Vec::new();
    let mut from = Some(0u64);
    while let Some(at) = from {
        let mut asked = params.clone();
        asked["moves_from"] = serde_json::json!(at);
        let frame = match (&samples, at, pages.len()) {
            (Some(folder), 0, _) => {
                rig.sample(folder, 1, "session.import", "paged_first", asked)
                    .await
            }
            (Some(folder), _, 1) => {
                rig.sample(folder, 1, "session.import", "paged_repeat", asked)
                    .await
            }
            _ => {
                rig.request(
                    1,
                    serde_json::json!({"method":"session.import","params":asked}),
                )
                .await
            }
        };
        assert_eq!(
            frame.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&frame.body)
        );
        assert!(frame.body.len() as u64 <= broca_wire::SUBC_MAX_FRAME_BYTES);
        let reply =
            serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["result"].clone();
        assert_eq!(reply["moves"]["total"], count as u64);
        assert_eq!(reply["imported_messages"], count as u64);
        from = reply["moves"]["next_from"].as_u64();
        pages.push(reply);
    }
    assert!(pages.len() > 1, "the list takes several pages");
    let rows: Vec<_> = pages
        .iter()
        .flat_map(|page| page["moves"]["rows"].as_array().unwrap().clone())
        .collect();
    assert_eq!(rows.len(), count);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["mid"], format!("msg_synthetic_{index:06}"));
        assert_eq!(row["entry"], index as u64 + 1);
    }
    if let Some(folder) = &samples {
        // The last page again, asked for by the index the page before it named, as a sample.
        let mut asked = params.clone();
        asked["moves_from"] = pages[pages.len() - 2]["moves"]["next_from"].clone();
        let last = rig
            .sample(folder, 1, "session.import", "paged_last", asked)
            .await;
        let last = serde_json::from_slice::<serde_json::Value>(&last.body).unwrap();
        assert_eq!(last["result"], pages[pages.len() - 1]);
    }
    let whole = match &samples {
        Some(folder) => {
            rig.sample(folder, 1, "session.import", "unpaged_too_large", params)
                .await
        }
        None => {
            rig.request(
                1,
                serde_json::json!({"method":"session.import","params":params}),
            )
            .await
        }
    };
    assert_eq!(whole.header.ty, FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&whole.body).unwrap()["code"],
        "response_too_large"
    );
}

/// Save the actual request and terminal frame bodies, not reconstructed typed responses.
#[tokio::test]
#[ignore = "writes synthetic wire samples; run explicitly"]
async fn opencode1_capture_head_import_wire_samples() {
    let folder = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.cortexkit/internal-docs/investigations/head-import-samples");
    std::fs::create_dir_all(&folder).unwrap();
    let mut rig = Rig::start("opencode1-wire-samples").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = opencode1_source(&rig, "wire-synthetic-head", None);
    let path = PathBuf::from(params["source"]["path"].as_str().unwrap());
    let mut media = |bytes: &[u8], mime: &str| {
        broca_core::media_validation::validate_media(bytes, mime)
            .map_err(|reason| format!("{reason:?}"))
    };
    let preview = broca_module::store_import::offline_moves(
        ck_session_import::head::opencode1::read(&path, "ses_synthetic", &mut media).unwrap(),
        Some(40),
    )
    .unwrap();
    let original = std::fs::read(&path).unwrap();
    let error = |frame: &Frame, code: &str| {
        assert_eq!(
            frame.header.ty,
            FrameType::Error,
            "{}",
            String::from_utf8_lossy(&frame.body)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap()["code"],
            code
        );
    };
    let mut wrong = params.clone();
    wrong["source"]["sha256"] = serde_json::json!("00");
    error(
        &rig.sample(&folder, 1, "session.import", "digest_mismatch", wrong)
            .await,
        "import_digest_mismatch",
    );
    let outside = rig.dir.join("synthetic-outside.db");
    std::fs::write(&outside, &original).unwrap();
    let mut wrong = params.clone();
    wrong["source"]["path"] = serde_json::json!(outside);
    error(
        &rig.sample(&folder, 1, "session.import", "path_refused", wrong)
            .await,
        "import_path_refused",
    );
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute("UPDATE part SET message_id='msg_009',data=json_set(data,'$.state.status','running') WHERE id='prt_009'",[]).unwrap();
    drop(db);
    let mut open = params.clone();
    open["source"]["sha256"] =
        serde_json::json!(ck_session_import::opencode_extract::sha256_file(&path).unwrap());
    error(
        &rig.sample(&folder, 1, "session.import", "open_call", open)
            .await,
        "import_open_call",
    );
    assert!(path.exists());
    std::fs::write(&path, original).unwrap();
    let imported = rig
        .sample(&folder, 1, "session.import", "success", params.clone())
        .await;
    assert_eq!(imported.header.ty, FrameType::Response);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&imported.body).unwrap()["result"]["moves"]
            ["shape"],
        "plugin"
    );
    let imported_result =
        serde_json::from_slice::<serde_json::Value>(&imported.body).unwrap()["result"].clone();
    assert_eq!(
        serde_json::to_value(preview.newest).unwrap(),
        imported_result["newest"],
        "a source newest is exact even in the offline wrapper"
    );
    let page = rig
        .sample(
            &folder,
            1,
            "session.read",
            "imported",
            serde_json::json!({}),
        )
        .await;
    assert_eq!(page.header.ty, FrameType::Response);
    let page = serde_json::from_slice::<serde_json::Value>(&page.body).unwrap();
    assert_imported_ids(
        &imported_result,
        &page["result"],
        "messages",
        "sampled session.read",
    );
    let raw = rig
        .result(
            1,
            "session.history",
            serde_json::json!({"from_ordinal":1,"limit":100}),
        )
        .await;
    assert_imported_ids(
        &imported_result,
        &raw,
        "messages",
        "sampled raw session.history",
    );
    let retry = rig
        .sample(&folder, 1, "session.import", "retry", params.clone())
        .await;
    assert_eq!(retry.body, imported.body);
    let mut reused = params.clone();
    reused["tag_floor"] = serde_json::json!(41);
    error(
        &rig.sample(&folder, 1, "session.import", "send_id_reuse", reused)
            .await,
        "send_id_reuse",
    );
    let installed = rig
        .sample(
            &folder,
            1,
            "session.install_view",
            "success",
            serde_json::json!({"view":view(1)}),
        )
        .await;
    assert_eq!(installed.header.ty, FrameType::Response);
    error(
        &rig.sample(
            &folder,
            1,
            "session.send",
            "without_plan",
            serde_json::json!({"send_id":"wire-no-plan","prompt":"Continue synthetic history."}),
        )
        .await,
        "invalid_params",
    );
    rig.bind(2, stranger(), Some(stamp(core()))).await;
    let send = serde_json::json!({"send_id":"wire-first-owner","prompt":"Continue synthetic history.","plan":head_plan()});
    error(
        &rig.sample(&folder, 2, "session.send", "non_owner", send.clone())
            .await,
        "scope_owner_mismatch",
    );
    let sent = rig
        .sample(&folder, 1, "session.send", "first_owner", send.clone())
        .await;
    assert_eq!(sent.header.ty, FrameType::Response);
    broca_test_support::event_timeout(
        "the sampled owner run to finish",
        async {
            loop {
                if rig.result(1, "run.status", serde_json::json!({})).await["state"] == "idle" {
                    break;
                }
                tokio::task::yield_now().await;
            }
        },
        || "the synthetic provider run is still active".into(),
    )
    .await
    .unwrap();
    assert_eq!(rig.runner.calls.lock().unwrap().len(), 1);
    rig.bind_compactor(3).await;
    let entries = rig
        .sample(
            &folder,
            3,
            "session.entries",
            "imported",
            serde_json::json!({"select":{"by":"after","limit":8},"detail":"full"}),
        )
        .await;
    assert_eq!(entries.header.ty, FrameType::Response);
    let entries = serde_json::from_slice::<serde_json::Value>(&entries.body).unwrap();
    assert_imported_ids(
        &imported_result,
        &entries,
        "entries",
        "sampled session.entries",
    );
    assert_imported_newest(&imported_result, &entries);
    let after_owner = rig
        .sample(
            &folder,
            1,
            "session.import",
            "after_owner_retry",
            params.clone(),
        )
        .await;
    assert_eq!(
        after_owner.body, imported.body,
        "import retry remains byte-identical after the first owner run"
    );
    let retry = rig.sample(&folder, 1, "session.send", "retry", send).await;
    assert_eq!(retry.header.ty, FrameType::Response);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&retry.body).unwrap()["result"]["run_id"],
        serde_json::from_slice::<serde_json::Value>(&sent.body).unwrap()["result"]["run_id"]
    );
    error(
        &rig.sample(
            &folder,
            1,
            "session.install_view",
            "after_step",
            serde_json::json!({"view":view(1)}),
        )
        .await,
        "install_view_refused",
    );
    let readme = "# Head-import wire samples\n\nCaptured from the real serve handler over synthetic SQLite history. Files contain exact UTF-8 frame bodies, without transport headers or pretty-printing. Requests include method/params; Management successful terminal bodies retain the result wrapper; compaction-runner session.entries returns a bare page. Paths and SHA-256 values name the temporary synthetic extract, deleted on durable success; these are observations, not reusable live requests.\n\nThe owner route is stamped for reserved prefrontal-core; the non-owner route is reserved magic-context under the same scope. The synthetic plan uses a stub model and synthetic compactor. Source message ordinals are one-based.\n\n- session.import.success: initial file-source import and plugin move list.\n- session.import.retry: identical request, byte-identical durable receipt reply after source deletion.\n- session.import.digest_mismatch: wrong source digest, import_digest_mismatch.\n- session.import.open_call: running tool in the last source row, import_open_call; extract retained.\n- session.import.send_id_reuse: same import_id with a different tag_floor, send_id_reuse.\n- session.import.path_refused: source outside import roots, import_path_refused.\n- session.install_view.success: owner installs version 1 before the first send.\n- session.install_view.after_step: install after a real synthetic model step, install_view_refused.\n- session.send.without_plan: owner first send without plan, invalid_params naming plan.\n- session.send.non_owner: another principal's first send, scope_owner_mismatch.\n- session.send.first_owner: owner first send with plan.model and no top-level model, active reply.\n- session.send.retry: identical send_id/request after the step, same run_id and finished reply.\n\nSHA256SUMS covers every JSON body and this README, not itself.\n";
    let readme = format!("{readme}\n- session.import.after_owner_retry: identical import request after the first owner run; the complete reply, including newest, is byte-identical to success.\n- session.read.imported: all eight imported entries before the first owner send; every move-list id matches by ordinal.\n- session.entries.imported: all eight imported entries on the compaction-runner/v1 role route after plan admission; every source and added id matches. Raw session.history is asserted too, without a sample file.\n");
    std::fs::write(folder.join("README.md"), readme).unwrap();
    let mut files = std::fs::read_dir(&folder)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
                || path.file_name().is_some_and(|name| name == "README.md")
        })
        .collect::<Vec<_>>();
    files.sort();
    assert_eq!(
        files.len(),
        31,
        "fifteen request/terminal pairs and a README"
    );
    let sums = files
        .iter()
        .map(|path| {
            format!(
                "{:x}  {}\n",
                sha2::Sha256::digest(std::fs::read(path).unwrap()),
                path.file_name().unwrap().to_str().unwrap()
            )
        })
        .collect::<String>();
    std::fs::write(folder.join("SHA256SUMS"), sums).unwrap();
}

/// The owner imports a head over a scoped route: the reply is the store's, the source file
/// leaves broca's import folder, a retry is answered the same, the head reads back from the new
/// storage under the source's message ids, and the owner installs the first view. Another
/// principal's install is refused.
/// The seeded run an import writes is stamped with wall-clock time, which retention counts the
/// session's deadline from; a retention past the limit a send has is refused before anything is
/// read or written.
#[tokio::test]
async fn an_import_is_stamped_with_wall_clock_time_and_its_retention_is_checked() {
    use sha2::Digest as _;
    let mut rig = Rig::start("import-time").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let mut params = rig.import_params("imp-time");
    params["retention"] = serde_json::json!(broca_module::retention::MAX_RETENTION_SECONDS + 1);
    assert_eq!(
        rig.refused(1, "session.import", params.clone()).await,
        "invalid_params"
    );
    assert!(
        rig.import_folder().join("imp-time.json").exists(),
        "a refused import leaves its source"
    );
    params.as_object_mut().unwrap().remove("retention");
    let before = broca_core::clock::epoch_millis().unwrap();
    rig.result(1, "session.import", params).await;
    let after = broca_core::clock::epoch_millis().unwrap();
    let digest = format!("{:x}", sha2::Sha256::digest("imp-time".as_bytes()));
    let admission = rig
        .result(
            1,
            "run.admission",
            serde_json::json!({"run_id": format!("import-{}", &digest[..32])}),
        )
        .await;
    let started = admission["started_at_ms"].as_u64().unwrap();
    assert!(
        (before..=after).contains(&started),
        "{before} <= {started} <= {after}: {admission}"
    );
}

#[tokio::test]
async fn an_owner_imports_a_head_and_installs_its_first_view_through_serve() {
    let mut rig = Rig::start("import").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    let reply = rig.result(1, "session.import", params.clone()).await;
    assert_eq!(reply["moves"]["shape"], "runner", "{reply}");
    let rows: Vec<_> = reply["moves"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            // The results split off `msg_a1` are broca's own message, under broca's id.
            if row["added"] == true {
                format!("added {}", row["entry"])
            } else {
                row["mid"].as_str().unwrap().to_owned()
            }
        })
        .collect();
    assert_eq!(rows, ["msg_u1", "msg_a1", "added 3", "msg_a2"], "{reply}");
    assert!(
        !rig.import_folder().join("imp-1.json").exists(),
        "the import deletes its source from broca's own folder"
    );
    assert_eq!(
        rig.result(1, "session.import", params.clone()).await,
        reply,
        "a retry is answered from the stored reply"
    );

    let page = rig.result(1, "session.read", serde_json::json!({})).await;
    let mids: Vec<_> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["mid"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(mids.len(), 4, "{page}");
    assert_eq!(
        [mids[0].as_str(), mids[1].as_str(), mids[3].as_str()],
        ["msg_u1", "msg_a1", "msg_a2"],
        "{page}"
    );

    rig.bind(2, stranger(), Some(stamp(core()))).await;
    assert_eq!(
        rig.refused(
            2,
            "session.install_view",
            serde_json::json!({"view": view(1)})
        )
        .await,
        "owner_required"
    );
    let installed = rig
        .result(
            1,
            "session.install_view",
            serde_json::json!({"view": view(1)}),
        )
        .await;
    assert_eq!(installed["version"], 1, "{installed}");
    assert_eq!(
        rig.result(
            1,
            "session.install_view",
            serde_json::json!({"view": view(1)})
        )
        .await,
        installed,
        "an install retry is answered the same"
    );
    rig.result(1,"session.send",serde_json::json!({"prompt":"Continue runner history.","send_id":"runner-first-owner","plan":head_plan()})).await;
    broca_test_support::wait_until("the runner-shaped imported owner run", || async {
        if rig.runner.calls.lock().unwrap().is_empty() {
            Err("no provider request yet".into())
        } else {
            Ok(())
        }
    })
    .await;
    rig.bind_compactor(3).await;
    let entries = rig
        .result(
            3,
            "session.entries",
            serde_json::json!({"select":{"by":"after","limit":4},"detail":"full"}),
        )
        .await;
    assert_imported_ids(&reply, &entries, "entries", "runner session.entries");
    assert_imported_newest(&reply, &entries);
    assert_eq!(
        rig.result(1, "session.import", params).await,
        reply,
        "runner import retry after owner admission retains newest"
    );
}

/// Published Thalamus exports keep call identities and notices through `session.read`
/// (default and display facts) and display `session.history`. Optional recording writes
/// the response frame bodies without re-encoding them.
#[tokio::test]
async fn thalamus_imported_heads_keep_calls_and_notices_in_wire_reads() {
    let fixtures = std::env::var_os("BROCA_THALAMUS_EXPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../ck-session-import/tests/fixtures/thalamus-export-v1")
        });
    let record = std::env::var_os("BROCA_IMPORTED_READ_RECORD_DIR").map(PathBuf::from);
    for (host, journal, carrier) in [
        ("opencode2", "export.json", "thalamus-harness"),
        ("pi", "export.jsonl", "thalamus-pi"),
    ] {
        let expected_calls = if host == "pi" { 1 } else { 2 };
        let source = fixtures.join(host);
        let bytes = std::fs::read(source.join(journal)).unwrap();
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(source.join("reply.json")).unwrap()).unwrap();
        assert_eq!(
            format!("{:x}", sha2::Sha256::digest(&bytes)),
            metadata["sha256"].as_str().unwrap()
        );
        let header: serde_json::Value = serde_json::from_slice(if host == "pi" {
            bytes.split(|byte| *byte == b'\n').next().unwrap()
        } else {
            &bytes
        })
        .unwrap();
        let session = if host == "pi" {
            &header["id"]
        } else {
            &header["info"]["id"]
        };
        let sends = std::fs::read(source.join("export.sends.json")).unwrap();
        let side: serde_json::Value = serde_json::from_slice(&sends).unwrap();
        let ride = side
            .as_array()
            .unwrap()
            .iter()
            .find(|send| send["call_key"].is_string())
            .unwrap();
        let note = side
            .as_array()
            .unwrap()
            .iter()
            .find(|send| send["template"] == "note")
            .unwrap();
        let mut rig = Rig::start(host).await;
        rig.bind(1, core(), Some(stamp(core()))).await;
        let folder = rig.import_folder();
        std::fs::create_dir_all(&folder).unwrap();
        let path = folder.join("head.json");
        std::fs::write(&path, bytes).unwrap();
        std::fs::write(folder.join("head.sends.json"), sends).unwrap();
        let imported = rig
            .result(
                1,
                "session.import",
                serde_json::json!({
                    "import_id": "real-thalamus-export",
                    "source": {"harness": host, "path": path, "session": session,
                               "sha256": metadata["sha256"]},
                    "origin": {"kind": "import", "source_ref": "thalamus:sample-head"},
                    "tag_floor": metadata["tag_floor"],
                }),
            )
            .await;
        let count = imported["moves"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|row| row["entry"].as_u64())
            .max()
            .unwrap();
        for (name, params) in [
            ("read-default-1.json", serde_json::json!({})),
            (
                "read-display-facts-1.json",
                serde_json::json!({"include_display_facts": true}),
            ),
        ] {
            let response = rig
                .request(
                    1,
                    serde_json::json!({"method": "session.read", "params": params}),
                )
                .await;
            assert_eq!(
                response.header.ty,
                FrameType::Response,
                "{}",
                String::from_utf8_lossy(&response.body)
            );
            let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            let page = &body["result"];
            let messages = page["messages"].as_array().unwrap();
            assert_eq!(
                messages.len() as u64,
                count,
                "the default page covers the whole imported transcript"
            );
            assert_eq!(messages[0]["ordinal"], 1);
            assert!(page.get("next_from_ordinal").is_none());
            let mut calls = 0;
            for message in messages {
                for call in message["tool_calls"].as_array().into_iter().flatten() {
                    calls += 1;
                    assert_eq!(call["call_carrier"], carrier);
                    assert_eq!(
                        call["call_key"],
                        format!(
                            "{}/{}",
                            message["mid"].as_str().unwrap(),
                            call["tool_call_id"].as_str().unwrap()
                        )
                    );
                }
            }
            assert_eq!(calls, expected_calls);
            if params["include_display_facts"] == true {
                let call = messages
                    .iter()
                    .flat_map(|message| message["tool_times"].as_array().into_iter().flatten())
                    .find(|call| call["call_key"] == ride["call_key"])
                    .expect("the original runner call key");
                assert_eq!(call["call_carrier"], carrier);
                for message in messages {
                    for time in message["tool_times"].as_array().into_iter().flatten() {
                        let call = message["tool_calls"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|call| call["tool_call_id"] == time["tool_call_id"])
                            .unwrap();
                        assert_eq!(time["call_key"], call["call_key"]);
                        assert_eq!(time["call_carrier"], carrier);
                    }
                }
                let notice = messages
                    .iter()
                    .flat_map(|message| message["notices"].as_array().into_iter().flatten())
                    .find(|notice| notice["call_key"] == ride["call_key"])
                    .expect("the delivered ride notice");
                assert_eq!(notice["text"], ride["text"]);
                assert_eq!(notice["send_id"], ride["send_id"]);
                assert_eq!(notice["sender"], ride["sender"]);
                assert_eq!(notice["call_carrier"], carrier);
                let result_page = rig
                    .result(
                        1,
                        "session.read",
                        serde_json::json!({
                            "from_ordinal": 3, "limit": 1, "include_display_facts": true,
                        }),
                    )
                    .await;
                assert_eq!(
                    result_page["messages"][0]["notices"][0], *notice,
                    "a page starting at the results keeps the notice"
                );
                let note_row = messages
                    .iter()
                    .find(|message| message["mid"] == note["mid"])
                    .unwrap();
                assert_eq!(note_row["notice"], true);
                assert_eq!(note_row["sender"], note["sender"]);
            } else {
                assert!(messages
                    .iter()
                    .all(|message| message.get("tool_times").is_none()
                        && message.get("notices").is_none()
                        && message.get("notice").is_none()));
            }
            if let Some(record) = &record {
                let out = record.join(host);
                std::fs::create_dir_all(&out).unwrap();
                std::fs::write(out.join(name), &response.body).unwrap();
            }
        }
        let response = rig
            .request(
                1,
                serde_json::json!({
                    "method": "session.history", "params": {"form": "display"},
                }),
            )
            .await;
        assert_eq!(
            response.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        let rows = body["result"]["messages"].as_array().unwrap();
        assert_eq!(rows.len() as u64, count);
        let mut display_calls = 0;
        let mut notices = 0;
        for row in rows {
            for part in row["row"]["parts"].as_array().into_iter().flatten() {
                if part["type"] == "tool_call" {
                    display_calls += 1;
                    assert_eq!(part["call_carrier"], carrier);
                    assert!(part["call_key"]
                        .as_str()
                        .unwrap()
                        .starts_with(row["mid"].as_str().unwrap()));
                }
            }
            for notice in row["row"]["notices"].as_array().into_iter().flatten() {
                notices += 1;
                assert_eq!(notice["call_key"], ride["call_key"]);
                assert_eq!(notice["call_carrier"], carrier);
            }
        }
        assert_eq!(display_calls, expected_calls);
        assert_eq!(notices, 1);
        if let Some(record) = &record {
            std::fs::write(
                record.join(host).join("history-display-1.json"),
                &response.body,
            )
            .unwrap();
        }
    }
}

/// The side file's opaque keys, pins and per-call times survive `session.read` and display
/// `session.history`. A call recorded as cancelled with no timestamps keeps unknown times
/// instead of taking the import's clock.
#[tokio::test]
async fn thalamus_call_side_file_keeps_pins_times_and_unknown_facts_through_serve() {
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/thalamus-export-calls-v1");
    for (host, journal, closure) in [
        ("opencode2", "export.json", false),
        ("pi", "export.jsonl", false),
        ("opencode2", "export.json", true),
    ] {
        let source = fixtures.join(host);
        let bytes = std::fs::read(source.join(journal)).unwrap();
        let mut side: serde_json::Value =
            serde_json::from_slice(&std::fs::read(source.join("export.calls.json")).unwrap())
                .unwrap();
        if closure {
            let call = side["calls"][0].as_object_mut().unwrap();
            call.remove("started_at");
            call.remove("finished_at");
            call.insert("unanswered".into(), serde_json::json!("cancelled"));
        }
        let tag = format!("call-facts-{host}-{closure}");
        let mut rig = Rig::start(&tag).await;
        rig.bind(1, core(), Some(stamp(core()))).await;
        let folder = rig.import_folder();
        std::fs::create_dir_all(&folder).unwrap();
        let path = folder.join(journal);
        std::fs::write(&path, &bytes).unwrap();
        std::fs::write(
            folder.join("export.calls.json"),
            serde_json::to_vec(&side).unwrap(),
        )
        .unwrap();
        rig.result(
            1,
            "session.import",
            serde_json::json!({
                "import_id": "call-facts",
                "source": {
                    "harness": host, "path": path,
                "session": "01a1c3a2-83cc-718b-bcb8-06f631d16cd7",
                    "sha256": format!("{:x}", sha2::Sha256::digest(&bytes)),
                },
                "origin": {"kind": "import", "source_ref": "thalamus:call-facts"},
            }),
        )
        .await;
        let default = rig.result(1, "session.read", serde_json::json!({})).await;
        let facts = rig
            .result(
                1,
                "session.read",
                serde_json::json!({"include_display_facts": true}),
            )
            .await;
        let display = rig
            .result(1, "session.history", serde_json::json!({"form": "display"}))
            .await;
        let rows = side["calls"].as_array().unwrap();
        let mut count = 0;
        for message in default["messages"].as_array().unwrap() {
            assert!(message.get("tool_times").is_none());
            for call in message["tool_calls"].as_array().into_iter().flatten() {
                count += 1;
                let expected = rows
                    .iter()
                    .find(|row| row["call_key"] == call["call_key"])
                    .unwrap();
                assert_eq!(message["mid"], expected["mid"]);
                assert_eq!(call["tool_call_id"], expected["tool_call_id"]);
                assert_eq!(call["call_carrier"], "thalamus-harness");
                assert!(
                    call.get("schema_pin").is_none(),
                    "tool_calls has no pin member"
                );
            }
        }
        assert_eq!(count, 2);
        let times: Vec<_> = facts["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["tool_times"].as_array().into_iter().flatten())
            .collect();
        let parts: Vec<_> = display["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["row"]["parts"].as_array().into_iter().flatten())
            .filter(|part| part["type"] == "tool_call")
            .collect();
        assert_eq!(times.len(), 2);
        assert_eq!(parts.len(), 2);
        for expected in rows {
            let time = times
                .iter()
                .find(|time| time["call_key"] == expected["call_key"])
                .unwrap();
            let part = parts
                .iter()
                .find(|part| part["call_key"] == expected["call_key"])
                .unwrap();
            for actual in [*time, *part] {
                assert_eq!(actual["call_carrier"], "thalamus-harness");
                for member in ["schema_pin", "started_at", "finished_at", "unanswered"] {
                    assert_eq!(actual.get(member), expected.get(member), "{host}: {member}");
                }
            }
        }
    }
}

/// An import naming a file outside broca's import folder is refused under the store's own code,
/// and the file is left alone.
#[tokio::test]
async fn an_import_from_outside_the_import_folder_is_refused_through_serve() {
    let mut rig = Rig::start("outside").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let mut params = rig.import_params("imp-1");
    let outside = rig.dir.join("elsewhere.json");
    std::fs::write(&outside, export()).unwrap();
    params["source"]["path"] = outside.display().to_string().into();
    assert_eq!(
        rig.refused(1, "session.import", params).await,
        "import_path_refused"
    );
    assert!(
        outside.exists(),
        "a file outside the import folder is never deleted"
    );
}

/// On an imported session, `run.tool_calls` and `session.baseline` are served from the store: a
/// run the session never started is unknown, and the owner reads the baseline the import froze.
/// `role.describe` still answers on the session's route. Cancels are authorized from the
/// stored owner: a stranger is refused as not the owner, and the owner's cancel is answered.
#[tokio::test]
async fn a_store_session_serves_its_ops_and_checks_owners_from_the_store() {
    let mut rig = Rig::start("gate").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    let before = broca_core::clock::epoch_millis().unwrap();
    rig.result(1, "session.import", params).await;
    let after = broca_core::clock::epoch_millis().unwrap();

    // The baseline is served from the store, to the owner.
    let baseline = rig
        .result(1, "session.baseline", serde_json::json!({}))
        .await;
    assert_eq!(baseline["state"], "ready", "{baseline}");
    assert_eq!(
        rig.refused(1, "run.tool_calls", serde_json::json!({"run_id": "r1"}))
            .await,
        broca_wire::UNKNOWN_RUN
    );
    // Run status is served from the store. With no run named it is the session's status, idle
    // once the import's own run has ended.
    let idle = rig.result(1, "run.status", serde_json::json!({})).await;
    assert_eq!(idle, serde_json::json!({"state": "idle"}));
    let describe = rig
        .request(
            1,
            serde_json::json!({"method": "role.describe", "params": {}}),
        )
        .await;
    assert_eq!(describe.header.ty, FrameType::Response);
    // The head is served from the store. The import's own completed run is the last run, and
    // the head's time is the import's, the newest time its log carries.
    let head = rig.result(1, "session.head", serde_json::json!({})).await;
    assert_eq!(head["last_run_state"]["state"], "completed", "{head}");
    assert!(
        head["last_run_state"]["run_id"]
            .as_str()
            .is_some_and(|run| run.starts_with("import-")),
        "{head}"
    );
    let updated = head["updated_at"].as_u64().unwrap();
    assert!((before..=after).contains(&updated), "{head}");
    assert_eq!(
        (head["first_ordinal"].clone(), head["last_ordinal"].clone()),
        (1.into(), 4.into()),
        "{head}"
    );
    // The import's run, named, answers from the store as completed.
    let run_id = head["last_run_state"]["run_id"].clone();
    let status = rig
        .result(1, "run.status", serde_json::json!({"run_id": run_id}))
        .await;
    assert_eq!(status["state"], "completed", "{status}");

    rig.bind(2, stranger(), Some(stamp(stranger()))).await;
    let cancel = serde_json::json!({"run_id": "r-none"});
    let refused = rig.refused(2, "run.cancel", cancel.clone()).await;
    assert_eq!(refused, "scope_owner_mismatch");
    let reply = rig
        .request(
            1,
            serde_json::json!({"method": "run.cancel", "params": cancel}),
        )
        .await;
    let body = String::from_utf8_lossy(&reply.body).into_owned();
    assert!(
        !body.contains("not_served_on_new_storage"),
        "the owner's cancel reaches the store actor: {body}"
    );
}

/// Every file under `dir` whose name ends with `suffix`.
fn files_ending(dir: &std::path::Path, suffix: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.to_string_lossy().ends_with(suffix) {
                found.push(path);
            }
        }
    }
    found
}

/// `session.restore_from_backup` through real dispatch. Only the operator's own connection may
/// call it, with `all` a boolean when given. A session whose files are all there is reported kept
/// and left alone, alone or among every session. Once the import's id map is gone, as from a
/// backup that listed the store before the import, restore holds the session (once its idle
/// actor has closed), cuts its open log back to where the batch holding the first record that
/// names the map starts, keeps the cut bytes beside the log, and the session reads again,
/// without the imported messages.
#[tokio::test]
async fn a_restore_is_operator_only_keeps_a_whole_session_and_cuts_one_lacking_a_file() {
    let mut rig = Rig::start("restore").await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    rig.result(1, "session.import", params).await;
    assert_eq!(
        rig.refused(1, "session.restore_from_backup", serde_json::json!({}))
            .await,
        "operator_required"
    );
    rig.bind(2, Principal::Direct, None).await;
    assert_eq!(
        rig.refused(
            2,
            "session.restore_from_backup",
            serde_json::json!({"all": "yes"})
        )
        .await,
        "invalid_params"
    );
    // The session's own files. The store's set of sessions that may owe recovery is left out:
    // the import's registration there is retired whenever its session closes, which can fall
    // on either side of a restore and says nothing about whether the restore wrote the session.
    let stored = |dir: &std::path::Path| {
        files_ending(dir, "")
            .into_iter()
            .filter(|path| !path.starts_with(dir.join("recovery-candidates")))
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let store = rig
        .dir
        .join(broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR);
    let before = stored(&store);
    let kept = rig
        .result(2, "session.restore_from_backup", serde_json::json!({}))
        .await;
    assert_eq!(kept["sessions"][0]["outcome"], "kept", "{kept}");
    assert_eq!(kept["sessions"][0]["class"], "durable", "{kept}");
    let all = rig
        .result(
            2,
            "session.restore_from_backup",
            serde_json::json!({"all": true}),
        )
        .await;
    assert_eq!(all["sessions"].as_array().unwrap().len(), 1, "{all}");
    assert_eq!(all["sessions"][0]["outcome"], "kept", "{all}");
    assert_eq!(stored(&store), before, "a kept session is never written");

    let maps = files_ending(&store, "-imported-ids");
    assert_eq!(maps.len(), 1, "the import stored one id map: {maps:?}");
    std::fs::remove_file(&maps[0]).unwrap();
    // The session's actor serves it until it has been idle a while; until then restore reports
    // it busy and changes nothing.
    let rig = Arc::new(tokio::sync::Mutex::new(rig));
    let session = broca_test_support::wait_until("restore to hold the session", || {
        let rig = rig.clone();
        async move {
            let reply = rig
                .lock()
                .await
                .result(2, "session.restore_from_backup", serde_json::json!({}))
                .await;
            let session = reply["sessions"][0].clone();
            match session["outcome"].as_str() {
                Some("busy") => Err(session.to_string()),
                _ => Ok(session),
            }
        }
    })
    .await;
    assert_eq!(session["outcome"], "log_cut", "{session}");
    assert!(
        session["cut"]["lacking"]
            .as_str()
            .unwrap()
            .starts_with("import id map "),
        "{session}"
    );
    let sidecar = session["cut"]["sidecar"].as_str().unwrap();
    assert!(std::path::Path::new(sidecar).is_file(), "{session}");
    // The sidecar is named by the wall clock, not by how long the daemon has run, so a restore
    // in a later run never names the same file.
    let stamp: u64 = sidecar
        .rsplit("restore-cut-")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(
        now - stamp < 60_000,
        "{sidecar} names {stamp}, now is {now}"
    );
    let page = rig
        .lock()
        .await
        .result(1, "session.read", serde_json::json!({}))
        .await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 0, "{page}");
}
