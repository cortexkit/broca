//! `session.import` from a file and `session.install_view`, through the real serve path into a
//! module whose new storage creates sessions: the route's attested principal and scope stamp
//! reach the store actor, the reply and refusals come back as the store answers them, and the
//! imported session then reads back from the new storage.
//!
//! The daemon side is played by the test, as in the scope carrier tests: it sends the HELLO_ACK
//! advertising `scopes/v1` and stamps each bind.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_module::executor::{RunExecutor, RunJob};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
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

/// Nothing in these tests runs a model step: an import and a view install write no run.
struct NoRuns;

#[async_trait]
impl RunExecutor for NoRuns {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("an import runs no model step")
    }
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
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    /// A module on a fresh state root whose new storage creates sessions. With `store` false,
    /// it has no new storage at all.
    async fn start(tag: &str, store: bool) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-head-import-serve-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor: Arc<dyn RunExecutor> = Arc::new(NoRuns);
        let mut opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal.clone(),
            executor.clone(),
        );
        let mut runtime = None;
        if store {
            let opened = Arc::new(
                broca_module::store_open::StoreRuntime::open(
                    &dir,
                    broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                    executor,
                    Duration::from_millis(100),
                )
                .await
                .unwrap(),
            );
            opener = opener.with_store_runtime(opened.clone());
            runtime = Some(opened);
        }
        let mut registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
        if let Some(runtime) = runtime {
            registry = registry.with_store_new_sessions();
            // Moves are refused by precondition 1 until the new storage serves every
            // operation the move requires, read from the serve layer's own allowlist.
            registry.set_store_restorer(Arc::new(
                broca_module_serve::store_restore::StoreRestore::new(
                    registry.clone(),
                    &dir,
                    Arc::new(broca_module::store_open::StoreSlot::loaded(runtime.clone())),
                    Arc::new(FileLeaseStore::new(dir.join("leases"))),
                ),
            ));
            let mover = broca_module_serve::store_move::StoreMove::new(
                registry.clone(),
                &dir,
                Arc::new(broca_module::store_open::StoreSlot::loaded(runtime)),
                wal,
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                broca_module_serve::store_move::served_by_serve_layer(),
            );
            registry.set_store_mover(Arc::new(mover));
        }
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

    /// Binds `channel` to the head session as the daemon relays it, stamped with `principal`
    /// and, when given, the scope the route was admitted under.
    async fn bind(&mut self, channel: u16, principal: Principal, scope: Option<ScopeStamp>) {
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
            role_versions: None,
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
        serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap()["result"].clone()
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

/// The owner imports a head over a scoped route: the reply is the store's, the source file
/// leaves broca's import folder, a retry is answered the same, the head reads back from the new
/// storage under the source's message ids, and the owner installs the first view. Another
/// principal's install is refused, and a send before adopt is refused.
#[tokio::test]
async fn an_owner_imports_a_head_and_installs_its_first_view_through_serve() {
    let mut rig = Rig::start("import", true).await;
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
        rig.result(1, "session.import", params).await,
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
        let mut rig = Rig::start(host, true).await;
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

/// An import naming a file outside broca's import folder is refused under the store's own code,
/// and the file is left alone.
#[tokio::test]
async fn an_import_from_outside_the_import_folder_is_refused_through_serve() {
    let mut rig = Rig::start("outside", true).await;
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

/// A module without the new storage refuses a file import and a view install by name, and an
/// inline-message import still reaches today's import.
#[tokio::test]
async fn a_module_without_the_new_storage_refuses_file_imports_by_name() {
    let mut rig = Rig::start("no-store", false).await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    assert_eq!(
        rig.refused(1, "session.import", params).await,
        "new_storage_unavailable"
    );
    assert_eq!(
        rig.refused(
            1,
            "session.install_view",
            serde_json::json!({"view": view(1)})
        )
        .await,
        "new_storage_unavailable"
    );
    assert!(rig.import_folder().join("imp-1.json").exists());
}

/// On a session kept in the new storage, an op with no store implementation yet is refused by
/// name before authorization reads anything, never with today's storage's advice to run a newer
/// build. `run.tool_calls` and `session.baseline` are served from the store: a run the session
/// never started is unknown, and the owner reads the baseline the import froze.
/// `role.describe` still answers on the session's route. Cancels are authorized from the
/// stored owner: a stranger is refused as not the owner, and the owner's cancel is answered.
#[tokio::test]
async fn a_store_session_refuses_unbuilt_ops_by_name_and_checks_owners_from_the_store() {
    let mut rig = Rig::start("gate", true).await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    rig.result(1, "session.import", params).await;

    assert_eq!(
        rig.refused(1, "run.event.get", serde_json::json!({"run_id": "r1"}))
            .await,
        "not_served_on_new_storage"
    );
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
    // An import carrying its messages inline is today's import, which writes a WAL.
    assert_eq!(
        rig.refused(
            1,
            "session.import",
            serde_json::json!({"import_id": "inline", "messages": []})
        )
        .await,
        "not_served_on_new_storage"
    );
    let describe = rig
        .request(
            1,
            serde_json::json!({"method": "role.describe", "params": {}}),
        )
        .await;
    assert_eq!(describe.header.ty, FrameType::Response);
    // The head is served from the store. The import's own completed run is the last run, and
    // the head's time is the newest imported message's.
    let head = rig.result(1, "session.head", serde_json::json!({})).await;
    assert_eq!(head["last_run_state"]["state"], "completed", "{head}");
    assert!(
        head["last_run_state"]["run_id"]
            .as_str()
            .is_some_and(|run| run.starts_with("import-")),
        "{head}"
    );
    assert_eq!(head["updated_at"], 4000, "{head}");
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
        !body.contains("wal_newer_writer") && !body.contains("not_served_on_new_storage"),
        "the owner's cancel reaches the store actor: {body}"
    );
}

/// `session.move_to_store` through real dispatch. Only the operator's own connection may call
/// it. The new storage serves every operation a move requires, so precondition 1 passes; a
/// session with no WAL on today's storage then has nothing to move, and the move refuses
/// `not_movable {no_live_wal}`, writing nothing. The operator's `class` param reaches the move
/// when it is `"durable"` and is refused `invalid_params` otherwise. The full move is
/// `store_move_serve::a_live_session_moves_to_the_new_storage_and_keeps_serving`.
#[tokio::test]
async fn a_move_is_operator_only_and_refuses_a_session_with_no_wal() {
    let mut rig = Rig::start("move-to-store", true).await;
    rig.bind(1, core(), None).await;
    assert_eq!(
        rig.refused(1, "session.move_to_store", serde_json::json!({}))
            .await,
        "operator_required"
    );
    rig.bind(2, Principal::Direct, None).await;
    let reply = rig
        .request(
            2,
            serde_json::json!({"method": "session.move_to_store", "params": {}}),
        )
        .await;
    assert_eq!(reply.header.ty, FrameType::Error);
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    let served = broca_module_serve::store_move::served_by_serve_layer();
    assert!(
        broca_module_serve::store_move::REQUIRED_OPERATIONS
            .iter()
            .all(|op| served(op)),
        "the new storage serves every operation a move requires"
    );
    assert_eq!(body["code"], "not_movable", "{body}");
    assert_eq!(body["detail"]["reason"], "no_live_wal", "{body}");
    assert!(
        !broca_wal::session_storage_moves(&rig.dir)
            .join(broca_wal::MOVE_JOURNAL_DIR)
            .exists(),
        "a refused move writes no journal"
    );
    // The operator may ask for the durable root; the request reaches the move, which refuses
    // for the missing WAL as before. Any other class is refused before the move starts.
    let durable = rig
        .request(
            2,
            serde_json::json!({"method": "session.move_to_store", "params": {"class": "durable"}}),
        )
        .await;
    let body: serde_json::Value = serde_json::from_slice(&durable.body).unwrap();
    assert_eq!(body["detail"]["reason"], "no_live_wal", "{body}");
    for class in [
        serde_json::json!("ordinary"),
        serde_json::json!("ephemeral"),
        serde_json::json!(true),
    ] {
        let refused = rig
            .request(
                2,
                serde_json::json!({"method": "session.move_to_store", "params": {"class": class}}),
            )
            .await;
        assert_eq!(refused.header.ty, FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "invalid_params", "{class}: {body}");
    }
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
    let mut rig = Rig::start("restore", true).await;
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
    let stored = |dir: &std::path::Path| {
        files_ending(dir, "")
            .into_iter()
            .map(|path| {
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let store = rig.dir.join(broca_wal::SESSION_STORAGE_DURABLE_DIR);
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
