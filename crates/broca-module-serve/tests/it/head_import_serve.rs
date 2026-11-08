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
        if store {
            let runtime = broca_module::store_open::StoreRuntime::open(
                &dir,
                broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                executor,
                Duration::from_millis(100),
            )
            .await
            .unwrap();
            opener = opener.with_store_runtime(Arc::new(runtime));
        }
        let mut registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
        if store {
            registry = registry.with_store_new_sessions();
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
/// build. `role.describe` still answers on the session's route. Cancels are authorized from the
/// stored owner: a stranger is refused as not the owner, and the owner's cancel is answered.
#[tokio::test]
async fn a_store_session_refuses_unbuilt_ops_by_name_and_checks_owners_from_the_store() {
    let mut rig = Rig::start("gate", true).await;
    rig.bind(1, core(), Some(stamp(core()))).await;
    let params = rig.import_params("imp-1");
    rig.result(1, "session.import", params).await;

    for method in ["run.tool_calls", "session.baseline"] {
        assert_eq!(
            rig.refused(1, method, serde_json::json!({"run_id": "r1"}))
                .await,
            "not_served_on_new_storage",
            "{method}"
        );
    }
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
