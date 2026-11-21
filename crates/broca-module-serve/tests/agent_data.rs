use async_trait::async_trait;
use broca_module::{registry::OpenError, LineageRegistry};
use broca_module_serve::{
    agent_data::{AgentDataService, SessionDoor},
    serve::{self, VaultGauge},
};
use broca_protocol::BindIdentity;
use broca_subc::session_door::{DoorRead, SessionDoorError, MAX_CALL_BYTES};
use broca_wire::agent_data as contract;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{read_frame, write_frame};

const CUT: &str = "0123456789abcdef0123456789abcdef";
const BACK: &str = "fedcba9876543210fedcba9876543210";
fn root(tag: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    broca_test_support::scratch_root().join(format!(
        "agent-data-{}-{}-{tag}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ))
}
#[derive(Default)]
struct FakeDoor {
    streams: Mutex<BTreeMap<String, Vec<u8>>>,
    sealed: Mutex<BTreeMap<(String, String), Vec<u8>>>,
    refusal: Mutex<Option<String>>,
    digest: Mutex<Option<String>>,
    calls: AtomicUsize,
    read_limit: AtomicUsize,
    drift: AtomicBool,
    fail_cover_once: AtomicBool,
}
impl FakeDoor {
    fn seal(&self, cut: &str) {
        for (reference, bytes) in self.streams.lock().unwrap().iter() {
            self.sealed
                .lock()
                .unwrap()
                .insert((cut.into(), reference.clone()), bytes.clone());
        }
    }
    fn plant(&self, cut: &str, reference: &str, bytes: Vec<u8>) {
        self.sealed
            .lock()
            .unwrap()
            .insert((cut.into(), reference.into()), bytes);
    }
}
#[async_trait]
impl SessionDoor for FakeDoor {
    async fn durable_offset(&self, reference: &str) -> Result<u64, SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_cover_once.swap(false, Ordering::SeqCst) {
            return Err(SessionDoorError {
                door_code: Some("unavailable".into()),
                message: "injected cover failure".into(),
            });
        }
        Ok(self
            .streams
            .lock()
            .unwrap()
            .get(reference)
            .map_or(0, |b| b.len() as u64))
    }
    async fn export(&self, reference: &str, bytes: &[u8]) -> Result<(), SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut streams = self.streams.lock().unwrap();
        let local = streams.entry(reference.into()).or_default();
        assert!(bytes.starts_with(local));
        *local = bytes.to_vec();
        Ok(())
    }
    async fn read(
        &self,
        reference: &str,
        offset: u64,
        cut: &str,
    ) -> Result<DoorRead, SessionDoorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(code) = self.refusal.lock().unwrap().clone() {
            return Err(SessionDoorError {
                door_code: Some(code),
                message: "injected door refusal".into(),
            });
        }
        let seals = self.sealed.lock().unwrap();
        let bytes = seals
            .get(&(cut.into(), reference.into()))
            .ok_or_else(|| SessionDoorError {
                door_code: Some("not_found".into()),
                message: "cut is not published".into(),
            })?;
        let configured = self.read_limit.load(Ordering::SeqCst);
        let limit = if configured == 0 {
            MAX_CALL_BYTES
        } else {
            configured
        };
        let end = bytes.len().min(offset as usize + limit);
        Ok(DoorRead {
            bytes: bytes[offset as usize..end].to_vec(),
            from_offset: offset,
            end_offset: end as u64,
            stream_end: bytes.len() as u64,
            digest: if offset > 0 && self.drift.load(Ordering::SeqCst) {
                "f".repeat(64)
            } else {
                self.digest
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| broca_wal::migration::digest(bytes))
            },
        })
    }
}
/// A participant on a computer: broca's state root with its session store open, the move
/// service over it, and the door it reads cuts from and exports them to.
struct Rig {
    root: PathBuf,
    index: Arc<broca_store::SqliteStore>,
    service: Arc<AgentDataService>,
    registry: Arc<LineageRegistry>,
    door: Arc<FakeDoor>,
}

struct NoRuns;
#[async_trait]
impl broca_module::executor::RunExecutor for NoRuns {}

/// The deployment key every rig's session store uses unless a test names another.
fn deployment_key() -> broca_session_store::keys::DeploymentKey {
    broca_session_store::keys::DeploymentKey::new(7, &[4; 32])
}

/// A rig at `path` whose session store opens with `key`.
async fn rig(
    path: PathBuf,
    door: Arc<FakeDoor>,
    key: broca_session_store::keys::DeploymentKey,
) -> Rig {
    std::fs::create_dir_all(&path).unwrap();
    let runtime = Arc::new(
        broca_module::store_open::StoreRuntime::open(
            &path,
            key,
            Arc::new(NoRuns),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    let index = Arc::new(broca_store::SqliteStore::open(path.join("run-index.db")).unwrap());
    // Boot's census: every session already stored here is in the session-ref index.
    broca_store::session_refs::migrate(
        &path.join("run-index.db"),
        &broca_store::session_refs::StoredSessions::new(&path),
    )
    .unwrap();
    let registry = Arc::new(LineageRegistry::new(Arc::new(
        broca_module::opener::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(path.join("leases"))),
            Arc::new(NoRuns),
        )
        .with_store_runtime(runtime),
    )));
    registry.set_session_address_inventory(index.clone());
    let service = Arc::new(AgentDataService::new(
        path.clone(),
        registry.as_ref().clone(),
        index.clone(),
        door.clone(),
    ));
    Rig {
        root: path,
        index,
        registry,
        service,
        door,
    }
}

impl Rig {
    async fn new(tag: &str, door: Arc<FakeDoor>) -> Self {
        rig(root(tag), door, deployment_key()).await
    }
    /// The same computer after a restart: a new process over the same state root.
    async fn reopen(&self) -> Self {
        rig(self.root.clone(), self.door.clone(), deployment_key()).await
    }
    fn session(&self, name: &str) -> BindIdentity {
        BindIdentity::new("/same/project", "broca", name)
    }
    fn params(&self, cut: &str, sessions: &[BindIdentity]) -> Value {
        json!({"cutId":cut,"refs":sessions.iter().map(|s|contract::session_reference(&s.harness,&s.project_root.to_string_lossy(),&s.session)).collect::<Vec<_>>()})
    }
    /// Stores `session` as binding it would, with its ref in the session-ref index: a run on a
    /// stub model, finished after one answer when `terminal`, still running otherwise.
    async fn seed(&self, session: &BindIdentity, terminal: bool) {
        store_session(self, session, terminal).await;
        // Binding a session records its ref in the index in production; moves find it there.
        broca_core::SessionRefIndex::record_session(
            self.index.as_ref(),
            &broca_core::session_addr(session),
            session,
        )
        .unwrap();
    }
    /// Every file the session store keeps for `session`, by path: a comparison of two snapshots
    /// shows whether an operation changed the session.
    fn stored(&self, session: &BindIdentity) -> BTreeMap<PathBuf, Vec<u8>> {
        fn walk(
            dir: &std::path::Path,
            address: &str,
            inside: bool,
            out: &mut BTreeMap<PathBuf, Vec<u8>>,
        ) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                let named = inside
                    || path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .contains(address);
                if path.is_dir() {
                    walk(&path, address, named, out);
                } else if named {
                    out.insert(path.clone(), std::fs::read(&path).unwrap());
                }
            }
        }
        let mut out = BTreeMap::new();
        for class in broca_wal::StorageClass::ALL {
            walk(
                &class.store_root(&self.root),
                &broca_core::session_addr(session),
                false,
                &mut out,
            );
        }
        out
    }
    /// Appends a record to `session` as later work on this computer would, so its copy is
    /// ahead of any cut taken before.
    async fn extend(&self, session: &BindIdentity) {
        use broca_session_store::{
            backend::{LocalBackend, SessionAddr},
            record::{Billed, Record},
            session::Session,
        };
        let run_id = seed_history(true)
            .records
            .iter()
            .find_map(|record| match record {
                Record::RunStart(start) => Some(start.run_id.clone()),
                _ => None,
            })
            .unwrap();
        let runtime = self.registry.store_runtime().unwrap();
        let backend = LocalBackend::new(broca_wal::StorageClass::Durable.store_root(&self.root));
        let mut writer = Session::load(
            &backend,
            SessionAddr(broca_core::session_addr(session)),
            &triple(session),
            runtime.deployment(),
        )
        .await
        .unwrap();
        writer
            .append(&[Record::Billed(Billed { run_id })], true)
            .await
            .unwrap();
        writer.close().await.unwrap();
    }
    /// Where an import of cut `cut` stages its copy of `session`.
    fn staged(&self, cut: &str, session: &BindIdentity) -> PathBuf {
        self.root
            .join("session-store-moves/staging")
            .join(cut)
            .join(broca_core::session_addr(session))
    }
    /// Where a move's freeze or tombstone for `session` is kept.
    fn fence(&self, session: &BindIdentity) -> PathBuf {
        self.root
            .join("session-store-moves/fences")
            .join(broca_core::session_addr(session))
    }
}

/// The session triple a session's log names, as broca derives it from its bind identity.
fn triple(session: &BindIdentity) -> broca_session_store::head::SessionTriple {
    broca_session_store::head::SessionTriple {
        project_root: session.project_root.display().to_string(),
        harness: session.harness.clone(),
        session: session.session.clone(),
    }
}

/// A session's records and setup parts, as the storage keeps them: a run started on a stub
/// model, either finished after one answer (`finished.json`) or still running with nothing
/// answered (`running.json`). The records name no session, so one history serves every one.
struct Seed {
    records: Vec<broca_session_store::record::Record>,
    parts: BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
}

fn seed_history(terminal: bool) -> Seed {
    let text = if terminal {
        include_str!("agent_data/fixtures/finished.json")
    } else {
        include_str!("agent_data/fixtures/running.json")
    };
    let fixture: Value = serde_json::from_str(text).unwrap();
    let bytes = |value: &Value| -> Vec<u8> {
        let text = value.as_str().unwrap();
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    };
    Seed {
        records: fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                let kind = u32::try_from(record["kind"].as_u64().unwrap()).unwrap();
                broca_session_store::record::Record::decode(kind, &bytes(&record["body"])).unwrap()
            })
            .collect(),
        parts: fixture["parts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|part| {
                let digest: [u8; 32] = bytes(&part[0]).try_into().unwrap();
                (broca_session_store::codec::Digest(digest), bytes(&part[1]))
            })
            .collect(),
    }
}

/// Writes `session`'s records into the store, and nothing into the session-ref index.
async fn store_session(rig: &Rig, session: &BindIdentity, terminal: bool) {
    store_session_with(
        rig,
        session,
        seed_history(terminal),
        broca_wal::StorageClass::Durable,
    )
    .await;
}

/// Writes `seeded` into the store as `session`'s records, kept in storage class `class`.
async fn store_session_with(
    rig: &Rig,
    session: &BindIdentity,
    seeded: Seed,
    class: broca_wal::StorageClass,
) {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        session::Session,
    };
    let runtime = rig.registry.store_runtime().unwrap();
    let backend = LocalBackend::new(class.store_root(&rig.root));
    let mut writer = Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(session)),
        triple(session),
        runtime.deployment(),
        false,
    )
    .await
    .unwrap();
    let key = runtime
        .deployment()
        .setup_key_for(runtime.deployment().new_sessions())
        .unwrap();
    let store = broca_session_store::setup::SetupStore::new(&backend, &key);
    for record in &seeded.records {
        if let broca_session_store::record::Record::SetupStub(stub) = record {
            for part in &stub.parts {
                use broca_session_loop::parts::Parts as _;
                store
                    .put(part.kind, &seeded.parts[&part.digest])
                    .await
                    .unwrap();
                runtime
                    .parts_for(class, runtime.deployment().new_sessions())
                    .clone()
                    .put(
                        part.kind,
                        &serde_json::from_slice(&seeded.parts[&part.digest]).unwrap(),
                    )
                    .unwrap();
            }
        }
    }
    writer.append(&seeded.records, true).await.unwrap();
    writer.close().await.unwrap();
}

fn flags() -> Flags {
    Flags::new(false, Priority::Interactive, false)
}
struct Wire {
    manifest: Value,
    stream: tokio::io::DuplexStream,
    serving: tokio::task::JoinHandle<Result<serve::ServeEnd, serve::ServeIoError>>,
    corr: u64,
}
impl Drop for Wire {
    fn drop(&mut self) {
        self.serving.abort();
    }
}
impl Wire {
    async fn bind_session(&mut self, session: &BindIdentity) {
        self.send(0, 0, 99, FrameType::Request, json!({"op":"route.bind","route_channel":7,"epoch":1,
            "target":{"kind":"management_surface","module_id":"broca"},"identity":session,"principal":null})).await;
        assert_eq!(self.next().await.header.ty, FrameType::Response);
    }
    async fn management(&mut self, method: &str, params: Value) -> Result<Value, Value> {
        self.raw(serde_json::to_vec(&json!({"method":method,"params":params})).unwrap())
            .await
    }
    async fn new(rig: &Rig, principal: Value, internal: bool) -> Self {
        Self::new_with_scope(rig, principal, internal, Value::Null).await
    }
    async fn new_with_scope(rig: &Rig, principal: Value, internal: bool, scope: Value) -> Self {
        Self::connect(rig, principal, internal, scope).await
    }
    async fn connect(rig: &Rig, principal: Value, internal: bool, scope: Value) -> Self {
        let (stream, module) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(module);
        let registry = rig.registry.clone();
        let service = rig.service.clone();
        let serving = tokio::spawn(async move {
            serve::serve_connection_with_agent_data(
                read,
                write,
                "broca",
                registry.clone(),
                registry,
                VaultGauge::not_in_use(),
                None,
                Default::default(),
                None,
                None,
                None,
                Some(service),
            )
            .await
        });
        let mut wire = Self {
            manifest: Value::Null,
            stream,
            serving,
            corr: 1,
        };
        let hello = wire.next().await;
        assert_eq!(hello.header.ty, FrameType::Hello);
        wire.manifest = serde_json::from_slice::<Value>(&hello.body).unwrap()["manifest"].clone();
        wire.send(
            0,
            0,
            hello.header.corr,
            FrameType::HelloAck,
            json!({"negotiated_ver":2,"subc_ops":[],"subc_capabilities":[]}),
        )
        .await;
        let target = if internal {
            json!({"kind":"internal_service","module_id":"broca","service_id":"agent-data"})
        } else {
            json!({"kind":"management_surface","module_id":"broca"})
        };
        wire.send(0, 0, 2, FrameType::Request, json!({"op":"route.bind","route_channel":7,"epoch":1,"target":target,
            "identity":{"project_root":"/untrusted","harness":"reserved:entorhinal","session":"forged"},"principal":principal,"scope":scope})).await;
        assert_eq!(wire.next().await.header.ty, FrameType::Response);
        wire
    }
    async fn next(&mut self) -> Frame {
        let corr = self.corr;
        broca_test_support::event_timeout(
            "agent-data wire reply",
            read_frame(&mut self.stream),
            || format!("correlation {corr}"),
        )
        .await
        .expect("serve reply deadline")
        .unwrap()
        .unwrap()
    }
    async fn send(&mut self, channel: u16, epoch: u32, corr: u64, ty: FrameType, body: Value) {
        write_frame(
            &mut self.stream,
            &Frame::build(
                ty,
                flags(),
                channel,
                epoch,
                corr,
                serde_json::to_vec(&body).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    async fn raw(&mut self, bytes: Vec<u8>) -> Result<Value, Value> {
        self.corr += 1;
        write_frame(
            &mut self.stream,
            &Frame::build(FrameType::Request, flags(), 7, 1, self.corr, bytes).unwrap(),
        )
        .await
        .unwrap();
        let frame = self.next().await;
        assert_eq!(frame.header.corr, self.corr);
        let body: Value = serde_json::from_slice(&frame.body).unwrap();
        match frame.header.ty {
            FrameType::Response => Ok(body["result"].clone()),
            FrameType::Error => Err(body),
            other => panic!("unexpected {other:?}"),
        }
    }
    async fn call(&mut self, op: &str, params: Value) -> Result<Value, Value> {
        self.raw(
            serde_json::to_vec(&json!({"method":format!("agent_data.{op}"),"params":params}))
                .unwrap(),
        )
        .await
    }
    async fn call_bytes(&mut self, op: &str, params: Value) -> (FrameType, Vec<u8>) {
        self.corr += 1;
        write_frame(
            &mut self.stream,
            &Frame::build(
                FrameType::Request,
                flags(),
                7,
                1,
                self.corr,
                serde_json::to_vec(&json!({"method":format!("agent_data.{op}"),"params":params}))
                    .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let frame = self.next().await;
        assert_eq!(frame.header.corr, self.corr);
        (frame.header.ty, frame.body)
    }
}
fn entorhinal() -> Value {
    json!({"kind":"reserved","module_id":"entorhinal"})
}
fn recorded_sessions(sessions: &[BindIdentity]) -> BTreeMap<String, BindIdentity> {
    sessions
        .iter()
        .map(|session| {
            (
                contract::session_ref(
                    &session.harness,
                    &session.project_root.to_string_lossy(),
                    &session.session,
                ),
                session.clone(),
            )
        })
        .collect()
}
fn assert_code(result: Result<Value, Value>, code: &str) -> Value {
    let error = result.unwrap_err();
    assert_eq!(error["code"], code, "{error}");
    serde_json::from_value::<contract::errors::ErrorBody>(error.clone())
        .unwrap()
        .typed_detail()
        .unwrap();
    error
}

#[tokio::test]
async fn describe_is_internal_and_declares_the_agent_owned_store() {
    let rig = Rig::new("describe", Arc::new(FakeDoor::default())).await;
    let mut internal = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        internal.call("describe", json!({})).await.unwrap(),
        json!({"ref_kinds":["broca-session"],"stores":[{"name":"broca/session-wals","sync_class":"agent"}],"external":false})
    );
    let manifest = internal.manifest.clone();
    assert!(manifest["capabilities"]["provides"]
        .as_array()
        .unwrap()
        .contains(&json!("agent-data/v1")));
    let services = manifest["provides"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|role| role["role"] == "internal_service")
        .collect::<Vec<_>>();
    assert_eq!(
        services,
        vec![
            &json!({"role":"internal_service","service_id":"agent-data","transport":"bulk","agent_facing":false,"operations":contract::REQUIRED_OPS})
        ]
    );
    let mut management = Wire::new(&rig, entorhinal(), false).await;
    assert!(
        management.call("describe", json!({})).await.is_err(),
        "agent-data is not a management operation"
    );
}

#[tokio::test]
async fn busy_without_cut_id_and_unknown_abort_follow_the_contract() {
    let rig = Rig::new("busy", Arc::new(FakeDoor::default())).await;
    let session = rig.session("active");
    rig.seed(&session, false).await;
    let params = rig.params(CUT, &[session]);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("busy", json!({"refs":params["refs"]}))
            .await
            .unwrap()["refs"][0]["reason_code"],
        "run_active"
    );
    assert_code(wire.call("prepare", params.clone()).await, "busy");
    assert!(!rig
        .root
        .join("agent-data/cuts")
        .join(format!("{CUT}.json"))
        .exists());
    let missing = json!({"ref_kind":"broca-session","ref":"ffffffffffffffffffffffffffffffff"});
    let mut mixed = params.clone();
    mixed["refs"].as_array_mut().unwrap().push(missing.clone());
    assert_eq!(
        assert_code(
            wire.call("busy", json!({"refs":mixed["refs"]})).await,
            "unknown_ref"
        )["detail"],
        missing
    );
    wire.call("abort", mixed.clone()).await.unwrap();
    assert_eq!(
        wire.call("status", mixed.clone()).await.unwrap(),
        json!({"step":"aborted"})
    );
    wire.call("abort", mixed).await.unwrap();
}

#[tokio::test]
async fn retention_marker_never_reaches_the_door() {
    // A session kept with retention lives in the ephemeral class: its content must never reach
    // the door, so a move refuses it before reading or exporting anything.
    let rig = Rig::new("retained", Arc::new(FakeDoor::default())).await;
    let session = rig.session("private");
    store_session_with(
        &rig,
        &session,
        seed_history(true),
        broca_wal::StorageClass::Ephemeral,
    )
    .await;
    broca_core::SessionRefIndex::record_session(
        rig.index.as_ref(),
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let before = rig.stored(&session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(
        wire.call("prepare", rig.params(CUT, std::slice::from_ref(&session)))
            .await,
        "not_exportable",
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert!(rig.door.streams.lock().unwrap().is_empty());
    assert_eq!(rig.stored(&session), before);
}

/// A receiver for a cut of one finished session that a source computer prepared and exported
/// through its own move service. The receiver's door holds the sealed export and has served no
/// call yet. Returns the receiver, the cut's params and the exported bytes.
async fn incoming_rig(tag: &str) -> (Rig, Value, Vec<u8>) {
    let (params, bytes) = exported_cut(tag, &["incoming"]).await;
    let door = Arc::new(FakeDoor::default());
    for (reference, bytes) in &bytes {
        door.plant(CUT, reference, bytes.clone());
    }
    let rig = Rig::new(tag, door).await;
    let bytes = bytes.into_values().next().unwrap();
    (rig, params, bytes)
}

/// Prepares and exports a cut of the finished sessions `names` on a source computer of its own,
/// and returns the cut's params and each ref's exported bytes.
async fn exported_cut(tag: &str, names: &[&str]) -> (Value, BTreeMap<String, Vec<u8>>) {
    let door = Arc::new(FakeDoor::default());
    let source = Rig::new(&format!("{tag}-source"), door.clone()).await;
    let sessions = names
        .iter()
        .map(|name| source.session(name))
        .collect::<Vec<_>>();
    for session in &sessions {
        source.seed(session, true).await;
    }
    let params = source.params(CUT, &sessions);
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("prepare", params.clone()).await.unwrap();
    from.call("export", params.clone()).await.unwrap();
    let streams = door.streams.lock().unwrap().clone();
    (params, streams)
}
fn assert_nothing_staged(rig: &Rig) {
    assert!(!rig.root.join("session-store-moves/staging").exists());
}
#[tokio::test]
async fn import_requires_the_doors_sealed_digest() {
    let (rig, params, _) = incoming_rig("no-digest").await;
    *rig.door.digest.lock().unwrap() = Some(String::new());
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "unavailable");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_verifies_the_doors_sealed_digest() {
    let (rig, params, _) = incoming_rig("bad-digest").await;
    *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "digest_mismatch");
    assert_nothing_staged(&rig);
}
/// A cut in the old session log format, as a build from before the session store would export
/// it, is refused as an incompatible version before anything is staged.
#[tokio::test]
async fn import_refuses_a_cut_in_the_old_log_format() {
    let (rig, params, _) = incoming_rig("old-format").await;
    rig.door.plant(
        CUT,
        params["refs"][0]["ref"].as_str().unwrap(),
        b"BRCA old session log bytes".to_vec(),
    );
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "incompatible_version");
    assert_eq!(error["detail"]["found"], "broca-wal");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_address_is_proved_by_the_exports_session_triple() {
    let (rig, mut params, bytes) = incoming_rig("address").await;
    let wrong = contract::session_ref("broca", "/same/project", "wrong");
    params["refs"][0]["ref"] = json!(wrong);
    rig.door.plant(CUT, &wrong, bytes);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "invalid_request");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_maps_door_refusals_without_staging() {
    for (door_code, participant_code) in [
        ("not_found", "cut_not_ready"),
        ("verify_failed", "digest_mismatch"),
        ("cut_item_missing", "participant_failed"),
        ("unavailable", "unavailable"),
    ] {
        let (rig, params, _) = incoming_rig(door_code).await;
        *rig.door.refusal.lock().unwrap() = Some(door_code.into());
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let error = assert_code(wire.call("import", params).await, participant_code);
        assert!(error["message"].as_str().unwrap().contains(door_code));
        assert_nothing_staged(&rig);
    }
}
#[tokio::test]
async fn whole_ref_import_preflight_leaves_every_local_and_staged_file_untouched() {
    let (params, streams) = exported_cut("multi", &["a", "b"]).await;
    let door = Arc::new(FakeDoor::default());
    for (reference, bytes) in &streams {
        door.plant(CUT, reference, bytes.clone());
    }
    let receiver = Rig::new("multi-receiver", door).await;
    let sessions = [receiver.session("a"), receiver.session("b")];
    // The receiver keeps its own, different copy of the second session.
    store_session(&receiver, &sessions[1], false).await;
    let before = receiver.stored(&sessions[1]);
    let mut wire = Wire::new(&receiver, entorhinal(), true).await;
    assert_code(wire.call("import", params).await, "local_advanced");
    assert!(receiver.stored(&sessions[0]).is_empty());
    assert_eq!(receiver.stored(&sessions[1]), before);
    assert_nothing_staged(&receiver);
}
/// A prepare that stopped after recording its intent and before replying leaves the intent for
/// a repeat to finish: status reads it without changing the cut's record, and the repeat
/// freezes every session and replies as a first prepare would.
#[tokio::test]
async fn status_keeps_a_partial_prepare_intent_byte_identical_until_repeat() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let rig = Rig::new("pending-prepare", Arc::new(FakeDoor::default())).await;
    let sessions = [rig.session("a"), rig.session("b")];
    for session in &sessions {
        rig.seed(session, true).await;
    }
    let params = rig.params(CUT, &sessions);
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "prepare".into(),
            request: params.clone(),
        }),
        sessions: recorded_sessions(&sessions),
        replies: Default::default(),
    };
    let path = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&rig.root, &path, &state).unwrap();
    let before = std::fs::read(&path).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"none"})
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let reply = wire.call("prepare", params.clone()).await.unwrap();
    assert_eq!(reply["items"].as_array().unwrap().len(), 2);
    for session in &sessions {
        assert!(rig.fence(session).with_extension("moving").exists());
    }
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"prepared"})
    );
    assert_eq!(wire.call("prepare", params).await.unwrap(), reply);
}

fn vectors(name: &str) -> Value {
    serde_json::from_str(cortexkit_role_agent_data::vectors::file(name).unwrap()).unwrap()
}
#[tokio::test]
async fn contract_request_vectors_pass_through_internal_serve() {
    let rig = Rig::new("request-vectors", Arc::new(FakeDoor::default())).await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let cases = vectors("requests.json");
    assert_code(wire.raw(serde_json::to_vec(&json!({"method":"agent_data.describe","params":{},"principal":"reserved:entorhinal"})).unwrap()).await,"invalid_request");
    for case in cases["requests"].as_array().unwrap() {
        let mut params = case["params"].clone();
        if let Some(count) = case["expand_refs"].as_u64() {
            params["refs"] = json!((0..count)
                .map(|n| json!({"ref_kind":"broca-session","ref":format!("ref-{n}")}))
                .collect::<Vec<_>>());
        }
        let request =
            serde_json::to_vec(&json!({"method":case["method"],"params":params})).unwrap();
        if let Err(error) = wire.raw(request).await {
            // Shared request vectors may use ref kinds this broca does not
            // declare. Those valid requests can be refused as unsupported kinds
            // after decoding, but must not fail the request decoder.
            if error["code"] == "invalid_request" {
                assert_eq!(
                    error["message"], "broca does not hold this session ref",
                    "{}",
                    case["name"]
                );
            }
        }
    }
    // Invalid requests must be decoded before this deliberately unreadable store.
    let junk = broca_wal::StorageClass::Durable
        .store_root(&rig.root)
        .join("heads/not-a-valid-address");
    std::fs::create_dir_all(&junk).unwrap();
    std::fs::write(junk.join("head"), b"invalid request must not read this").unwrap();
    for case in cases["refused"].as_array().unwrap() {
        let mut params = case["params"].clone();
        if let Some(count) = case["expand_refs"].as_u64() {
            params["refs"] = json!((0..count)
                .map(|n| json!({"ref_kind":"broca-session","ref":format!("ref-{n}")}))
                .collect::<Vec<_>>());
        }
        assert_code(
            wire.raw(
                serde_json::to_vec(&json!({"method":case["method"],"params":params})).unwrap(),
            )
            .await,
            case["code"].as_str().unwrap(),
        );
    }
}
#[tokio::test]
async fn contract_step_vectors_pass_through_internal_serve() {
    use broca_module::cut_state::{CutState, CutStore};
    let cases = vectors("steps.json");
    for case in cases["transitions"].as_array().unwrap() {
        let rig = Rig::new("step-vector", Arc::new(FakeDoor::default())).await;
        let state = CutState {
            step: serde_json::from_value(case["from"].clone()).unwrap(),
            refs: Some(contract::refs::Refs::new(vec![]).unwrap()),
            intent: None,
            sessions: Default::default(),
            replies: Default::default(),
        };
        broca_wal::migration::atomic_json(&rig.root, &CutStore::new(&rig.root).path(CUT), &state)
            .unwrap();
        let before = std::fs::read(CutStore::new(&rig.root).path(CUT)).unwrap();
        let mut params = json!({"refs":[],"cutId":CUT});
        if case["op"] == "describe" {
            params = json!({});
        }
        if case["op"] == "busy" {
            params = json!({"refs":[]});
        }
        if case["op"] == "release" {
            params["holder"] = json!("receiver");
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.call(case["op"].as_str().unwrap(), params).await;
        if let Some(code) = case["refused"].as_str() {
            assert_code(result, code);
            assert_eq!(
                std::fs::read(CutStore::new(&rig.root).path(CUT)).unwrap(),
                before
            );
        } else {
            result.unwrap_or_else(|error| panic!("{}: {error}", case["op"]));
            assert_eq!(
                wire.call("status", json!({"refs":[],"cutId":CUT}))
                    .await
                    .unwrap()["step"],
                case["to"]
            );
        }
    }
}
#[tokio::test]
async fn contract_idempotency_vectors_replay_before_resolving_sessions() {
    use broca_module::cut_state::{CutState, CutStore, Reply};
    for case in vectors("idempotency.json")["cases"].as_array().unwrap() {
        let rig = Rig::new("idempotency-vector", Arc::new(FakeDoor::default())).await;
        let first = &case["first"];
        let mut state = CutState {
            refs: first["params"]
                .get("refs")
                .map(|r| serde_json::from_value(r.clone()).unwrap()),
            ..Default::default()
        };
        let witness = json!({"stored_first_reply":case["name"]});
        state.replies.insert(
            first["method"].as_str().unwrap().into(),
            Reply {
                request: first["params"].clone(),
                value: witness.clone(),
            },
        );
        if let Some(cut) = first["params"]["cutId"].as_str() {
            broca_wal::migration::atomic_json(
                &rig.root,
                &CutStore::new(&rig.root).path(cut),
                &state,
            )
            .unwrap();
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.raw(serde_json::to_vec(&case["again"]).unwrap()).await;
        match case["outcome"].as_str().unwrap() {
            "replay" => assert_eq!(result.unwrap(), witness, "{}", case["name"]),
            "id_reused" => {
                assert_code(result, "id_reused");
            }
            "exempt" | "new_key" => match result {
                Ok(reply) => assert_ne!(reply, witness),
                Err(error) => assert_ne!(error["code"], "id_reused", "{}", case["name"]),
            },
            other => panic!("unknown vector outcome {other}"),
        }
    }
}
#[tokio::test]
async fn contract_ref_set_vectors_are_checked_before_artifacts() {
    use broca_module::cut_state::{CutState, CutStore};
    for case in vectors("ref-sets.json")["cases"].as_array().unwrap() {
        let rig = Rig::new("ref-set-vector", Arc::new(FakeDoor::default())).await;
        let state = CutState {
            step: serde_json::from_value(case["step"].clone()).unwrap(),
            refs: case["started_with"]
                .as_array()
                .map(|_| serde_json::from_value(case["started_with"].clone()).unwrap()),
            intent: None,
            sessions: Default::default(),
            replies: Default::default(),
        };
        broca_wal::migration::atomic_json(&rig.root, &CutStore::new(&rig.root).path(CUT), &state)
            .unwrap();
        let mut params = json!({"refs":case["refs"],"cutId":CUT});
        if case["op"] == "release" {
            params["holder"] = json!("receiver");
        }
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let result = wire.call(case["op"].as_str().unwrap(), params).await;
        if case["outcome"] == "ok" {
            if let Err(error) = result {
                assert_ne!(error["code"], "invalid_state", "{}", case["name"]);
            }
        } else {
            let error = assert_code(result, case["outcome"]["code"].as_str().unwrap());
            assert_eq!(
                error["detail"], case["outcome"]["detail"],
                "{}",
                case["name"]
            );
        }
    }
}
#[tokio::test]
async fn session_ref_vectors_are_resolved_by_the_serve_participant() {
    // The contract's vectors pin the shared derivation for any runner. broca derives its
    // sessions' refs as runner `broca`, so the vectors check the derivation, and a broca session
    // is resolved through the participant.
    for case in vectors("session-refs.json")["cases"].as_array().unwrap() {
        let identity = cortexkit_role_llm_runner::SessionIdentity {
            runner: case["runner"].as_str().unwrap(),
            harness: case["harness"].as_str().unwrap(),
            session: case["session"].as_str().unwrap(),
        };
        assert_eq!(
            cortexkit_role_llm_runner::session_ref(&identity),
            case["sessionRef"].as_str().unwrap(),
            "{case}"
        );
    }
    let cases = [
        json!({"harness":"broca","project_root":"/Users/ufukaltinok/.local/share/cortexkit/alfonso/worktrees/c0c39eea197fcc68/pool-952","session":"alfonso:bg_de298dcca128a1e1","sessionRef":"cc8f376395c1f915b357326b6741ddcb"}),
    ];
    for case in cases {
        let session = BindIdentity::new(
            case["project_root"].as_str().unwrap(),
            case["harness"].as_str().unwrap(),
            case["session"].as_str().unwrap(),
        );
        for reference in [
            case["sessionRef"].as_str().unwrap().to_owned(),
            broca_core::agent_session_ref(&session),
        ] {
            let rig = Rig::new("ref-vector", Arc::new(FakeDoor::default())).await;
            rig.seed(&session, true).await;
            let mut wire = Wire::new(&rig, entorhinal(), true).await;
            let reply = wire
                .call(
                    "prepare",
                    json!({"cutId":CUT,"refs":[{"ref_kind":"broca-session","ref":reference}]}),
                )
                .await
                .unwrap();
            assert_eq!(reply["items"][0]["sessionRef"], reference);
        }
    }
}

#[tokio::test]
async fn session_ref_v2_ambiguous_roots_refuse_through_serve() {
    let rig = Rig::new("ref-ambiguous", Arc::new(FakeDoor::default())).await;
    let one = BindIdentity::new("/synthetic/one", "runner", "shared");
    let two = BindIdentity::new("/synthetic/two", "runner", "shared");
    rig.seed(&one, true).await;
    rig.seed(&two, true).await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = wire.call("prepare", json!({"cutId":CUT,"refs":[{"ref_kind":"broca-session","ref":broca_core::agent_session_ref(&one)}]})).await.unwrap_err();
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("session_ref_ambiguous"),
        "{error}"
    );
}

#[cfg(unix)]
#[path = "agent_data/real_daemon.rs"]
mod real_daemon;

#[path = "agent_data/inventory.rs"]
mod inventory;

#[path = "agent_data/store_moves.rs"]
mod store_moves;

/// A session whose open batch holds a call its provider answers later (an approval or a
/// question) is busy: moving it would strand the answer.
#[tokio::test]
async fn a_session_with_a_held_call_is_busy_before_prepare() {
    use broca_session_store::record::{BatchStart, Record, ToolHeld, ToolIntent};
    let rig = Rig::new("approval-debt", Arc::new(FakeDoor::default())).await;
    let session = rig.session("approval");
    let mut seeded = seed_history(false);
    let run_id = seeded
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunStart(start) => Some(start.run_id.clone()),
            _ => None,
        })
        .unwrap();
    let call = broca_protocol::ToolCallId("approval-call".into());
    seeded.records.extend([
        Record::BatchStart(BatchStart {
            run_id: run_id.clone(),
            batch_id: "approval-batch".into(),
            step_id: 0,
            planned: vec![call.clone()],
            mid: None,
            shown_tags: vec![],
        }),
        Record::ToolIntent(ToolIntent {
            run_id,
            batch_id: "approval-batch".into(),
            tool_call_id: call.clone(),
            tool_name: Some("read_file".into()),
            module_id: Some("aft".into()),
            args_digest: broca_session_store::codec::Digest([0; 32]),
            args: None,
            call_key: Some("approval-key".into()),
            approval_digest: None,
            dispatched_at_ms: None,
            schema_pin: None,
            hooks: None,
            key_derived: None,
        }),
        Record::ToolHeld(ToolHeld {
            batch_id: "approval-batch".into(),
            tool_call_id: call,
            call_key: "approval-key".into(),
            module: "aft".into(),
            reason: "approval".into(),
            detail_digest: None,
            held_at_ms: 1,
            sequential: false,
        }),
    ]);
    store_session_with(&rig, &session, seeded, broca_wal::StorageClass::Durable).await;
    broca_core::SessionRefIndex::record_session(
        rig.index.as_ref(),
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let reply = wire
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["state"], "busy");
    assert_eq!(reply["refs"][0]["reason_code"], "approval_open");
    assert_code(wire.call("prepare", params).await, "busy");
    assert!(!rig.fence(&session).with_extension("moving").exists());
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

/// A source that already moved the session away (prepared, exported and released it under cut
/// `BACK`) refuses a later cut's `op` as an unknown ref naming where the session went, and
/// changes nothing it keeps.
async fn source_tombstone_case(op: contract::Op) {
    use broca_module::cut_state::{CutState, CutStore};
    let rig = Rig::new("source-moved-code", Arc::new(FakeDoor::default())).await;
    let session = rig.session("moved-source");
    rig.seed(&session, true).await;
    let back = rig.params(BACK, std::slice::from_ref(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("prepare", back.clone()).await.unwrap();
    wire.call("export", back.clone()).await.unwrap();
    wire.call(
        "release",
        json!({"cutId":BACK,"refs":back["refs"],"holder":"other-mac"}),
    )
    .await
    .unwrap();
    let mut params = rig.params(CUT, std::slice::from_ref(&session));
    let state = CutState {
        step: match op {
            contract::Op::Prepare => contract::step::Step::None,
            contract::Op::Export => contract::step::Step::Prepared,
            contract::Op::Release => contract::step::Step::Exported,
            _ => unreachable!(),
        },
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: None,
        sessions: recorded_sessions(std::slice::from_ref(&session)),
        replies: Default::default(),
    };
    broca_wal::migration::atomic_json(&rig.root, &CutStore::new(&rig.root).path(CUT), &state)
        .unwrap();
    if op == contract::Op::Release {
        params["holder"] = json!("new-mac");
    }
    let before = rig.stored(&session);
    let error = assert_code(
        wire.call(
            op.method().strip_prefix("agent_data.").unwrap(),
            params.clone(),
        )
        .await,
        "unknown_ref",
    );
    assert_eq!(error["detail"], params["refs"][0]);
    assert!(error["message"].as_str().unwrap().contains("other-mac"));
    assert!(error["message"].as_str().unwrap().contains(BACK));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(!contract::errors::driver_action_for(op, &error.code).retryable());
    assert!(contract::errors::may_refuse(op, &error.code));
    assert_eq!(rig.stored(&session), before);
}
#[tokio::test]
async fn prepare_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Prepare).await;
}
#[tokio::test]
async fn export_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Export).await;
}
#[tokio::test]
async fn release_tombstone_is_terminal_unknown_ref() {
    source_tombstone_case(contract::Op::Release).await;
}

/// A cut whose session's freeze names another cut is corrupt authority state: export and release
/// refuse it as a terminal invalid state and change neither record.
async fn contradictory_owner_case(op: contract::Op) {
    use broca_module::cut_state::CutStore;
    let rig = Rig::new("foreign-owner", Arc::new(FakeDoor::default())).await;
    let session = rig.session("foreign-owned");
    rig.seed(&session, true).await;
    let mut params = rig.params(CUT, std::slice::from_ref(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("prepare", params.clone()).await.unwrap();
    let step = if op == contract::Op::Export {
        contract::step::Step::Prepared
    } else {
        wire.call("export", params.clone()).await.unwrap();
        contract::step::Step::Exported
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    // The session's freeze is then rewritten to name cut `BACK`, its pinned version kept.
    let sidecar = rig.fence(&session).with_extension("moving");
    let mut freeze: broca_wal::migration::Freeze =
        broca_wal::migration::read_json(&sidecar).unwrap().unwrap();
    freeze.cut_id = BACK.into();
    broca_wal::migration::atomic_json(&rig.root, &sidecar, &freeze).unwrap();
    let old_sidecar = std::fs::read(&sidecar).unwrap();
    let old_authority = std::fs::read(&authority).unwrap();
    if op == contract::Op::Release {
        params["holder"] = json!("receiver");
    }
    let error = assert_code(
        wire.call(op.method().strip_prefix("agent_data.").unwrap(), params)
            .await,
        "invalid_state",
    );
    assert_eq!(error["detail"]["step"], serde_json::to_value(step).unwrap());
    assert!(error["message"].as_str().unwrap().contains(CUT));
    assert!(error["message"].as_str().unwrap().contains(BACK));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(!contract::errors::driver_action_for(op, &error.code).retryable());
    assert!(contract::errors::may_refuse(op, &error.code));
    assert_eq!(std::fs::read(sidecar).unwrap(), old_sidecar);
    assert_eq!(std::fs::read(authority).unwrap(), old_authority);
}
#[tokio::test]
async fn export_foreign_owner_is_terminal_invalid_state() {
    contradictory_owner_case(contract::Op::Export).await;
}
#[tokio::test]
async fn release_foreign_owner_is_terminal_invalid_state() {
    contradictory_owner_case(contract::Op::Release).await;
}

#[tokio::test]
async fn import_refuses_sealed_metadata_that_changes_between_pages() {
    let (rig, params, _) = incoming_rig("seal-drift").await;
    rig.door.read_limit.store(100, Ordering::SeqCst);
    rig.door.drift.store(true, Ordering::SeqCst);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("import", params).await, "unavailable");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("metadata or page range changed"));
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 2);
    assert_nothing_staged(&rig);
}

fn decode_reply(op: contract::Op, reply: &Value) -> Result<Value, serde_json::Error> {
    fn decode<T: serde::de::DeserializeOwned + serde::Serialize>(
        reply: &Value,
    ) -> Result<Value, serde_json::Error> {
        serde_json::to_value(serde_json::from_value::<T>(reply.clone())?)
    }
    use contract::*;
    match op {
        Op::Describe => decode::<describe::DescribeReply>(reply),
        Op::Busy => decode::<busy::BusyReply>(reply),
        Op::Status => decode::<status::StatusReply>(reply),
        Op::Prepare => decode::<prepare::PrepareReply>(reply),
        Op::Export => decode::<export::ExportReply>(reply),
        Op::Diverged => decode::<diverged::DivergedReply>(reply),
        Op::Import => decode::<import::ImportReply>(reply),
        Op::Activate => decode::<activate::ActivateReply>(reply),
        Op::Abort => decode::<abort::AbortReply>(reply),
        Op::Release => decode::<release::ReleaseReply>(reply),
    }
}
#[tokio::test]
async fn contract_reply_vectors_decode_the_real_internal_service_answers() {
    let file = vectors("replies.json");
    for case in file["replies"].as_array().unwrap() {
        let op = contract::Op::from_method(case["method"].as_str().unwrap()).unwrap();
        assert_eq!(
            decode_reply(op, &case["reply"]).unwrap(),
            case["reply"],
            "{}",
            case["name"]
        );
        let rig = Rig::new("reply-vector", Arc::new(FakeDoor::default())).await;
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let mut params = json!({"refs":[],"cutId":CUT});
        match op {
            contract::Op::Describe => params = json!({}),
            contract::Op::Busy => params = json!({"refs":[]}),
            contract::Op::Export => {
                wire.call("prepare", params.clone()).await.unwrap();
            }
            contract::Op::Activate => {
                wire.call("import", params.clone()).await.unwrap();
            }
            contract::Op::Release => {
                wire.call("prepare", params.clone()).await.unwrap();
                wire.call("export", params.clone()).await.unwrap();
                params["holder"] = json!("receiver");
            }
            _ => {}
        }
        let reply = wire
            .call(op.method().strip_prefix("agent_data.").unwrap(), params)
            .await
            .unwrap();
        assert_eq!(decode_reply(op, &reply).unwrap(), reply, "{}", case["name"]);
    }
    for case in file["refused"].as_array().unwrap() {
        let op = contract::Op::from_method(case["method"].as_str().unwrap()).unwrap();
        assert!(
            decode_reply(op, &case["reply"]).is_err(),
            "{}",
            case["name"]
        );
    }
}
#[tokio::test]
async fn contract_op_refusal_vectors_bound_served_decode_refusals() {
    for case in vectors("op-refusals.json")["ops"].as_array().unwrap() {
        let method = format!("agent_data.{}", case["op"].as_str().unwrap());
        let op = contract::Op::from_method(&method).unwrap();
        let declared = contract::errors::allowed_refusals(op)
            .iter()
            .map(|c| c.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut vector = case["codes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        // Codes added after the base table, each with its own vector file.
        for additions in ["digest-mismatch.json", "participant-failed.json"] {
            for addition in vectors(additions)["op_additions"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|a| a["op"] == case["op"])
            {
                vector.extend(
                    addition["codes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|code| code.as_str().unwrap().to_owned()),
                );
            }
        }
        let declared = declared
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(declared, vector);
        let rig = Rig::new("op-refusal-vector", Arc::new(FakeDoor::default())).await;
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let error = assert_code(
            wire.raw(
                serde_json::to_vec(&json!({"method":method,"params":{"unknown":true}})).unwrap(),
            )
            .await,
            "invalid_request",
        );
        assert!(vector.contains(error["code"].as_str().unwrap()));
    }
}

/// Gives `rig` its own copy of `session` kept with retention, in the ephemeral class, as a
/// session a sender asked to have deleted after a while.
async fn retained_copy(rig: &Rig, session: &BindIdentity) {
    store_session_with(
        rig,
        session,
        seed_history(true),
        broca_wal::StorageClass::Ephemeral,
    )
    .await;
    broca_core::SessionRefIndex::record_session(
        rig.index.as_ref(),
        &broca_core::session_addr(session),
        session,
    )
    .unwrap();
}
#[tokio::test]
async fn diverged_marks_a_retained_local_copy_advanced() {
    let (rig, params, _) = incoming_rig("retained-divergence").await;
    let session = rig.session("incoming");
    retained_copy(&rig, &session).await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params).await.unwrap(),
        json!({"refs":[{"state":"advanced"}]})
    );
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn import_reports_local_advanced_when_retention_appears_after_at_cut() {
    let (rig, params, _) = incoming_rig("retained-import").await;
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    retained_copy(&rig, &session).await;
    let before = rig.stored(&session);
    let error = assert_code(wire.call("import", params).await, "local_advanced");
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("held in a different storage class"));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert_eq!(
        contract::errors::driver_action_for(contract::Op::Import, &error.code),
        contract::errors::DriverAction::TerminalForStep
    );
    assert_eq!(rig.stored(&session), before);
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn activate_reports_local_advanced_when_retention_appears_after_import() {
    let (rig, params, _) = incoming_rig("retained-activate").await;
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    wire.call("import", params.clone()).await.unwrap();
    let staged = rig.staged(CUT, &session);
    assert!(staged.exists());
    retained_copy(&rig, &session).await;
    let before = rig.stored(&session);
    let error = assert_code(
        wire.call("activate", params.clone()).await,
        "local_advanced",
    );
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("held in a different storage class"));
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert_eq!(
        contract::errors::driver_action_for(contract::Op::Activate, &error.code),
        contract::errors::DriverAction::TerminalForStep
    );
    assert_eq!(rig.stored(&session), before);
    assert!(staged.exists());
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"imported"})
    );
}
#[tokio::test]
async fn activation_busy_freeze_has_typed_refs_and_preserves_staging() {
    let (rig, params, _) = incoming_rig("activation-freeze").await;
    let session = rig.session("incoming");
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("import", params.clone()).await.unwrap();
    // Meanwhile the receiver's own copy of the session is frozen by another cut.
    rig.seed(&session, true).await;
    wire.call("prepare", rig.params(BACK, std::slice::from_ref(&session)))
        .await
        .unwrap();
    let before = rig.stored(&session);
    let error = assert_code(wire.call("activate", params.clone()).await, "busy");
    assert_eq!(error["detail"]["refs"][0]["ref"], params["refs"][0]["ref"]);
    assert_eq!(error["detail"]["refs"][0]["reason_code"], "moving");
    assert!(rig.staged(CUT, &session).exists());
    assert_eq!(rig.stored(&session), before);
}

#[tokio::test]
async fn pending_import_abort_uses_only_owned_staging_and_never_the_door() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    for empty in [false, true] {
        let (rig, mut params, _) = incoming_rig("abort-partial-import").await;
        let session = rig.session("incoming");
        let other_session = rig.session("other-unheld");
        params["refs"].as_array_mut().unwrap().push(
            serde_json::to_value(contract::session_reference(
                &other_session.harness,
                &other_session.project_root.to_string_lossy(),
                &other_session.session,
            ))
            .unwrap(),
        );
        // An import that stopped while staging: a partial copy of the first session, and with
        // `empty` an empty staging folder for the second. Abort discards both without reading
        // what they hold.
        let staged = rig.staged(CUT, &session);
        std::fs::create_dir_all(&staged).unwrap();
        if !empty {
            std::fs::write(staged.join("partial"), b"interrupted staging write").unwrap();
        }
        let other_staged = rig.staged(CUT, &other_session);
        if empty {
            std::fs::create_dir_all(&other_staged).unwrap();
        }
        let other = rig.staged(BACK, &session);
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(other.join("other-cut"), b"other cut").unwrap();
        let state = CutState {
            step: contract::step::Step::None,
            refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
            intent: Some(Intent {
                op: "import".into(),
                request: params.clone(),
            }),
            sessions: recorded_sessions(&[session.clone(), other_session]),
            replies: Default::default(),
        };
        let authority = CutStore::new(&rig.root).path(CUT);
        broca_wal::migration::atomic_json(&rig.root, &authority, &state).unwrap();
        *rig.door.refusal.lock().unwrap() = Some("unavailable".into());
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        wire.call("abort", params.clone()).await.unwrap();
        assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
        assert!(!staged.exists());
        assert!(!other_staged.exists());
        assert_eq!(
            std::fs::read(other.join("other-cut")).unwrap(),
            b"other cut"
        );
        let state: CutState = serde_json::from_slice(&std::fs::read(authority).unwrap()).unwrap();
        assert_eq!(state.step, contract::step::Step::Aborted);
        assert!(state.intent.is_none());
        assert!(!state.replies.contains_key("agent_data.import"));
        rig.service.reconcile_boot().await.unwrap();
        assert_eq!(
            wire.call("status", params).await.unwrap(),
            json!({"step":"aborted"})
        );
    }
}
#[tokio::test]
async fn boot_keeps_an_unreadable_cut_pending_and_status_read_only() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let (rig, params, _) = incoming_rig("boot-unavailable").await;
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params.clone(),
        }),
        sessions: recorded_sessions(&[rig.session("incoming")]),
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&rig.root, &authority, &state).unwrap();
    let before = std::fs::read(&authority).unwrap();
    *rig.door.refusal.lock().unwrap() = Some("unavailable".into());
    rig.service.reconcile_boot().await.unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("status", params.clone()).await.unwrap(),
        json!({"step":"none"})
    );
    assert_eq!(std::fs::read(&authority).unwrap(), before);
    *rig.door.refusal.lock().unwrap() = None;
    wire.call("import", params.clone()).await.unwrap();
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"imported"})
    );
}
#[tokio::test]
async fn two_ref_import_replays_and_activation_installs_both_sessions() {
    let (params, streams) = exported_cut("two-ref", &["a", "b"]).await;
    let door = Arc::new(FakeDoor::default());
    for (reference, bytes) in &streams {
        door.plant(CUT, reference, bytes.clone());
    }
    let receiver = Rig::new("two-ref-receiver", door.clone()).await;
    let sessions = [receiver.session("a"), receiver.session("b")];
    let mut wire = Wire::new(&receiver, entorhinal(), true).await;
    let first = wire.call("import", params.clone()).await.unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    assert_eq!(wire.call("import", params.clone()).await.unwrap(), first);
    wire.call("activate", params).await.unwrap();
    for session in &sessions {
        receiver.registry.check_move(session, true).unwrap();
        assert!(!receiver.stored(session).is_empty());
    }
}

/// A finished session with a send queued for its next run is busy: moving it would leave the
/// send behind.
#[tokio::test]
async fn closed_session_with_a_queued_send_is_busy_before_prepare() {
    use broca_session_store::record::{QueuedSend, Record};
    let rig = Rig::new("queued-send", Arc::new(FakeDoor::default())).await;
    let session = rig.session("queued");
    let mut seeded = seed_history(true);
    seeded.records.push(Record::QueuedSend(QueuedSend {
        submission_id: "pending".into(),
        send_id: None,
        input: vec![],
        setup: vec![],
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: None,
        media: vec![],
        message_marks: None,
        named_speaker: None,
    }));
    store_session_with(&rig, &session, seeded, broca_wal::StorageClass::Durable).await;
    broca_core::SessionRefIndex::record_session(
        rig.index.as_ref(),
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let before = rig.stored(&session);
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let reply = wire
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(reply["refs"][0]["reason_code"], "send_queued");
    assert_code(wire.call("prepare", params).await, "busy");
    assert_eq!(rig.stored(&session), before);
    assert!(!rig.fence(&session).with_extension("moving").exists());
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct ErrorEvents(Arc<Mutex<Vec<String>>>);
impl tracing::Subscriber for ErrorEvents {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::ERROR
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0.push_str(&format!(" {}={value:?}", field.name()));
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
#[tokio::test]
async fn diverged_unheld_ref_is_at_cut_even_when_import_will_refuse_its_seal() {
    let (rig, params, _) = incoming_rig("unheld-corrupt-cut").await;
    *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let (frame_type, body) = wire.call_bytes("diverged", params.clone()).await;
    assert_eq!(frame_type, FrameType::Response);
    assert_eq!(body, br#"{"result":{"refs":[{"state":"at_cut"}]}}"#);
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert_code(wire.call("import", params).await, "digest_mismatch");
    assert_nothing_staged(&rig);
}
#[tokio::test]
async fn diverged_unheld_ref_does_not_read_the_door_or_complete_a_pending_import() {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let (rig, params, _) = incoming_rig("unheld-no-door").await;
    *rig.door.refusal.lock().unwrap() = Some("verify_failed".into());
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    assert_eq!(
        wire.call("diverged", params.clone()).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    let state = CutState {
        step: contract::step::Step::None,
        refs: Some(serde_json::from_value(params["refs"].clone()).unwrap()),
        intent: Some(Intent {
            op: "import".into(),
            request: params.clone(),
        }),
        sessions: recorded_sessions(&[rig.session("incoming")]),
        replies: Default::default(),
    };
    let authority = CutStore::new(&rig.root).path(CUT);
    broca_wal::migration::atomic_json(&rig.root, &authority, &state).unwrap();
    let before = std::fs::read(&authority).unwrap();
    assert_eq!(
        wire.call("diverged", params).await.unwrap(),
        json!({"refs":[{"state":"at_cut"}]})
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(authority).unwrap(), before);
}
#[tokio::test]
async fn diverged_held_ref_with_a_bad_cut_is_unknown_and_logs_the_cause() {
    for cause in ["verify_failed", "sealed SHA mismatch"] {
        let (rig, params, _) = incoming_rig("held-bad-cut").await;
        let session = rig.session("incoming");
        rig.seed(&session, true).await;
        let before = rig.stored(&session);
        if cause == "verify_failed" {
            *rig.door.refusal.lock().unwrap() = Some(cause.into());
        } else {
            *rig.door.digest.lock().unwrap() = Some("0".repeat(64));
        }
        let events = ErrorEvents(Arc::default());
        let dispatch = tracing::Dispatch::new(events.clone());
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let mut wire = Wire::new(&rig, entorhinal(), true).await;
        let (frame_type, body) = wire.call_bytes("diverged", params.clone()).await;
        assert_eq!(frame_type, FrameType::Response);
        assert_eq!(
            body,
            format!(r#"{{"result":{{"refs":[{{"reason":"{cause}","state":"unknown"}}]}}}}"#)
                .as_bytes()
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"result":{"refs":[{"state":"unknown","reason":cause}]}})
        );
        assert_eq!(rig.door.calls.load(Ordering::SeqCst), 1);
        assert_eq!(rig.stored(&session), before);
        assert_nothing_staged(&rig);
        let logs = events.0.lock().unwrap().join("\n");
        assert!(logs.contains(cause), "{logs}");
        assert!(logs.contains(CUT), "{logs}");
        assert!(
            logs.contains(params["refs"][0]["ref"].as_str().unwrap()),
            "{logs}"
        );
        assert!(!logs.contains("tool transcript result"));
    }
}

/// While another holder has a session's lease, export and release of its cut answer a
/// retryable `unavailable`, and both succeed once the lease is free.
#[tokio::test]
async fn source_export_and_release_lease_contention_use_retryable_unavailable() {
    use broca_core::LeaseStore as _;
    let rig = Rig::new("source-lease", Arc::new(FakeDoor::default())).await;
    let session = rig.session("leased");
    rig.seed(&session, true).await;
    let params = rig.params(CUT, std::slice::from_ref(&session));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("prepare", params.clone()).await.unwrap();
    let leases = broca_core::FileLeaseStore::new(rig.root.join("leases"));
    let held = leases
        .acquire(&broca_core::lease_key_for(&session))
        .unwrap();
    let error = assert_code(wire.call("export", params.clone()).await, "unavailable");
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(contract::errors::driver_action_for(contract::Op::Export, &error.code).retryable());
    drop(held);
    wire.call("export", params.clone()).await.unwrap();
    let mut release = params.clone();
    release["holder"] = json!("receiver");
    let held = leases
        .acquire(&broca_core::lease_key_for(&session))
        .unwrap();
    let error = assert_code(wire.call("release", release.clone()).await, "unavailable");
    let error: contract::errors::ErrorBody = serde_json::from_value(error).unwrap();
    assert!(contract::errors::driver_action_for(contract::Op::Release, &error.code).retryable());
    drop(held);
    wire.call("release", release).await.unwrap();
}

/// A second cut can't take over or abort a session another cut froze, also after a restart.
#[tokio::test]
async fn two_cut_ids_cannot_replace_or_abort_the_others_freeze_across_restart() {
    let rig = Rig::new("two-cuts", Arc::new(FakeDoor::default())).await;
    let session = rig.session("frozen");
    rig.seed(&session, true).await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("prepare", rig.params(CUT, std::slice::from_ref(&session)))
        .await
        .unwrap();
    let freeze = rig.fence(&session).with_extension("moving");
    let before = std::fs::read(&freeze).unwrap();
    let stored = rig.stored(&session);
    drop(wire);
    let restarted = rig.reopen().await;
    let mut wire = Wire::new(&restarted, entorhinal(), true).await;
    let other = restarted.params(BACK, std::slice::from_ref(&session));
    assert_code(wire.call("prepare", other.clone()).await, "busy");
    wire.call("abort", other).await.unwrap();
    assert_eq!(std::fs::read(&freeze).unwrap(), before);
    assert_eq!(restarted.stored(&session), stored);
    assert!(matches!(
        restarted.registry.check_move(&session, true),
        Err(OpenError::SessionMoving { .. })
    ));
}

#[tokio::test]
async fn refused_first_prepare_does_not_capture_refs_for_the_next_request() {
    let rig = Rig::new("new-first-refs", Arc::new(FakeDoor::default())).await;
    let a = rig.session("a");
    let b = rig.session("b");
    rig.seed(&a, true).await;
    rig.seed(&b, true).await;
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    rig.door.fail_cover_once.store(true, Ordering::SeqCst);
    assert_code(
        wire.call("prepare", rig.params(CUT, std::slice::from_ref(&a)))
            .await,
        "unavailable",
    );
    assert!(!rig.fence(&a).with_extension("moving").exists());
    let params = rig.params(CUT, std::slice::from_ref(&b));
    let prepared = wire.call("prepare", params.clone()).await.unwrap();
    assert_eq!(prepared["items"][0]["ref"], params["refs"][0]["ref"]);
    assert!(!rig.fence(&a).with_extension("moving").exists());
    assert!(rig.fence(&b).with_extension("moving").exists());
    assert_eq!(
        wire.call("prepare", params.clone()).await.unwrap(),
        prepared
    );
    let exported = wire.call("export", params.clone()).await.unwrap();
    assert_eq!(exported["items"][0]["sessionRef"], params["refs"][0]["ref"]);
    wire.call("abort", params.clone()).await.unwrap();
    assert!(!rig.fence(&b).with_extension("moving").exists());
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"aborted"})
    );
}

/// Makes `session`'s stored head unreadable, as a damaged disk would.
fn unreadable(rig: &Rig, session: &BindIdentity) {
    let head = broca_wal::StorageClass::Durable
        .store_root(&rig.root)
        .join("heads")
        .join(broca_core::session_addr(session))
        .join("head");
    assert!(head.is_file(), "{}", head.display());
    std::fs::write(head, b"not a session head").unwrap();
}

#[tokio::test]
async fn healthy_cut_abort_uses_recorded_addresses_beside_an_unreadable_wal() {
    let mut rig = Rig::new("abort-bad-neighbor", Arc::new(FakeDoor::default())).await;
    let a = rig.session("a");
    let b = rig.session("b");
    rig.seed(&a, true).await;
    rig.seed(&b, true).await;
    let params = rig.params(CUT, std::slice::from_ref(&a));
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("prepare", params.clone()).await.unwrap();
    let state: Value = serde_json::from_slice(
        &std::fs::read(broca_module::cut_state::CutStore::new(&rig.root).path(CUT)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        state["sessions"][params["refs"][0]["ref"].as_str().unwrap()],
        serde_json::to_value(&a).unwrap()
    );
    unreadable(&rig, &b);
    let events = ErrorEvents(Arc::default());
    let dispatch = tracing::Dispatch::new(events.clone());
    let _log = tracing::dispatcher::set_default(&dispatch);
    drop(wire);
    rig.service = Arc::new(AgentDataService::new(
        rig.root.clone(),
        rig.registry.as_ref().clone(),
        rig.index.clone(),
        rig.door.clone(),
    ));
    rig.service.reconcile_boot().await.unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    wire.call("abort", params.clone()).await.unwrap();
    assert!(!rig.fence(&a).with_extension("moving").exists());
    assert_eq!(
        wire.call("status", params).await.unwrap(),
        json!({"step":"aborted"})
    );
    assert!(
        !events
            .0
            .lock()
            .unwrap()
            .join("\n")
            .contains(&broca_core::session_addr(&b)),
        "cut recovery scanned its unreadable neighbor"
    );
}

#[tokio::test]
async fn divergence_is_per_ref_when_one_held_session_is_unreadable() {
    let rig = Rig::new("diverged-bad-neighbor", Arc::new(FakeDoor::default())).await;
    let a = rig.session("a");
    let b = rig.session("b");
    rig.seed(&a, true).await;
    rig.seed(&b, true).await;
    // `a` was exported here and the move abandoned: the door holds this computer's own copy,
    // sealed under cut `BACK`.
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let exported = rig.params(CUT, std::slice::from_ref(&a));
    wire.call("prepare", exported.clone()).await.unwrap();
    wire.call("export", exported.clone()).await.unwrap();
    wire.call("abort", exported).await.unwrap();
    rig.door.seal(BACK);
    unreadable(&rig, &b);
    let calls = rig.door.calls.load(Ordering::SeqCst);
    assert_eq!(
        wire.call("diverged", rig.params(BACK, &[a, b]))
            .await
            .unwrap(),
        json!({"refs":[{"state":"at_cut"},{"state":"unknown"}]})
    );
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), calls + 1);
}

#[tokio::test]
async fn cut_without_required_addresses_is_named_unreadable_before_lookup() {
    use broca_module::cut_state::CutStore;
    let rig = Rig::new("mapless-cut", Arc::new(FakeDoor::default())).await;
    let a = rig.session("a");
    let b = rig.session("b");
    rig.seed(&a, true).await;
    rig.seed(&b, true).await;
    unreadable(&rig, &b);
    let params = rig.params(CUT, std::slice::from_ref(&a));
    let path = CutStore::new(&rig.root).path(CUT);
    let state = json!({"step":"prepared","refs":params["refs"],"intent":null,"replies":{}});
    broca_wal::migration::atomic_json(&rig.root, &path, &state).unwrap();
    let before = std::fs::read(&path).unwrap();
    let mut wire = Wire::new(&rig, entorhinal(), true).await;
    let error = assert_code(wire.call("status", params).await, "unavailable");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains(path.to_str().unwrap()),
        "{error}"
    );
    assert!(
        error["message"].as_str().unwrap().contains("sessions"),
        "{error}"
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(rig.door.calls.load(Ordering::SeqCst), 0);
}
