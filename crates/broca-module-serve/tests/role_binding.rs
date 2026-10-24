//! Role declarations select isolated management routes; sessionless roles never touch a lineage.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broca_module::fanout::Delivery;
use broca_module::registry::{ActorHandle, LineageOpener, OpenError, RegistryHandle};
use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse, ServeError,
    ServeHandler, SessionReadRequest, SessionReadResponse, StatusResponse, SubscribeFrom,
};
use broca_module::LineageRegistry;
use broca_module_serve::serve::{
    serve_connection_with_services, ServeEnd, ServeIoError, VaultGauge,
};
use broca_module_serve::system_one::{BatchCatalog, Config, SystemOne};
use broca_protocol::BindIdentity;
use serde_json::{json, Value};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Priority,
    PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::mpsc;

struct NoSessions;

#[async_trait::async_trait]
impl ServeHandler for NoSessions {
    async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
        panic!("role dispatch reached session.send")
    }
    async fn retract(
        &self,
        _: &BindIdentity,
        _: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        panic!("role dispatch reached session.retract")
    }
    async fn subscribe(
        &self,
        _: &BindIdentity,
        _: SubscribeFrom,
        _: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        panic!("role dispatch reached session.subscribe")
    }
    async fn read(&self, _: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        panic!("role dispatch reached session.read")
    }
    async fn cancel(
        &self,
        _: &BindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        panic!("role dispatch reached run.cancel")
    }
    async fn status(&self, _: &BindIdentity) -> Result<StatusResponse, ServeError> {
        panic!("role dispatch reached session.status")
    }
    async fn run_status(
        &self,
        _: &BindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        panic!("role dispatch reached run.status")
    }
}

#[derive(Default)]
struct NoLineages(AtomicUsize);

#[async_trait::async_trait]
impl LineageOpener for NoLineages {
    async fn open(
        &self,
        _: &BindIdentity,
        _: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(OpenError::Open("sessionless route opened a lineage".into()))
    }
}

#[derive(Default)]
struct PoisonWal(AtomicUsize);

impl broca_core::WalStore for PoisonWal {
    fn open(
        &self,
        _: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        panic!("sessionless route opened a WAL writer")
    }

    fn read_records(
        &self,
        _: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(broca_wal::WalError::Io(std::io::Error::other(
            "poisoned WAL",
        )))
    }
}

struct EmptyCatalog;

impl BatchCatalog for EmptyCatalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        None
    }
}

fn flags() -> Flags {
    Flags::new(false, Priority::Passive, false)
}

struct Rig {
    daemon: tokio::io::DuplexStream,
    opens: Arc<NoLineages>,
    history: Arc<PoisonWal>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, ServeIoError>>,
}

impl Rig {
    async fn start(classifier: bool) -> Self {
        let (daemon, module) = tokio::io::duplex(1 << 20);
        let (reader, writer) = tokio::io::split(module);
        let opens = Arc::new(NoLineages::default());
        let history = Arc::new(PoisonWal::default());
        let system_one = classifier.then(|| {
            Arc::new(SystemOne::new(
                Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
                "test",
                Arc::new(EmptyCatalog),
                vec![],
                Config::default(),
                Arc::new(AtomicUsize::new(0)),
            ))
        });
        let serve = tokio::spawn(serve_connection_with_services(
            reader,
            writer,
            "broca",
            Arc::new(NoSessions),
            Arc::new(LineageRegistry::new_with_reader(
                opens.clone(),
                history.clone(),
                None,
            )),
            VaultGauge::not_in_use(),
            None,
            Default::default(),
            Default::default(),
            None,
            None,
            None,
            None,
            system_one,
        ));
        let mut rig = Self {
            daemon,
            opens,
            history,
            serve,
        };
        let hello = rig.reply().await;
        assert_eq!(hello.header.ty, FrameType::Hello);
        rig.write(
            Frame::build(
                FrameType::HelloAck,
                flags(),
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
            .unwrap(),
        )
        .await;
        rig
    }

    async fn write(&mut self, frame: Frame) {
        write_frame(&mut self.daemon, &frame).await.unwrap();
    }

    async fn reply(&mut self) -> Frame {
        tokio::time::timeout(Duration::from_secs(10), read_frame(&mut self.daemon))
            .await
            .expect("serve did not answer within 10 seconds")
            .unwrap()
            .unwrap()
    }

    async fn bind(
        &mut self,
        channel: u16,
        roles: Option<&[(&str, &str)]>,
        sessionless: bool,
    ) -> Frame {
        self.bind_with_stamp(channel, roles, sessionless, sessionless)
            .await
    }

    async fn bind_with_stamp(
        &mut self,
        channel: u16,
        roles: Option<&[(&str, &str)]>,
        sessionless: bool,
        stamped: bool,
    ) -> Frame {
        // Sessionless binds send an invalid identity to prove they bypass key validation.
        // They also carry a scope stamp, which must not trigger session-history authorization.
        let identity = if sessionless {
            SubcBindIdentity::new("/role-binding-project", "runner", "not\u{1f}keyable")
        } else {
            SubcBindIdentity::new("/role-binding-project", "runner", "role-binding")
        };
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity,
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: stamped.then(|| {
                serde_json::from_value(json!({
                    "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
                    "ref": "run", "scope_epoch": 1, "kind": "head", "owner_authorized": true,
                    "attributes": {"agent_id": "agent", "run_id": "run"}
                }))
                .unwrap()
            }),
            role_versions: roles.map(|roles| {
                roles
                    .iter()
                    .map(|(role, version)| (role.to_string(), version.to_string()))
                    .collect::<BTreeMap<_, _>>()
            }),
            call_mode: None,
        };
        self.write(
            Frame::build(
                FrameType::Request,
                flags(),
                0,
                0,
                1000 + u64::from(channel),
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await;
        self.reply().await
    }

    async fn request(&mut self, channel: u16, method: &str) -> Frame {
        self.write(
            Frame::build(
                FrameType::Request,
                flags(),
                channel,
                1,
                1,
                serde_json::to_vec(&json!({"method": method, "params": {}})).unwrap(),
            )
            .unwrap(),
        )
        .await;
        self.reply().await
    }

    async fn finish(mut self) {
        self.write(Frame::build(FrameType::Goodbye, flags(), 0, 0, 9999, vec![]).unwrap())
            .await;
        assert_eq!(self.serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
        assert_eq!(
            self.opens.0.load(Ordering::SeqCst),
            0,
            "a route opened session history"
        );
    }
}

fn body(frame: &Frame) -> Value {
    serde_json::from_slice(&frame.body).unwrap()
}

fn assert_unknown(frame: &Frame, method: &str) {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "{method}: {}",
        body(frame)
    );
    assert_eq!(
        body(frame)["code"],
        "unknown_method",
        "{method}: {}",
        body(frame)
    );
}

#[tokio::test]
async fn legacy_and_explicit_runner_bindings_keep_the_same_bytes() {
    let mut rig = Rig::start(true).await;
    let declarations: &[Option<&[(&str, &str)]>] = &[
        None,
        Some(&[]),
        Some(&[("unrelated-role", "v9")]),
        Some(&[("llm-runner", "v1")]),
    ];
    let mut legacy = None;
    for (index, roles) in declarations.iter().enumerate() {
        let channel = index as u16 + 1;
        assert_eq!(
            rig.bind(channel, *roles, false).await.header.ty,
            FrameType::Response
        );
        let reply = rig.request(channel, "role.describe").await;
        assert_eq!(reply.header.ty, FrameType::Response);
        assert!(body(&reply)["result"]["majors"].is_array());
        if let Some(bytes) = &legacy {
            assert_eq!(&reply.body, bytes);
        } else {
            legacy = Some(reply.body);
        }
        for method in [
            "classify.run",
            "tool.catalog",
            "tool.call",
            "late_results",
            "late_results.ack",
            "permissions.list",
            "permissions.revoke",
        ] {
            assert_unknown(&rig.request(channel, method).await, method);
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn classifier_binding_remains_sessionless_and_role_isolated() {
    let mut rig = Rig::start(true).await;
    assert_eq!(
        rig.bind(1, Some(&[("classifier", "v1")]), true)
            .await
            .header
            .ty,
        FrameType::Response
    );
    let reply = rig.request(1, "role.describe").await;
    assert_eq!(reply.header.ty, FrameType::Response);
    assert_eq!(body(&reply)["result"]["role"], "classifier/v1");
    for method in [
        "session.send",
        "session.read",
        "session.subscribe",
        "tool.catalog",
        "tool.call",
        "late_results",
        "late_results.ack",
        "permissions.list",
        "permissions.revoke",
    ] {
        assert_unknown(&rig.request(1, method).await, method);
    }
    rig.finish().await;
}

#[tokio::test]
async fn new_roles_are_sessionless_and_answer_only_unknown_method() {
    // Neither placeholder role depends on the classifier being configured.
    let mut rig = Rig::start(false).await;
    // The poisoned reader must refuse an ordinary stamped bind, so a passing sessionless
    // bind cannot be explained by a fixture that silently skips history authorization.
    let ordinary = rig
        .bind_with_stamp(3, Some(&[("llm-runner", "v1")]), false, true)
        .await;
    assert_eq!(ordinary.header.ty, FrameType::Error);
    assert!(rig.history.0.swap(0, Ordering::SeqCst) > 0);
    for (channel, role) in [(1, "tool-provider"), (2, "permissions")] {
        let bound = rig.bind(channel, Some(&[(role, "v1")]), true).await;
        assert_eq!(
            bound.header.ty,
            FrameType::Response,
            "{role}: {}",
            body(&bound)
        );
        for method in [
            "role.describe",
            "classify.run",
            "session.send",
            "session.read",
            "session.head",
            "session.subscribe",
            "run.status",
            "vault.audit",
            "tool.catalog",
            "tool.call",
            "late_results",
            "late_results.ack",
            "permissions.list",
            "permissions.revoke",
            "unrecognized.op",
        ] {
            assert_unknown(&rig.request(channel, method).await, method);
        }
    }
    assert_eq!(
        rig.history.0.load(Ordering::SeqCst),
        0,
        "sessionless routes read session history"
    );
    rig.finish().await;
}

#[tokio::test]
async fn mixed_roles_are_refused_as_one_role_per_route() {
    let mut rig = Rig::start(true).await;
    let roles = ["llm-runner", "classifier", "tool-provider", "permissions"];
    for (index, first) in roles.iter().enumerate() {
        for second in &roles[index + 1..] {
            let reply = rig
                .bind(1, Some(&[(first, "v1"), (second, "v1")]), false)
                .await;
            assert_eq!(reply.header.ty, FrameType::Error);
            assert_eq!(body(&reply)["code"], "role_versions_unsupported");
            assert!(body(&reply)["message"]
                .as_str()
                .unwrap()
                .starts_with("a route serves one role;"));
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn unsupported_role_versions_are_refused() {
    let mut rig = Rig::start(true).await;
    for role in ["llm-runner", "classifier", "tool-provider", "permissions"] {
        let reply = rig.bind(1, Some(&[(role, "v2")]), false).await;
        assert_eq!(reply.header.ty, FrameType::Error);
        assert_eq!(body(&reply)["code"], "role_versions_unsupported");
        assert!(body(&reply)["message"]
            .as_str()
            .unwrap()
            .contains(&format!("this build serves {role} v1 only")));
    }
    rig.finish().await;
}

#[tokio::test]
async fn unconfigured_classifier_is_still_refused() {
    let mut rig = Rig::start(false).await;
    let reply = rig.bind(1, Some(&[("classifier", "v1")]), false).await;
    assert_eq!(reply.header.ty, FrameType::Error);
    assert_eq!(body(&reply)["code"], "role_versions_unsupported");
    assert_eq!(
        body(&reply)["message"],
        "this build does not serve classifier v1"
    );
    rig.finish().await;
}

#[test]
fn manifest_does_not_advertise_unimplemented_roles_or_operations() {
    use subc_protocol::manifest::ProviderRole;
    let manifest = broca_module_serve::manifest::manifest("broca");
    let capabilities = manifest.capabilities.as_ref().unwrap();
    for capability in ["llm-runner/v1", "classifier/v1"] {
        assert!(
            capabilities
                .provides
                .iter()
                .any(|provided| provided == capability),
            "missing {capability}"
        );
    }
    for capability in ["tool-provider/v1", "permissions/v1"] {
        assert!(
            !capabilities
                .provides
                .iter()
                .any(|provided| provided == capability),
            "unimplemented role advertised: {capability}"
        );
    }
    let operations = manifest
        .provides
        .iter()
        .find_map(|role| match role {
            ProviderRole::ManagementSurface { operations, .. } => Some(operations),
            _ => None,
        })
        .unwrap();
    for name in [
        "tool.catalog",
        "tool.call",
        "late_results",
        "late_results.ack",
        "permissions.list",
        "permissions.revoke",
    ] {
        assert!(
            !operations.iter().any(|operation| operation.name == name),
            "unimplemented operation advertised: {name}"
        );
    }
}
