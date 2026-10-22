//! Routes that declare the `classifier` role: what they serve, what they refuse, and that a
//! hung System One provider never delays session work on the same connection.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse,
    SessionReadResponse, StatusResponse,
};
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};

use super::*;
use crate::system_one::{
    BatchCatalog, ClassifierProvider, Config, ModelPin, ProviderReply, SystemOne, JEV_1_13_0,
};

/// A session handler that answers reads and sends at once, counting each.
#[derive(Default)]
struct InstantSessions {
    reads: AtomicUsize,
    sends: AtomicUsize,
}

#[async_trait::async_trait]
impl ServeHandler for InstantSessions {
    async fn send(&self, _req: SendRequest) -> Result<SendResponse, ServeError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Err(ServeError::UnknownSend)
    }
    async fn retract(
        &self,
        _session: &BrocaBindIdentity,
        _submission_id: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unimplemented!()
    }
    async fn subscribe(
        &self,
        _session: &BrocaBindIdentity,
        _from: SubscribeFrom,
        _sink: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unimplemented!()
    }
    async fn read(&self, _req: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(SessionReadResponse {
            next_late_after: None,
            deferred_calls: None,
            late_approvals: None,
            open_batch: None,
            lineage_id: None,
            messages: Vec::new(),
            next_from_ordinal: None,
            head: None,
            lineage_state: broca_wire::SessionReadLineageState {
                last_run_id: None,
                state: "idle".into(),
                reason: None,
                error: None,
                usage: None,
            },
            tools: None,
            tools_run_id: None,
        })
    }
    async fn cancel(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unimplemented!()
    }
    async fn status(&self, _session: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        unimplemented!()
    }
    async fn run_status(
        &self,
        _session: &BrocaBindIdentity,
        _run_id: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unimplemented!()
    }
}

struct NoLineages;

#[async_trait::async_trait]
impl broca_module::registry::LineageOpener for NoLineages {
    async fn open(
        &self,
        _key: &BrocaBindIdentity,
        _gen: u64,
        _registry: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        Err(broca_module::registry::OpenError::Open(
            "this test opens no lineage".into(),
        ))
    }
}

/// A provider whose every call waits for a permit the test grants.
struct HeldProvider {
    gate: Semaphore,
    entered: Notify,
    held: AtomicUsize,
}

#[async_trait::async_trait]
impl ClassifierProvider for HeldProvider {
    fn egress_host(&self) -> String {
        "stand-in.invalid".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _body: String) -> ProviderReply {
        self.held.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        let _permit = self.gate.acquire().await.unwrap();
        ProviderReply::Answered(
            br#"{"model":"jev-latest","answers":{"q":{"type":"noul","noul":0.5}}}"#.to_vec(),
        )
    }
}

struct JevOnly;

impl BatchCatalog for JevOnly {
    fn row(&self, catalog_id: &str) -> Option<broca_catalog::ModelBatchRow> {
        (catalog_id == JEV_1_13_0).then(|| broca_catalog::ModelBatchRow {
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
        })
    }
}

struct Rig {
    daemon: tokio::io::DuplexStream,
    handler: Arc<InstantSessions>,
    provider: Arc<HeldProvider>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, ServeIoError>>,
}

async fn start() -> Rig {
    let (mut daemon, module) = tokio::io::duplex(1 << 20);
    let (reader, writer) = tokio::io::split(module);
    let handler = Arc::new(InstantSessions::default());
    let provider = Arc::new(HeldProvider {
        gate: Semaphore::new(0),
        entered: Notify::new(),
        held: AtomicUsize::new(0),
    });
    let system_one = Arc::new(SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "test",
        Arc::new(JevOnly),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    ));
    let serve = tokio::spawn(serve_connection_with_services(
        reader,
        writer,
        "broca",
        handler.clone(),
        Arc::new(LineageRegistry::new(Arc::new(NoLineages))),
        VaultGauge::not_in_use(),
        None,
        crate::wal_archive::WalArchiveConfig::default(),
        crate::consumer_routes::ConsumerRoutes::default(),
        None,
        None,
        None,
        None,
        Some(system_one),
    ));
    let hello = read_frame(&mut daemon).await.unwrap().unwrap();
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::HelloAck,
            control_flags(),
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
    .await
    .unwrap();
    Rig {
        daemon,
        handler,
        provider,
        serve,
    }
}

impl Rig {
    /// Bind `channel` declaring `roles`; return the bind's answer frame.
    async fn bind(&mut self, channel: u16, roles: &[(&str, &str)]) -> Frame {
        let role_versions: BTreeMap<String, String> = roles
            .iter()
            .map(|(role, version)| (role.to_string(), version.to_string()))
            .collect();
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("/tmp/project-a", "runner", "classifier-test"),
            principal: None,
            consumer_capabilities: None,
            admission_facts: None,
            scope: None,
            role_versions: (!role_versions.is_empty()).then_some(role_versions),
            call_mode: None,
        };
        write_frame(
            &mut self.daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                1000 + u64::from(channel),
                serde_json::to_vec(&bind).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        read_frame(&mut self.daemon).await.unwrap().unwrap()
    }

    async fn request(&mut self, channel: u16, corr: u64, method: &str, params: Value) {
        write_frame(
            &mut self.daemon,
            &Frame::build(
                FrameType::Request,
                control_flags(),
                channel,
                1,
                corr,
                serde_json::to_vec(&json!({"method": method, "params": params})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }

    async fn reply(&mut self) -> Frame {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            read_frame(&mut self.daemon),
        )
        .await
        .expect("no reply within 10s")
        .unwrap()
        .unwrap()
    }

    async fn finish(mut self) {
        write_frame(
            &mut self.daemon,
            &Frame::build(FrameType::Goodbye, control_flags(), 0, 0, 9999, vec![]).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(self.serve.await.unwrap().unwrap(), ServeEnd::Goodbye);
    }
}

fn code(frame: &Frame) -> String {
    assert_eq!(frame.header.ty, FrameType::Error, "{frame:?}");
    serde_json::from_slice::<Value>(&frame.body).unwrap()["code"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn classify_params(batch: &str) -> Value {
    json!({
        "batch_id": batch,
        "model": JEV_1_13_0,
        "questions": {"q": {"type": "noul", "instructions": "Is it flagged?"}},
        "items": [{"state": "a"}],
    })
}

#[tokio::test]
async fn a_classifier_route_serves_only_the_classifier_role() {
    let mut rig = start().await;
    assert_eq!(
        rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS])
            .await
            .header
            .ty,
        FrameType::Response
    );
    rig.request(9, 1, "role.describe", json!({})).await;
    let describe = rig.reply().await;
    assert_eq!(describe.header.ty, FrameType::Response);
    let describe: Value = serde_json::from_slice(&describe.body).unwrap();
    assert_eq!(describe["result"]["role"], "classifier/v1");
    assert_eq!(describe["result"]["models"][0]["model"], JEV_1_13_0);
    for (corr, method) in [
        (2, "session.read"),
        (3, "session.send"),
        (4, "session.head"),
        (5, "run.status"),
        (6, "session.subscribe"),
    ] {
        rig.request(9, corr, method, json!({})).await;
        let reply = rig.reply().await;
        assert_eq!(reply.header.corr, corr);
        assert_eq!(code(&reply), "unknown_method", "{method}");
    }
    assert_eq!(rig.handler.reads.load(Ordering::SeqCst), 0);
    assert_eq!(rig.handler.sends.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[tokio::test]
async fn a_route_without_the_classifier_role_refuses_classify_run() {
    let mut rig = start().await;
    assert_eq!(rig.bind(7, &[]).await.header.ty, FrameType::Response);
    rig.request(7, 1, "classify.run", classify_params("b"))
        .await;
    assert_eq!(code(&rig.reply().await), "unknown_method");
    rig.request(7, 2, "role.describe", json!({})).await;
    let describe: Value = serde_json::from_slice(&rig.reply().await.body).unwrap();
    assert!(
        describe["result"]["majors"].is_array(),
        "an llm-runner route keeps the llm-runner describe: {describe}"
    );
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[tokio::test]
async fn a_bind_naming_two_roles_or_an_unserved_version_is_refused() {
    let mut rig = start().await;
    let both = rig
        .bind(9, &[("classifier", "v1"), ("llm-runner", "v1")])
        .await;
    assert_eq!(code(&both), ROLE_VERSIONS_UNSUPPORTED);
    let v2 = rig.bind(10, &[("classifier", "v2")]).await;
    assert_eq!(code(&v2), ROLE_VERSIONS_UNSUPPORTED);
    // Refused binds bind nothing: a request on either channel finds no route.
    assert!(!rig_has_route(&mut rig, 10).await);
    rig.finish().await;
}

/// A request on an unbound channel is dropped by the serve loop's binding guard; a role.describe
/// on a bound one is answered. Tell them apart by sending one and then a ping.
async fn rig_has_route(rig: &mut Rig, channel: u16) -> bool {
    rig.request(channel, 77, "role.describe", json!({})).await;
    write_frame(
        &mut rig.daemon,
        &Frame::build(FrameType::Ping, control_flags(), 0, 0, 78, vec![]).unwrap(),
    )
    .await
    .unwrap();
    let frame = rig.reply().await;
    frame.header.corr == 77
}

/// A classify.run whose provider never answers holds only itself: session reads and sends on
/// another route of the same connection are answered while it waits, and it is answered once
/// the provider is.
#[tokio::test]
async fn a_hung_classifier_provider_never_delays_session_work() {
    let mut rig = start().await;
    assert_eq!(rig.bind(7, &[]).await.header.ty, FrameType::Response);
    assert_eq!(
        rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS])
            .await
            .header
            .ty,
        FrameType::Response
    );
    // Enough held batches to fill every one of the connection's request slots, were they
    // taken: none may be.
    let batches = MAX_INFLIGHT_ROUTE_REQUESTS as u64 + 4;
    for n in 0..batches {
        rig.request(
            9,
            100 + n,
            "classify.run",
            classify_params(&format!("held-{n}")),
        )
        .await;
    }
    while rig.provider.held.load(Ordering::SeqCst) == 0 {
        rig.provider.entered.notified().await;
    }
    rig.request(7, 1, "session.read", json!({})).await;
    rig.request(
        7,
        2,
        "session.send",
        json!({"prompt": "next", "send_id": "turn-1",
               "model": {"provider": "fake", "model": "fake"}}),
    )
    .await;
    let mut answered = std::collections::BTreeSet::new();
    while answered.len() < 2 {
        let frame = rig.reply().await;
        assert!(
            frame.header.corr < 100,
            "a classify.run answered while its provider was held: {frame:?}"
        );
        answered.insert(frame.header.corr);
    }
    assert_eq!(rig.handler.reads.load(Ordering::SeqCst), 1);
    assert!(rig.provider.held.load(Ordering::SeqCst) >= 1);
    rig.provider.gate.add_permits(batches as usize);
    let mut classified = std::collections::BTreeSet::new();
    while classified.len() < batches as usize {
        let frame = rig.reply().await;
        assert_eq!(frame.header.ty, FrameType::Response, "{frame:?}");
        classified.insert(frame.header.corr);
    }
    rig.finish().await;
}
