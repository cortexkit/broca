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
        ProviderReply::AttributedAnswer {
            body: br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec(),
            auth_selection: Some("apikey:typesafe:primary".into()), account_ref: Some("primary".into()),
        }
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
    store: Arc<broca_store::SystemOneStore>,
    system_one: Arc<SystemOne>,
    export: Arc<broca_store::SqliteStore>,
    serve: tokio::task::JoinHandle<Result<ServeEnd, ServeIoError>>,
}

async fn start() -> Rig {
    start_with_basis(crate::billing_routing::ChargeBasisTable::shipped_defaults()).await
}

async fn start_with_basis(table: crate::billing_routing::ChargeBasisTable) -> Rig {
    let (mut daemon, module) = tokio::io::duplex(1 << 20);
    let (reader, writer) = tokio::io::split(module);
    let handler = Arc::new(InstantSessions::default());
    let provider = Arc::new(HeldProvider {
        gate: Semaphore::new(0),
        entered: Notify::new(),
        held: AtomicUsize::new(0),
    });
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "classifier-export-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let export_path = root.join("run-index.db");
    drop(broca_store::SqliteStore::open(&export_path).unwrap());
    // Fix only the source identity and clock so the real producer's complete bytes, including
    // fact ids, can be compared with the downstream decoder's golden fixtures.
    rusqlite::Connection::open(&export_path)
        .unwrap()
        .execute(
            "UPDATE export_meta SET source_instance_id='system-one-golden-source'",
            [],
        )
        .unwrap();
    let export = Arc::new(broca_store::SqliteStore::open(&export_path).unwrap());
    let system_one = Arc::new(
        SystemOne::new(
            store.clone(),
            "test",
            Arc::new(JevOnly),
            vec![ModelPin::jev(provider.clone())],
            Config::default(),
            Arc::new(AtomicUsize::new(0)),
        )
        .with_export(export.clone(), Arc::new(table))
        .with_clock(Arc::new(|| 1792772000000)),
    );
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
        Some(system_one.clone()),
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
        store,
        system_one,
        export,
        serve,
    }
}

impl Rig {
    /// Bind `channel` declaring `roles`; return the bind's answer frame.
    async fn bind(&mut self, channel: u16, roles: &[(&str, &str)]) -> Frame {
        self.bind_with_scope(channel, roles, None, Some(run_stamp("run")))
            .await
    }

    async fn bind_with_scope(
        &mut self,
        channel: u16,
        roles: &[(&str, &str)],
        principal: Option<subc_protocol::Principal>,
        scope: Option<subc_protocol::scope::ScopeStamp>,
    ) -> Frame {
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
            principal,
            consumer_capabilities: None,
            admission_facts: None,
            scope,
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

fn run_stamp(run: &str) -> subc_protocol::scope::ScopeStamp {
    serde_json::from_value(json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "ref": run, "scope_epoch": 1, "kind": "head", "owner_authorized": true,
        "attributes": {"agent_id": "agent", "run_id": run}
    }))
    .unwrap()
}

#[tokio::test]
async fn unscoped_classifier_call_writes_and_sends_nothing() {
    let mut rig = start().await;
    rig.provider.gate.add_permits(1);
    assert_eq!(
        rig.bind_with_scope(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS], None, None)
            .await
            .header
            .ty,
        FrameType::Response
    );
    let mut params = classify_params("unscoped");
    params["scope"] = json!({"agent_id": "forged", "run_id": "forged"});
    rig.request(9, 1, "classify.run", params).await;
    assert_eq!(code(&rig.reply().await), "scope_required");
    assert_eq!(rig.store.ledger("classifier", "unscoped").unwrap(), None);
    assert!(rig
        .store
        .outcomes("classifier", "unscoped")
        .unwrap()
        .is_empty());
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 0);
    assert!(rig
        .export
        .read_export_page(None, 100)
        .unwrap()
        .facts
        .is_empty());
    assert_eq!(rig.system_one.health()["batches_failed"], 1);
    rig.finish().await;
}

#[tokio::test]
async fn scoped_classifier_exports_one_measurement_per_paid_item_and_none_on_resend() {
    let mut rig = start().await;
    rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS]).await;
    rig.provider.gate.add_permits(2);
    let mut params = classify_params("paid-run");
    params["items"] = json!([{"state": "a"}, {"state": "b"}]);
    for corr in [1, 2] {
        rig.request(9, corr, "classify.run", params.clone()).await;
        assert_eq!(rig.reply().await.header.ty, FrameType::Response);
        let page = rig.export.read_export_page(None, 100).unwrap();
        assert_eq!(page.facts.len(), 2);
        assert_eq!(
            serde_json::to_string(&page.facts[0].segment).unwrap(),
            include_str!("../tests/golden/system_one_run_fact.json").trim()
        );
        for (index, fact) in page.facts.iter().enumerate() {
            let broca_core::system_one_fact::UsageExportSegment::SystemOne(fact) = &fact.segment
            else {
                panic!("not a System One fact")
            };
            assert_eq!(fact.item_index, index as u64);
            assert_eq!(fact.run_id.as_deref(), Some("run"));
            assert_eq!(fact.flow_id, None);
            assert_eq!(
                fact.auth_selection.as_deref(),
                Some("apikey:typesafe:primary")
            );
            assert_eq!(fact.account_ref.as_deref(), Some("primary"));
            assert_eq!(fact.usage.input_tokens, Some(100));
            assert_eq!(fact.usage.output_tokens, None);
            assert_eq!(fact.charge_basis, broca_core::ChargeBasis::MeteredApi);
            assert_eq!(fact.model, JEV_1_13_0);
        }
    }
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 2);
    assert_eq!(
        rig.system_one.health()["spend_nanos_by_model"][JEV_1_13_0],
        8400
    );
    rig.finish().await;
}

#[tokio::test]
async fn flow_classifier_measurement_never_inherits_its_owner_agent_or_run() {
    let mut rig = start().await;
    let stamp = serde_json::from_value(json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "ref": "owners-run", "scope_epoch": 1, "kind": "head", "owner_authorized": true,
        "attributes": {"flow_id": "flow-1", "agent_id": "agent", "run_id": "owners-run"}
    }))
    .unwrap();
    let basal = Some(subc_protocol::Principal::Reserved {
        module_id: "basal".into(),
    });
    rig.bind_with_scope(
        9,
        &[broca_subc::CLASSIFIER_ROLE_VERSIONS],
        basal,
        Some(stamp),
    )
    .await;
    rig.provider.gate.add_permits(1);
    rig.request(9, 1, "classify.run", classify_params("paid-flow"))
        .await;
    assert_eq!(rig.reply().await.header.ty, FrameType::Response);
    let page = rig.export.read_export_page(None, 100).unwrap();
    assert_eq!(page.facts.len(), 1);
    let value = serde_json::to_value(&page.facts[0].segment).unwrap();
    assert_eq!(
        serde_json::to_string(&page.facts[0].segment).unwrap(),
        include_str!("../tests/golden/system_one_flow_fact.json").trim()
    );
    assert_eq!(value["flow_id"], "flow-1");
    for absent in [
        "run_id",
        "agent_id",
        "session",
        "resolved_credential_id",
        "cost_nanos",
        "input_rate_nanos",
        "output_rate_nanos",
        "cost_basis",
    ] {
        assert!(value.get(absent).is_none(), "{absent}: {value}");
    }
    rig.finish().await;
}

#[tokio::test]
async fn basal_classifier_requires_a_flow_even_with_a_run_stamp() {
    let mut rig = start().await;
    rig.provider.gate.add_permits(2);
    for (channel, scope) in [(9, Some(run_stamp("run"))), (10, None)] {
        let principal = Some(subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        });
        assert_eq!(
            rig.bind_with_scope(
                channel,
                &[broca_subc::CLASSIFIER_ROLE_VERSIONS],
                principal,
                scope
            )
            .await
            .header
            .ty,
            FrameType::Response
        );
        rig.request(channel, 1, "classify.run", classify_params("basal"))
            .await;
        assert_eq!(code(&rig.reply().await), "flow_scope_required");
        assert_eq!(rig.store.ledger("classifier", "basal").unwrap(), None);
    }
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[tokio::test]
async fn classifier_retry_cannot_move_its_original_scope() {
    let mut rig = start().await;
    rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS]).await;
    rig.provider.gate.add_permits(1);
    rig.request(9, 1, "classify.run", classify_params("frozen"))
        .await;
    assert_eq!(rig.reply().await.header.ty, FrameType::Response);
    rig.bind_with_scope(
        10,
        &[broca_subc::CLASSIFIER_ROLE_VERSIONS],
        None,
        Some(run_stamp("other")),
    )
    .await;
    rig.request(10, 2, "classify.run", classify_params("frozen"))
        .await;
    assert_eq!(code(&rig.reply().await), "batch_scope_mismatch");
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 1);
    rig.request(9, 3, "classify.run", classify_params("frozen"))
        .await;
    assert_eq!(rig.reply().await.header.ty, FrameType::Response);
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 1);
    assert_eq!(rig.system_one.health()["items_answered"], 1);
    rig.finish().await;
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

#[tokio::test]
async fn startup_replays_committed_measurement_intents_before_serving() {
    use broca_store::system_one::{
        BatchClaim, BatchScope, Claimed, FactContext, Outcome, PaidRecord, StoredOutcome,
    };
    let rig = start().await;
    let scope = BatchScope::Run {
        agent_id: "agent".into(),
        run_id: "run".into(),
    };
    assert_eq!(
        rig.store
            .claim(&BatchClaim {
                role: "classifier",
                batch_id: "crash",
                model: JEV_1_13_0,
                identity: "{}",
                body_digest: "digest",
                frozen_row: "{}",
                scope: &scope,
                ceiling_nanos: None,
                owner: "dead-process",
                now_ms: 1,
            })
            .unwrap(),
        Claimed::Fresh
    );
    let context = FactContext {
        source_instance_id: rig.export.source_instance_id().into(),
        provider: "typesafe".into(),
        model: JEV_1_13_0.into(),
        auth_selection: Some("apikey:typesafe:primary".into()),
        account_ref: Some("primary".into()),
        charge_basis: broca_core::ChargeBasis::MeteredApi,
        charge_basis_mapping_version: 5,
        charge_basis_overrides: "none".into(),
    };
    rig.store
        .record_paid(
            "classifier",
            "crash",
            "dead-process",
            &StoredOutcome {
                index: 0,
                outcome: Outcome::Answered("{}".into()),
                input_tokens: Some(100),
                output_tokens: None,
            },
            &PaidRecord {
                spend_nanos: 4200,
                now_ms: 2,
                fact: Some(&context),
            },
        )
        .unwrap();
    assert!(rig
        .export
        .read_export_page(None, 100)
        .unwrap()
        .facts
        .is_empty());
    let restarted = SystemOne::new(
        rig.store.clone(),
        "new-process",
        Arc::new(JevOnly),
        vec![ModelPin::jev(rig.provider.clone())],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    )
    .with_export(
        rig.export.clone(),
        Arc::new(crate::billing_routing::ChargeBasisTable::shipped_defaults()),
    );
    assert_eq!(restarted.release_abandoned().unwrap(), 1);
    assert_eq!(
        rig.export.read_export_page(None, 100).unwrap().facts.len(),
        1
    );
    assert!(rig.store.pending_facts().unwrap().is_empty());
    assert_eq!(restarted.health()["spend_nanos_by_model"][JEV_1_13_0], 4200);
    restarted.release_abandoned().unwrap();
    assert_eq!(
        rig.export.read_export_page(None, 100).unwrap().facts.len(),
        1
    );
    assert_eq!(rig.provider.held.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[tokio::test]
async fn classifier_fact_resolves_charge_basis_from_its_configured_table() {
    let mut rig = start_with_basis(crate::billing_routing::ChargeBasisTable::empty()).await;
    rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS]).await;
    rig.provider.gate.add_permits(1);
    rig.request(9, 1, "classify.run", classify_params("basis"))
        .await;
    assert_eq!(rig.reply().await.header.ty, FrameType::Response);
    let page = rig.export.read_export_page(None, 100).unwrap();
    assert_eq!(page.facts.len(), 1);
    let value = serde_json::to_value(&page.facts[0].segment).unwrap();
    assert_eq!(value["charge_basis"], "unknown");
    rig.finish().await;
}
