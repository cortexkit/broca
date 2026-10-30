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
    // held counts every provider entry; waiting falls when a call finishes or is cancelled.
    waiting: AtomicUsize,
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
        struct WaitingCall<'a>(&'a AtomicUsize);
        impl Drop for WaitingCall<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let _waiting = WaitingCall(&self.waiting);
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
            model_name: None,
            provider_name: None,
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
        waiting: AtomicUsize::new(0),
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
        self.bind_in_project(channel, roles, principal, scope, "/tmp/project-a")
            .await
    }

    async fn bind_in_project(
        &mut self,
        channel: u16,
        roles: &[(&str, &str)],
        principal: Option<subc_protocol::Principal>,
        scope: Option<subc_protocol::scope::ScopeStamp>,
        project_root: &str,
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
            identity: SubcBindIdentity::new(project_root, "runner", "classifier-test"),
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
        let provider = self.provider.clone();
        // File-backed measurement exports can delay classifier replies on slow
        // disks. wait_for guards against a deadlocked read without making disk
        // latency part of the route contract.
        broca_test_support::wait_for(
            "classifier route reply",
            read_frame(&mut self.daemon),
            move || {
                format!(
                    "provider calls entered={}",
                    provider.held.load(Ordering::SeqCst)
                )
            },
        )
        .await
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

/// Compare a fact the real producer exported with its golden file, which the downstream
/// decoder pins. `BROCA_UPDATE_GOLDEN=1` rewrites the file from these bytes first.
fn assert_fact_golden(actual: &str, file: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(file);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{actual}\n")).unwrap();
    }
    let pinned = std::fs::read_to_string(&path).unwrap();
    assert_eq!(actual, pinned.trim_end(), "{file}");
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
        assert_fact_golden(
            &serde_json::to_string(&page.facts[0].segment).unwrap(),
            "system_one_run_fact.json",
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
            assert_eq!(fact.provider, "typesafe");
            assert_eq!(fact.model, "jev-1.13.0");
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
    assert_fact_golden(
        &serde_json::to_string(&page.facts[0].segment).unwrap(),
        "system_one_flow_fact.json",
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
    let mut rig = broca_test_support::wait_for("classifier rig startup", start(), || {
        "awaiting the serve hello; no requests sent or handlers reached".into()
    })
    .await;
    let provider = rig.provider.clone();
    let handler = rig.handler.clone();
    let phase = std::cell::Cell::new("binding the session route");
    let sent = std::cell::Cell::new(0);
    let answered = std::cell::RefCell::new(std::collections::BTreeSet::new());
    let classified = std::cell::RefCell::new(std::collections::BTreeSet::new());
    broca_test_support::wait_for(
        "held classifier calls and independent session work",
        async {
            let project_root = broca_test_support::scratch_root()
                .join(format!("classifier-session-{}", std::process::id()));
            std::fs::create_dir_all(&project_root).unwrap();
            // session.send refuses a missing project root before invoking its handler.
            assert_eq!(
                rig.bind_in_project(
                    7,
                    &[],
                    None,
                    Some(run_stamp("run")),
                    project_root.to_str().unwrap(),
                )
                .await
                .header
                .ty,
                FrameType::Response
            );
            phase.set("binding the classifier route");
            assert_eq!(
                rig.bind(9, &[broca_subc::CLASSIFIER_ROLE_VERSIONS])
                    .await
                    .header
                    .ty,
                FrameType::Response
            );
            // Send more held batches than the connection has request slots. Classifier calls
            // must not occupy those slots and prevent the session requests below from running.
            let batches = MAX_INFLIGHT_ROUTE_REQUESTS as u64 + 4;
            phase.set("sending classifier requests");
            for n in 0..batches {
                rig.request(
                    9,
                    100 + n,
                    "classify.run",
                    classify_params(&format!("held-{n}")),
                )
                .await;
                sent.set(n + 1);
            }
            // A refusal can answer before the provider is called. Observe that reply as well as
            // entry, otherwise an admission regression leaves the test waiting for an impossible event.
            phase.set("waiting for provider entry");
            loop {
                let notified = provider.entered.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if provider.held.load(Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    reply = read_frame(&mut rig.daemon) => {
                        panic!("classify.run answered before its provider was held: {reply:?}");
                    }
                }
            }
            phase.set("sending session.read corr=1");
            rig.request(7, 1, "session.read", json!({})).await;
            phase.set("sending session.send corr=2");
            rig.request(
                7,
                2,
                "session.send",
                json!({"prompt": "next", "send_id": "turn-1",
                       "model": {"provider": "fake", "model": "fake"}}),
            )
            .await;
            phase.set("waiting for session replies corr={1, 2}");
            while answered.borrow().len() < 2 {
                let frame = read_frame(&mut rig.daemon).await.unwrap().unwrap();
                assert!(
                    frame.header.corr < 100,
                    "a classify.run answered while its provider was held: {frame:?}"
                );
                answered.borrow_mut().insert(frame.header.corr);
            }
            assert_eq!(rig.handler.reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                rig.handler.sends.load(Ordering::SeqCst),
                1,
                "session.send corr=2 must reach its handler while classifier calls are held"
            );
            assert!(rig.provider.held.load(Ordering::SeqCst) >= 1);
            rig.provider.gate.add_permits(batches as usize);
            phase.set("waiting for classifier replies after releasing the provider");
            while classified.borrow().len() < batches as usize {
                let frame = read_frame(&mut rig.daemon).await.unwrap().unwrap();
                assert_eq!(frame.header.ty, FrameType::Response, "{frame:?}");
                classified.borrow_mut().insert(frame.header.corr);
            }
            phase.set("finishing the connection");
            rig.finish().await;
        },
        || {
            format!(
                "phase={}; classifier requests sent={}; session corr ids answered={:?} (expected {{1, 2}}); classifier corr ids answered={:?} (expected 100..{}); provider calls held={}; provider calls entered={}; session.read handler reached={}; session.send handler reached={}",
                phase.get(), sent.get(), answered.borrow(), classified.borrow(),
                100 + MAX_INFLIGHT_ROUTE_REQUESTS as u64 + 4,
                provider.waiting.load(Ordering::SeqCst),
                provider.held.load(Ordering::SeqCst),
                handler.reads.load(Ordering::SeqCst), handler.sends.load(Ordering::SeqCst),
            )
        },
    )
    .await;
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
        model: "jev-1.13.0".into(),
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
    // An in-memory store cannot prove an owner is dead, so it keeps the claim. Dead-owner release is
    // covered by a_batch_a_dead_process_held_is_released_at_startup_and_resumed and the real
    // subprocess test owner_recovery_never_steals_a_live_process_claim.
    assert_eq!(restarted.release_abandoned().unwrap(), 0);
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

/// A Workers AI provider that answers at once, as Cloudflare does for Clef.
struct ClefAnswers;

#[async_trait::async_trait]
impl ClassifierProvider for ClefAnswers {
    fn egress_host(&self) -> String {
        "stand-in.invalid".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _body: String) -> ProviderReply {
        ProviderReply::AttributedAnswer {
            body: br#"{"answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#
                .to_vec(),
            auth_selection: Some("apikey:cloudflare-workers-ai:primary".into()),
            account_ref: Some("primary".into()),
        }
    }
}

struct ClefOnly;

impl BatchCatalog for ClefOnly {
    fn row(&self, catalog_id: &str) -> Option<broca_catalog::ModelBatchRow> {
        (catalog_id == crate::system_one::CLEF).then(|| broca_catalog::ModelBatchRow {
            model_name: None,
            provider_name: None,
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(240_000_000),
            output_rate_nanos: Some(0),
        })
    }
}

/// Clef's model id carries slashes of its own, so the catalog key splits only at its first
/// slash: the fact names provider `cloudflare-workers-ai` and model `@cf/cloudflare/clef`.
#[tokio::test]
async fn a_clef_fact_names_the_provider_and_its_slashed_model_id_apart() {
    let root = broca_test_support::scratch_root()
        .join(format!("classifier-export-clef-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let export = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let system_one = Arc::new(
        SystemOne::new(
            Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
            "test",
            Arc::new(ClefOnly),
            vec![ModelPin::clef(Arc::new(ClefAnswers))],
            Config::default(),
            Arc::new(AtomicUsize::new(0)),
        )
        .with_export(
            export.clone(),
            Arc::new(crate::billing_routing::ChargeBasisTable::shipped_defaults()),
        ),
    );
    let mut params = classify_params("clef");
    params["model"] = json!("cloudflare-workers-ai/@cf/cloudflare/clef");
    system_one
        .clone()
        .spawn_call(
            params.to_string(),
            broca_store::system_one::BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "run".into(),
            },
        )
        .await
        .unwrap();
    let page = export.read_export_page(None, 100).unwrap();
    assert_eq!(page.facts.len(), 1);
    let broca_core::system_one_fact::UsageExportSegment::SystemOne(fact) = &page.facts[0].segment
    else {
        panic!("not a System One fact")
    };
    assert_eq!(fact.provider, "cloudflare-workers-ai");
    assert_eq!(fact.model, "@cf/cloudflare/clef");
    assert_eq!(
        system_one.health()["spend_nanos_by_model"]["cloudflare-workers-ai/@cf/cloudflare/clef"],
        24_000,
        "health still keys spend by the whole catalog key"
    );
}

/// A routed key with no provider/model split could not be attributed in a usage fact, so the
/// call is refused before its provider is called or anything is stored.
#[tokio::test]
async fn a_routed_key_without_a_provider_split_is_refused_before_any_call() {
    struct AnyRow;
    impl BatchCatalog for AnyRow {
        fn row(&self, _catalog_id: &str) -> Option<broca_catalog::ModelBatchRow> {
            ClefOnly.row(crate::system_one::CLEF)
        }
    }
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let mut pin = ModelPin::clef(Arc::new(ClefAnswers));
    pin.catalog_id = "clef".into();
    let system_one = Arc::new(SystemOne::new(
        store.clone(),
        "test",
        Arc::new(AnyRow),
        vec![pin],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    ));
    let mut params = classify_params("bare");
    params["model"] = json!("clef");
    let refused = system_one
        .clone()
        .spawn_call(
            params.to_string(),
            broca_store::system_one::BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "run".into(),
            },
        )
        .await
        .unwrap_err();
    let refused: Value = serde_json::from_slice(&refused).unwrap();
    assert_eq!(refused["code"], "model_unavailable", "{refused}");
    assert_eq!(store.ledger(system_one.store_role(), "bare").unwrap(), None);
}
