//! Fetch-plan admission through the real serve path and core loop over a real file WAL.
//!
//! A session admitted without a plan (every session before plans existed) must keep exactly the
//! bytes it had: its `RunStarted`, the reply to the send that admitted it, and its
//! `session.baseline` reply.
//!
//! A session whose first send carries a plan is fetched once per provider (through an in-test
//! admitter that answers what the plan composed and counts every call), frozen in its
//! `RunStarted`, and rendered from that record afterwards. The plans are the vendored
//! fetch-plan vectors.

use crate::delivery_rig;

use delivery_rig::*;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_fetch_plan::{DescribeAnswer, FetchError, Fetcher, Plan, PlanAdmitter, Refusal};
use broca_wal::WalRecord;
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::CatalogRequest;
use serde_json::{json, Value};
use subc_protocol::FrameType;

/// The `RunStarted` a plan-less first send writes, as record JSON, with the parts that differ
/// per run (the scratch project root, the run id and the timestamp) replaced by placeholders. Every other byte is pinned.
const PLAN_LESS_RUN_STARTED: &str = r#"{"type":"run_started","written_at_ms":0,"run_id":"@RUN_ID@","session":{"project_root":"@ROOT@","harness":"runner","session":"delivery"},"ts_ms":0,"config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[{"name":"work","description":"does work","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"render_config":"stateless"},"computed_tags":{"checkpoint":0}},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"},"send_id":"turn-1","request_identity_digest":"{\"version\":1,\"model\":\"158ff0524c84046f8e4277fe15cb3c7c9b77118fc5b296d09e0dea85c0fa281f\",\"input\":\"4047097b2648d6b400be8a670f467110f9366f7b79e3b450d3eb138e36acdab4\",\"tools\":\"0c1c3c61240370c46bbf8b8cf618a7ac639f1f2c70481bf04a1497d7069e0594\",\"policy_allowed\":\"22f199ad3c03aa6cdc52d4e7e38efd8ae9d46f28bfcdf960d1d4a16de4f4bb30\",\"tool_choice\":\"d7522ccd58caaccd080ff42fc701046086ea73d690df7b1b597b620e0aa33284\",\"generation\":\"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a\",\"cache\":\"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b\",\"budget\":\"74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b\",\"stop_when\":\"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945\"}","marker_order_version":1}"#;

/// The `session.baseline` reply of a session admitted without a plan, running the rig's one
/// tool: no items, no composition digest, both generations 0.
const PLAN_LESS_BASELINE_REPLY: &str = r#"{"result":{"state":"ready","baseline":{"items":[],"tool_names":["work"],"accepted_generation":0,"applied_generation":0}}}"#;

/// `record` as JSON with the per-run parts replaced, so the rest can be compared byte for byte.
fn normalized_run_started(record: &WalRecord, run_id: &str, root: &str) -> String {
    let mut normalized = record.clone();
    if let WalRecord::RunStarted {
        ts_ms,
        written_at_ms,
        ..
    } = &mut normalized
    {
        *ts_ms = Some(0);
        *written_at_ms = Some(0);
    }
    // The root as it appears inside a JSON string (backslashes escaped on Windows).
    let escaped_root = serde_json::to_string(root).unwrap();
    let escaped_root = &escaped_root[1..escaped_root.len() - 1];
    serde_json::to_string(&normalized)
        .unwrap()
        .replace(run_id, "@RUN_ID@")
        .replace(escaped_root, "@ROOT@")
}

/// A plan-less session keeps its bytes: the `RunStarted` its first send writes, the reply to that
/// send (with the plan-less baseline), and the `session.baseline` reply.
#[tokio::test]
async fn a_plan_less_session_keeps_its_bytes() {
    let mut rig = Rig::start("plan-less-pin").await;
    rig.script.push(&[Step::Text("answer")]);
    let active = rig.send("turn-1", "hi", None).await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    assert_golden_admitting(&active, "send_reply.active.json", &run_id);
    let records = rig.wait_for_finished(1).await;
    let started = records
        .iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .expect("the first send wrote a RunStarted");
    let root = rig.session.project_root.to_string_lossy().into_owned();
    assert_eq!(
        normalized_run_started(started, &run_id, &root),
        PLAN_LESS_RUN_STARTED
    );
    let WalRecord::RunStarted { manifest, .. } = started else {
        unreachable!()
    };
    assert!(manifest.is_none());
    // Plan-less episodes freeze computed tags and their newly recorded input time; both
    // facts require an understanding reader even without a plan manifest.
    assert_eq!(
        started.required_features(),
        [
            broca_wal::MESSAGE_TIMES_FEATURE,
            broca_wal::RUNNER_TAGS_V2_FEATURE
        ]
    );

    let baseline = rig
        .request(
            7,
            serde_json::json!({"method": "session.baseline", "params": {}}),
        )
        .await;
    assert_eq!(
        String::from_utf8_lossy(&baseline.body),
        PLAN_LESS_BASELINE_REPLY
    );
    rig.finish().await;
}

/// Load a pinned fetch-plan/v1 plan from broca-fetch-plan's test fixtures. These tests
/// normally use delivery_rig::Rig without a compaction plane, so remove the compaction item
/// and set system-text model selectors to its scripted fake/fake model. Model-mismatch
/// refusals are tested separately rather than through this fixture adaptation.
fn vector(relative: &str) -> Value {
    let mut plan = raw_vector(relative);
    plan.as_object_mut().unwrap().remove("compaction_item");
    for item in plan["system_text_items"]
        .as_array_mut()
        .into_iter()
        .flatten()
    {
        if item["params"].get("model").is_some() {
            item["params"]["model"] = json!({"provider": "fake", "id": "fake"});
        }
    }
    plan
}

/// A vendored fetch-plan vector file, exactly as vendored.
fn raw_vector(relative: &str) -> Value {
    let path = relative.split('/').fold(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("broca-fetch-plan")
            .join("tests")
            .join("fixtures")
            .join("fetch-plan-v1"),
        |path, part| path.join(part),
    );
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// The text a provider answers for its system-text item in these tests.
fn text_of(provider: &str) -> String {
    format!("{provider} guidance")
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Admits plans by answering, for each provider, exactly what the plan composed for it, and
/// records every call it is asked to make. `refuse` makes one provider's catalog fetch refuse
/// with a code.
#[derive(Default)]
pub(super) struct CountingAdmitter {
    calls: Mutex<Vec<String>>,
    refuse: Mutex<Option<(String, String)>>,
    /// A composition digest every provider answers instead of the plan's, so the answers
    /// disagree with the plan and admission refuses it `plan_stale`.
    stale_digest: Mutex<Option<String>>,
    reply: Mutex<Option<Value>>,
    reply_schema: Mutex<Option<Value>>,
    /// Every provider's catalog answer declares late results and a `content/v1` result format
    /// for each of its tools, as AFT's and Core's do in production.
    declare: Mutex<bool>,
    /// A catalog `reference_slots` member every answered tool carries, when set.
    reference_slots: Mutex<Option<Value>>,
    /// Whether the admitter accepts plan items opting into output references.
    output_refs: Mutex<bool>,
}

impl CountingAdmitter {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// Make every answered catalog tool declare `slots` as its `reference_slots`.
    pub(super) fn answer_reference_slots(&self, slots: Value) {
        *self.reference_slots.lock().unwrap() = Some(slots);
    }

    /// Accept plan items that opt into output references, as a runner serving the group does.
    pub(super) fn admit_output_refs(&self) {
        *self.output_refs.lock().unwrap() = true;
    }
}

struct Answering<'a> {
    plan: &'a Plan,
    admitter: &'a CountingAdmitter,
    session: String,
}

#[async_trait]
impl Fetcher for Answering<'_> {
    async fn catalog(&self, provider: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        self.admitter
            .calls
            .lock()
            .unwrap()
            .push(format!("{}:catalog:{provider}", self.session));
        if let Some((refused, code)) = self.admitter.refuse.lock().unwrap().clone() {
            if refused == provider {
                return Err(FetchError::Refused {
                    code,
                    detail: None,
                    retryable: false,
                });
            }
        }
        let digest = self
            .admitter
            .stale_digest
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| self.plan.composition_digest.clone());
        let tools: Vec<Value> = self
            .plan
            .composed
            .get(provider)
            .into_iter()
            .flatten()
            .map(|tool| {
                let schema = self.admitter.reply_schema.lock().unwrap().clone()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
                let mut answer = json!({
                    "name": tool.name,
                    "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                    "semantics": 1,
                    "capabilities": tool.capabilities,
                    "description": format!("{provider} {}", tool.name),
                    "input_schema": schema,
                });
                if let Some(reply) = self.admitter.reply.lock().unwrap().clone() {
                    answer["reply"] = reply;
                }
                if *self.admitter.declare.lock().unwrap() {
                    answer["result_format"] = json!("content/v1");
                }
                if let Some(slots) = self.admitter.reference_slots.lock().unwrap().clone() {
                    answer["reference_slots"] = slots;
                }
                answer
            })
            .collect();
        let mut answer = json!({
            "generation": "1",
            "catalog_digest": sha256_hex(provider.as_bytes()),
            "composition_digest": digest,
            "tools": tools,
        });
        if *self.admitter.declare.lock().unwrap() {
            answer["capabilities"] = json!({"late_results": true});
        }
        if request.system_text.is_some() {
            let text = text_of(provider);
            answer["system_text"] = json!({
                "text": text,
                "item_digest": sha256_hex(text.as_bytes()),
                "preflight_digest": "preflight",
                "composition_digest": digest,
            });
        }
        Ok(answer)
    }

    async fn declare(
        &self,
        provider: &str,
        _request: &DeclareRequest,
    ) -> Result<Value, FetchError> {
        self.admitter
            .calls
            .lock()
            .unwrap()
            .push(format!("{}:declare:{provider}", self.session));
        let step = self
            .plan
            .step_items
            .iter()
            .find(|step| step.item.provider == provider)
            .unwrap();
        Ok(json!({"subscriptions": step.subscriptions}))
    }

    /// Every compaction provider these tests name answers with the synchronous op set, which the
    /// published role no longer accepts, so a plan that reaches this describe is refused. The
    /// real-daemon tests below ask a real provider instead.
    async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
        self.admitter
            .calls
            .lock()
            .unwrap()
            .push(format!("{}:describe:{provider}", self.session));
        Ok(DescribeAnswer::Answered(
            json!({"majors":[{"version":"compaction-provider/v1",
            "ops":["role.describe","compaction.setup","compaction.step"]}],
            "implementation_version":"0.0.0-test"}),
        ))
    }
}

#[async_trait]
impl PlanAdmitter for CountingAdmitter {
    async fn admit(
        &self,
        plan: &Plan,
        session: &broca_protocol::BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, Refusal> {
        let fetcher = Answering {
            plan,
            admitter: self,
            session: session.session.clone(),
        };
        broca_fetch_plan::admit(
            plan,
            &fetcher,
            session,
            broca_fetch_plan::ADMISSION_DEADLINE,
        )
        .await
    }

    fn admits_output_refs(&self) -> bool {
        *self.output_refs.lock().unwrap()
    }
}

pub(super) async fn plan_rig(tag: &str) -> (Rig, Arc<CountingAdmitter>) {
    let rig = Rig::start(tag).await;
    let admitter = Arc::new(CountingAdmitter::default());
    rig.registry().set_plan_admitter(admitter.clone());
    (rig, admitter)
}

/// A rig whose channel 7 carries a daemon-attested scope, as Core's routes do for workers: a
/// plan naming the worker tool preset is refused on an unscoped route.
async fn worker_plan_rig(tag: &str) -> (Rig, Arc<CountingAdmitter>) {
    let mut rig = Rig::start_with_scopes(tag).await;
    rig.bind_with_scope(7, Some(scope_owner()), Some(scope_stamp(false)))
        .await;
    let admitter = Arc::new(CountingAdmitter::default());
    rig.registry().set_plan_admitter(admitter.clone());
    (rig, admitter)
}

/// An unscoped route with a worker-preset plan is refused before any fetch or WAL byte, and the
/// same plan is admitted once the route carries a scope.
#[tokio::test]
async fn a_worker_preset_plan_requires_a_scoped_route() {
    let plan = vector("plans/broca-worker.json");
    let mut rig = Rig::start_with_scopes("worker-unscoped").await;
    let admitter = Arc::new(CountingAdmitter::default());
    rig.registry().set_plan_admitter(admitter.clone());
    let frame = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let body = error_body(&frame);
    assert_eq!(body["code"], "scope_required", "{body}");
    assert_eq!(
        body["detail"],
        json!({"field": "plan.tool_items", "preset": "worker"})
    );
    assert!(
        rig.wal_bytes().is_empty(),
        "refusal must precede the first WAL byte"
    );
    assert!(
        admitter.calls().is_empty(),
        "refusal must precede provider fetches"
    );
    rig.finish().await;

    let (mut rig, admitter) = worker_plan_rig("worker-scoped").await;
    rig.script.push(&[Step::Text("worker")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    assert!(
        !admitter.calls().is_empty(),
        "a scoped worker plan is admitted"
    );
    rig.finish().await;
}

#[tokio::test]
async fn absent_system_text_preset_refuses_before_fetch_or_wal_write() {
    let (mut rig, admitter) = plan_rig("no-default-system-text").await;
    let mut plan = vector("plans/broca-head-no-compaction.json");
    plan["system_text_items"][0]
        .as_object_mut()
        .unwrap()
        .remove("preset");
    let frame = send_plan(
        &mut rig,
        7,
        "missing-text-preset",
        "hi",
        Some(&plan),
        json!({}),
    )
    .await;
    assert_eq!(error_body(&frame)["code"], "invalid_params");
    assert_eq!(
        error_body(&frame)["detail"],
        json!({"field":"plan.system_text_items[0].preset"})
    );
    assert!(admitter.calls().is_empty());
    assert!(rig.wal_bytes().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn invalid_reply_refuses_before_any_wal_write() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../broca-fetch-plan/tests/fixtures/tool-provider-v1/reply-deadline.json"
    ))
    .unwrap();
    let (mut rig, admitter) = worker_plan_rig("invalid-reply").await;
    let plan = vector("plans/broca-worker.json");
    let parsed =
        broca_fetch_plan::parse_plan(&plan, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap();
    let tool_name = &parsed.composed["aft"][0].name;
    for group in ["invalid", "malformed"] {
        for case in vectors[group].as_array().unwrap() {
            *admitter.reply.lock().unwrap() = Some(case["reply"].clone());
            *admitter.reply_schema.lock().unwrap() = Some(
                case.get("input_schema")
                    .unwrap_or(&vectors["input_schema"])
                    .clone(),
            );
            let refused = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
            let body = error_body(&refused);
            assert_eq!(body["code"], "fetch_unavailable", "{}", case["name"]);
            assert_eq!(body["detail"]["provider_code"], "invalid_answer");
            assert_eq!(body["detail"]["tool"], *tool_name);
            let member = body["detail"]["member"].as_str().unwrap();
            if let Some(expected) = case["member"].as_str() {
                assert!(member.ends_with(expected), "{}: {member}", case["name"]);
            }
            assert!(
                rig.wal_bytes().is_empty(),
                "{} wrote WAL bytes",
                case["name"]
            );
        }
    }
    rig.finish().await;
}

fn scope_stamp(flow: bool) -> subc_protocol::scope::ScopeStamp {
    use subc_protocol::scope::{ScopeAttributes, ScopeKind, ScopeStamp};
    ScopeStamp::new(scope_owner(), "head-1", 1, ScopeKind::Head)
        .with_attributes(ScopeAttributes::new().with_flow_id(flow.then(|| "flow-1".into())))
        .with_owner_authorized(true)
}

fn scope_owner() -> subc_protocol::Principal {
    subc_protocol::Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

async fn scoped_plan_rig(tag: &str, flow: bool) -> (Rig, Arc<CountingAdmitter>) {
    let mut rig = Rig::start_with_scopes(tag).await;
    rig.bind_with_scope(8, Some(scope_owner()), Some(scope_stamp(flow)))
        .await;
    let admitter = Arc::new(CountingAdmitter::default());
    rig.registry().set_plan_admitter(admitter.clone());
    (rig, admitter)
}

fn legacy_tools() -> Value {
    json!([{"name": "work", "description": "does work", "input_schema": {"type": "object"}}])
}

/// Admission must refuse the shape before writing even the first WAL frame, not leave a run
/// whose every tool call lacks the preset required by scoped providers.
#[tokio::test]
async fn scoped_legacy_tools_require_a_plan_before_any_fetch_or_wal_write() {
    let (mut rig, admitter) = scoped_plan_rig("scoped-tools-refused", false).await;
    let before = rig.wal_bytes();
    assert!(before.is_empty());
    let reply = send_plan(
        &mut rig,
        8,
        "turn-1",
        "hi",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    assert_eq!(error_body(&reply)["code"], "scoped_tools_require_plan");
    assert_golden(&reply, "scoped_tools_require_plan.json", "");
    assert_eq!(rig.wal_bytes(), before);
    assert!(admitter.calls().is_empty());
    assert!(rig.script.prompts.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn scoped_plan_and_toolless_sends_still_admit() {
    let (mut rig, admitter) = scoped_plan_rig("scoped-plan-allowed", false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    let reply = send_plan(&mut rig, 8, "turn-1", "hi", Some(&plan), json!({})).await;
    result(&reply);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(admitter.calls().len(), 3);
    assert!(matches!(
        run_started(&records)[0],
        WalRecord::RunStarted {
            scope: Some(_),
            manifest: Some(_),
            ..
        }
    ));
    // A later send uses the frozen plan without carrying the plan again.
    let reply = send_plan(&mut rig, 8, "turn-2", "hi", None, json!({})).await;
    result(&reply);
    rig.wait_for_finished(2).await;
    assert_eq!(admitter.calls().len(), 3);
    rig.finish().await;

    for (tag, extra) in [
        ("absent", json!({})),
        ("empty", json!({"tools": []})),
        ("null", json!({"tools": null})),
    ] {
        let (mut rig, admitter) = scoped_plan_rig(&format!("scoped-tools-{tag}"), false).await;
        let reply = send_plan(&mut rig, 8, "turn-1", "hi", None, extra).await;
        result(&reply);
        let records = rig.wait_for_finished(1).await;
        assert!(matches!(
            run_started(&records)[0],
            WalRecord::RunStarted {
                scope: Some(_),
                manifest: None,
                ..
            }
        ));
        assert!(admitter.calls().is_empty());
        rig.finish().await;
    }
}

#[tokio::test]
async fn scoped_toolless_steers_interrupts_and_queued_sends_still_admit() {
    for delivery in ["steer", "interrupt", "queue"] {
        let (mut rig, admitter) =
            scoped_plan_rig(&format!("scoped-{delivery}-allowed"), false).await;
        rig.script.hold_model(true);
        let first = send_plan(&mut rig, 8, "turn-1", "hi", None, json!({})).await;
        let first_id = result(&first)["run_id"].as_str().unwrap().to_owned();
        rig.wait_entered().await;
        let reply = send_plan(
            &mut rig,
            8,
            "turn-2",
            "next",
            None,
            json!({"delivery": delivery}),
        )
        .await;
        if delivery == "steer" {
            assert_eq!(result(&reply)["run_id"], first_id);
            assert!(rig
                .records()
                .iter()
                .any(|record| matches!(record, WalRecord::Steered { .. })));
        } else {
            assert_eq!(result(&reply)["state"], "pending");
            assert!(rig
                .records()
                .iter()
                .any(|record| matches!(record, WalRecord::Queued { .. })));
        }
        assert!(admitter.calls().is_empty());
        rig.script.hold_model(false);
        rig.wait_for_finished(if delivery == "steer" { 1 } else { 2 })
            .await;
        rig.finish().await;
    }
}

/// A scoped session created without a plan cannot add tools, or a plan, on a later send.
/// When several refusals apply, the owner and plan refusals are returned before this one.
#[tokio::test]
async fn later_scoped_planless_tool_declarations_refuse_without_writing() {
    let (mut rig, admitter) = scoped_plan_rig("scoped-later-tools-refused", false).await;
    let first = send_plan(&mut rig, 8, "turn-1", "hi", None, json!({})).await;
    result(&first);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    for delivery in ["queue", "steer", "interrupt"] {
        let reply = send_plan(
            &mut rig,
            8,
            delivery,
            "tools",
            None,
            json!({"delivery": delivery, "tools": legacy_tools()}),
        )
        .await;
        assert_eq!(error_body(&reply)["code"], "scoped_tools_require_plan");
        assert_eq!(rig.wal_bytes(), before);
    }
    let plan = vector("plans/broca-head-no-compaction.json");
    let reply = send_plan(&mut rig, 8, "plan", "hi", Some(&plan), json!({})).await;
    assert_eq!(error_body(&reply)["code"], "plan_drift");
    // A send on a route with no scope stamp must still get the owner refusal, not the
    // tools-without-plan refusal.
    let reply = send_plan(
        &mut rig,
        7,
        "unstamped",
        "hi",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    assert_eq!(error_body(&reply)["code"], "scope_owner_mismatch");
    assert_eq!(rig.wal_bytes(), before);
    assert!(admitter.calls().is_empty());
    rig.finish().await;
}

/// A session that adopted a scope can still hold an older admitted run with a legacy tool
/// list. Resending that run's `send_id` returns the run's recorded answer; its tool list is
/// not checked again.
#[tokio::test]
async fn an_admitted_legacy_tool_run_keeps_its_keyed_answer_after_scope_adoption() {
    let mut rig = Rig::start_with_scopes("scoped-tools-dedup").await;
    rig.bind_as(9, Some(scope_owner())).await;
    let first = send_plan(
        &mut rig,
        9,
        "turn-1",
        "hi",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    let run_id = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.bind_with_scope(8, Some(scope_owner()), Some(scope_stamp(false)))
        .await;
    let adopted = send_plan(&mut rig, 8, "turn-2", "adopt", None, json!({"tools": []})).await;
    result(&adopted);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    let retry = send_plan(
        &mut rig,
        8,
        "turn-1",
        "hi",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    let retry_result = result(&retry);
    assert_eq!(retry_result["state"], "finished");
    assert_eq!(retry_result["run_id"], run_id);
    assert_eq!(retry_result["reason"], "completed");
    assert_eq!(retry_result["baseline"]["tool_names"], json!(["work"]));
    let changed = send_plan(
        &mut rig,
        8,
        "turn-1",
        "changed",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    assert_eq!(error_body(&changed)["code"], "send_id_reuse");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[tokio::test]
async fn flow_sends_keep_their_own_tool_refusal() {
    let (mut rig, admitter) = scoped_plan_rig("flow-tools-refused", true).await;
    let reply = send_plan(
        &mut rig,
        8,
        "turn-1",
        "hi",
        None,
        json!({"tools": legacy_tools()}),
    )
    .await;
    let body = error_body(&reply);
    assert_eq!(body["code"], "invalid_params");
    assert_eq!(body["detail"]["field"], "tools");
    assert!(rig.wal_bytes().is_empty());
    let accepted = send_plan(&mut rig, 8, "turn-2", "hi", None, json!({})).await;
    result(&accepted);
    rig.wait_for_finished(1).await;
    assert!(admitter.calls().is_empty());
    rig.finish().await;
}

/// Send `prompt` under `send_id` on `channel` with `plan` and `extra` members.
async fn send_plan(
    rig: &mut Rig,
    channel: u16,
    send_id: &str,
    prompt: &str,
    plan: Option<&Value>,
    extra: Value,
) -> subc_protocol::Frame {
    let mut params = json!({
        "prompt": prompt,
        "send_id": send_id,
        "model": {"provider": "fake", "model": "fake"},
    });
    if let Some(plan) = plan {
        params["plan"] = plan.clone();
    }
    for (key, value) in extra.as_object().unwrap() {
        params[key] = value.clone();
    }
    rig.request(channel, json!({"method": "session.send", "params": params}))
        .await
}

fn error_body(frame: &subc_protocol::Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

fn run_started(records: &[WalRecord]) -> Vec<&WalRecord> {
    records
        .iter()
        .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
        .collect()
}

fn manifest_of(record: &WalRecord) -> &broca_wal::PlanManifest {
    match record {
        WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => manifest,
        other => panic!("no manifest on {other:?}"),
    }
}

const HEAD_TOOLS: [&str; 9] = [
    "bash",
    "bash_status",
    "edit",
    "read",
    "ctx_memory",
    "ctx_note",
    "ctx_search",
    "browser_use",
    "computer_use",
];

const WORKER_TOOLS: [&str; 8] = [
    "bash",
    "edit",
    "read",
    "ask",
    "browser_use",
    "computer_use",
    "knowhow",
    "sidekick",
];

/// On the first send with a plan, each provider is fetched once and its returned tools and joined
/// system text are saved in the `RunStarted` record. The reply and `session.baseline` use the
/// baseline from that record. A later send without a plan reuses the saved tools and manifest,
/// without fetching providers or repeating the system text.
#[tokio::test]
async fn a_plan_is_fetched_once_frozen_and_rendered_from_the_record() {
    let (mut rig, admitter) = plan_rig("plan-frozen").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    let reply = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let first = result(&reply);
    let records = rig.wait_for_finished(1).await;
    // A reader-only release must not introduce transform metadata into production writes.
    for record in &records {
        let value = serde_json::to_value(record).unwrap();
        assert!(
            value.get("hooks").is_none(),
            "reader release wrote hooks: {value}"
        );
        if let Some(manifest) = value.get("manifest") {
            assert!(
                manifest.get("declarations").is_none(),
                "reader release wrote declarations: {value}"
            );
        }
    }
    let mut calls = admitter.calls();
    calls.sort();
    assert_eq!(
        calls,
        [
            "delivery:catalog:aft",
            "delivery:catalog:magic-context",
            "delivery:catalog:prefrontal-core",
        ],
        "one catalog fetch per provider"
    );
    let started = run_started(&records);
    assert_eq!(started.len(), 1);
    let manifest = manifest_of(started[0]);
    assert_eq!(
        manifest.plan_identity,
        broca_fetch_plan::plan_identity(&plan).unwrap()
    );
    let joined = ["prefrontal-core", "aft", "magic-context"]
        .map(text_of)
        .join("\n\n");
    assert_eq!(manifest.system_text, joined);

    // The model saw the frozen tools in plan order and the joined text as the system message.
    let prompts = rig.script.prompts.lock().unwrap().clone();
    let offered = rig.script.offered_tools.lock().unwrap().clone();
    assert_eq!(offered[0], HEAD_TOOLS.map(String::from).to_vec());
    assert_eq!(prompts[0][0].role, broca_protocol::content::Role::System);
    assert_eq!(
        prompts[0][0].content,
        vec![broca_protocol::content::ContentBlock::Text {
            text: joined.clone()
        }]
    );

    // The reply's baseline and `session.baseline` say the same, from the record.
    let baseline = &first["baseline"];
    assert_eq!(
        baseline["composition_digest"],
        manifest.composition_digest.as_str()
    );
    assert_eq!(baseline["tool_names"], json!(HEAD_TOOLS));
    assert_eq!(
        baseline["items"],
        json!(["aft", "magic-context", "prefrontal-core"]
            .map(|p| json!({"provider": p, "item_digest": sha256_hex(p.as_bytes())})))
    );
    assert!(baseline.get("absent_items").is_none());
    let read = rig
        .request(7, json!({"method": "session.baseline", "params": {}}))
        .await;
    assert_eq!(
        result(&read),
        json!({"state": "ready", "baseline": baseline})
    );

    // A later send without the plan fetches nothing and renders the same tools; only the first
    // episode carries the system text. (A resend of the plan itself is covered below.)
    let fetched = admitter.calls().len();
    send_plan(&mut rig, 7, "turn-2", "next", None, json!({})).await;
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        admitter.calls().len(),
        fetched,
        "a later send fetches nothing"
    );
    let started = run_started(&records);
    assert_eq!(started.len(), 2);
    for record in &started {
        assert_eq!(
            manifest_of(record),
            manifest,
            "every episode repeats the manifest"
        );
    }
    let offered = rig.script.offered_tools.lock().unwrap().clone();
    let prompts = rig.script.prompts.lock().unwrap().clone();
    for (tools, prompt) in offered.iter().zip(&prompts) {
        assert_eq!(tools, &HEAD_TOOLS.map(String::from).to_vec());
        let systems = prompt
            .iter()
            .filter(|m| m.role == broca_protocol::content::Role::System)
            .count();
        assert_eq!(
            systems, 1,
            "the system text renders once, from the first episode"
        );
    }
    rig.finish().await;
}

/// The worker plan resolves and forwards every tool named in its vector, including sidekick.
#[tokio::test]
async fn a_worker_plan_forwards_its_vector_tool_list() {
    let (mut rig, admitter) = worker_plan_rig("worker-plan-forwarded").await;
    let plan = vector("plans/broca-worker.json");
    rig.script.push(&[Step::Text("worker")]);
    let reply = send_plan(&mut rig, 7, "worker-turn", "hi", Some(&plan), json!({})).await;
    let first = result(&reply);
    let records = rig.wait_for_finished(1).await;

    let mut calls = admitter.calls();
    calls.sort();
    assert_eq!(
        calls,
        ["delivery:catalog:aft", "delivery:catalog:prefrontal-core",],
        "the worker plan fetches its two catalog providers once"
    );
    let started = run_started(&records);
    assert_eq!(started.len(), 1);
    let manifest = manifest_of(started[0]);
    let manifest_tools: Vec<&str> = manifest
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(manifest_tools, WORKER_TOOLS);
    assert!(manifest_tools.contains(&"sidekick"));
    let offered = rig.script.offered_tools.lock().unwrap().clone();
    assert_eq!(offered[0], WORKER_TOOLS.map(String::from).to_vec());
    assert_eq!(first["baseline"]["tool_names"], json!(WORKER_TOOLS));
    assert_eq!(
        manifest.plan_identity,
        broca_fetch_plan::plan_identity(&plan).unwrap()
    );
    rig.finish().await;
}

/// What a later episode of a planned session must look like: its `RunStarted` repeats the first
/// episode's manifest, offers exactly the manifest's tools, and its input holds no second system
/// message; the model is offered the manifest's tools on every step and sees the system text
/// once.
fn assert_later_episode_renders_the_frozen_manifest(rig: &Rig, records: &[WalRecord], label: &str) {
    let started = run_started(records);
    assert_eq!(started.len(), 2, "{label}: a second episode started");
    let frozen = manifest_of(started[0]);
    let WalRecord::RunStarted {
        manifest: Some(manifest),
        config,
        input,
        ..
    } = started[1]
    else {
        panic!("{label}: the later episode carries no manifest");
    };
    assert_eq!(&**manifest, frozen, "{label}: the same frozen manifest");
    assert_eq!(
        config.tools.as_ref(),
        frozen.tools.as_ref(),
        "{label}: the manifest's tools"
    );
    assert!(
        input
            .iter()
            .all(|message| message.role != broca_protocol::content::Role::System),
        "{label}: no second system message"
    );
    let offered = rig.script.offered_tools.lock().unwrap().clone();
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert!(
        offered.len() >= 2,
        "{label}: both episodes called the model"
    );
    for (tools, prompt) in offered.iter().zip(&prompts) {
        assert_eq!(tools, &HEAD_TOOLS.map(String::from).to_vec(), "{label}");
        let systems = prompt
            .iter()
            .filter(|m| m.role == broca_protocol::content::Role::System)
            .count();
        assert_eq!(systems, 1, "{label}: the system text renders once");
    }
}

/// A plain send while a planned session's first episode is running queues behind it, and when
/// that episode ends it drains as the next episode: same frozen manifest, the manifest's tools,
/// no second system message, and nothing fetched again.
#[tokio::test]
async fn a_queued_send_on_a_planned_session_drains_with_the_frozen_manifest() {
    let (mut rig, admitter) = plan_rig("plan-queue").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    rig.script.hold_model(true);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_entered().await;
    let fetched = admitter.calls().len();
    let queued = send_plan(&mut rig, 7, "turn-2", "next", None, json!({})).await;
    assert!(
        result(&queued)["submission_id"].is_string(),
        "the send queued behind the running episode"
    );
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(
        admitter.calls().len(),
        fetched,
        "the drained send fetches nothing"
    );
    assert_later_episode_renders_the_frozen_manifest(&rig, &records, "queued");
    rig.finish().await;
}

/// An `interrupt` into a planned session's running first episode cancels it and starts its own
/// episode: same frozen manifest, the manifest's tools, no second system message, and nothing
/// fetched again.
#[tokio::test]
async fn an_interrupt_on_a_planned_session_starts_with_the_frozen_manifest() {
    let (mut rig, admitter) = plan_rig("plan-interrupt").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    rig.script.hold_model(true);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_entered().await;
    let fetched = admitter.calls().len();
    result(
        &send_plan(
            &mut rig,
            7,
            "stop-1",
            "do this instead",
            None,
            json!({"delivery": "interrupt"}),
        )
        .await,
    );
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    let endings: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunFinished { reason, .. } => Some(*reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        endings,
        [
            broca_protocol::caller::RunFinishReason::Cancelled,
            broca_protocol::caller::RunFinishReason::Completed
        ]
    );
    assert_eq!(
        admitter.calls().len(),
        fetched,
        "the interrupt fetches nothing"
    );
    assert_later_episode_renders_the_frozen_manifest(&rig, &records, "interrupt");
    rig.finish().await;
}

/// A first send whose run paused for re-authentication right after admission is resent, plan
/// and all, under the same `send_id`, as the caller does once the credential is fixed. The
/// resend fetches nothing and re-drives the same episode from the frozen record: it is
/// answered `active` for that run with the same baseline, and writes no second `RunStarted`
/// (re-driving the session's first episode rebuilds exactly the input its `RunStarted` holds,
/// system text included, or the resend would be refused as a different request).
#[tokio::test]
async fn a_resent_paused_first_send_fetches_nothing_and_redrives_its_episode() {
    let (mut rig, admitter) = plan_rig("plan-resume").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    *rig.script.pause_next_run.lock().unwrap() = true;
    let paused = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let first = result(&paused);
    let run_1 = first["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let fetched = admitter.calls().len();

    let resent = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let again = result(&resent);
    assert_eq!(again["run_id"], run_1.as_str());
    assert_eq!(again["baseline"], first["baseline"]);
    rig.wait_idle().await;
    assert_eq!(
        admitter.calls().len(),
        fetched,
        "the resend fetches nothing"
    );
    let records = rig.records();
    let started = run_started(&records);
    assert_eq!(started.len(), 1, "the resend re-drives the same episode");
    assert!(
        manifest_of(started[0]).plan_identity == broca_fetch_plan::plan_identity(&plan).unwrap()
    );
    rig.finish().await;
}

/// The first `RunStarted` carrying a manifest is a gated frame requiring `plan-manifest/v1`, and
/// the WAL raised its feature floor to that name before writing it, so placement never lets a
/// build that cannot read the frame run on this machine. Before it, the floor names nothing.
#[tokio::test]
async fn the_first_planned_episode_raises_the_feature_floor() {
    let (mut rig, _admitter) = plan_rig("plan-floor").await;
    let floor_path = rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME);
    assert!(broca_wal::read_floor(&floor_path).unwrap().is_empty());
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script.push(&[Step::Text("first")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    let records = rig.wait_for_finished(1).await;
    let started = run_started(&records);
    assert_eq!(
        started[0].required_features(),
        [
            broca_wal::MESSAGE_TIMES_FEATURE,
            broca_wal::PLAN_MANIFEST_FEATURE,
            broca_wal::RUNNER_TAGS_V2_FEATURE
        ]
    );
    let floor = broca_wal::read_floor(&floor_path).unwrap();
    assert!(
        floor.contains(broca_wal::PLAN_MANIFEST_FEATURE),
        "{floor:?}"
    );
    rig.finish().await;
}

/// `session.baseline` answers only the session's owner: a caller under another identity is
/// refused `scope_owner_mismatch`.
#[tokio::test]
async fn session_baseline_refuses_a_non_owner() {
    let (mut rig, _admitter) = plan_rig("plan-owner").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.bind_as(
        8,
        Some(subc_protocol::Principal::Reserved {
            module_id: "prefrontal-core".into(),
        }),
    )
    .await;
    rig.script.push(&[Step::Text("first")]);
    let admitted = result(&send_plan(&mut rig, 8, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    let owner = rig
        .request(8, json!({"method": "session.baseline", "params": {}}))
        .await;
    assert_eq!(
        result(&owner),
        json!({"state": "ready", "baseline": admitted["baseline"]})
    );
    rig.bind_as(
        9,
        Some(subc_protocol::Principal::Reserved {
            module_id: "someone-else".into(),
        }),
    )
    .await;
    let refused = rig
        .request(9, json!({"method": "session.baseline", "params": {}}))
        .await;
    assert_eq!(error_body(&refused)["code"], "scope_owner_mismatch");
    rig.finish().await;
}

/// A hook-bearing plan is refused before providers are fetched or the WAL is written.
#[tokio::test]
async fn text_only_and_hook_only_plan_refuses_before_fetch_or_write() {
    let (mut rig, admitter) = plan_rig("plan-text-only-hook-refusal").await;
    let plan = vector("plans/text-only-and-hook-only.json");
    let before = rig.wal_bytes();
    let frame = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    assert_eq!(
        error_body(&frame)["detail"],
        json!({"field": "plan.composition.compaction"})
    );
    assert_eq!(rig.wal_bytes(), before);
    assert!(admitter.calls().is_empty());
    rig.finish().await;
}

/// A second session with the same plan fetches again: nothing fetched for one session is
/// reused for another.
#[tokio::test]
async fn a_second_session_with_the_same_plan_fetches_again() {
    let (mut rig, admitter) = plan_rig("plan-no-cache").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    rig.bind_session(11, "other").await;
    result(&send_plan(&mut rig, 11, "turn-1", "hi", Some(&plan), json!({})).await);
    let per_session = |session: &str| {
        admitter
            .calls()
            .iter()
            .filter(|call| call.starts_with(&format!("{session}:")))
            .count()
    };
    assert_eq!(per_session("delivery"), 3);
    assert_eq!(per_session("other"), 3);
    rig.finish().await;
}

/// Sets up how the admitter answers before one refused send.
type Arrange = Box<dyn Fn(&CountingAdmitter)>;

/// Every admission refusal writes nothing: the session's WAL is exactly as long as before. A
/// refused first send leaves no WAL at all, and a later send with another plan (or with a
/// plan on a session admitted without one) is refused `plan_drift` naming both identities.
#[tokio::test]
async fn every_admission_refusal_writes_nothing() {
    let (mut rig, admitter) = plan_rig("plan-refusals").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    let refusals: Vec<(&str, Value, Value, Arrange)> = vec![
        (
            "tools beside the plan",
            plan.clone(),
            json!({"tools": []}),
            Box::new(|_| {}),
        ),
        (
            "system beside the plan",
            plan.clone(),
            json!({"system": "legacy"}),
            Box::new(|_| {}),
        ),
        (
            "a required provider refuses",
            plan.clone(),
            json!({}),
            Box::new(|a| {
                *a.refuse.lock().unwrap() = Some(("aft".into(), "catalog_refused".into()))
            }),
        ),
        (
            "a stale composition",
            plan.clone(),
            json!({}),
            Box::new(|a| {
                *a.refuse.lock().unwrap() = None;
                *a.stale_digest.lock().unwrap() = Some("0".repeat(64));
            }),
        ),
        (
            "a compaction item, which broca does not take",
            {
                let mut plan = vector("plans/broca-head.json");
                plan["compaction_item"] =
                    raw_vector("plans/broca-head.json")["compaction_item"].clone();
                plan
            },
            json!({}),
            Box::new(|_| {}),
        ),
        (
            "a tool-name collision",
            {
                let mut plan = vector("plans/broca-head-plexus-no-exclude.json");
                plan["step_transform_items"] = json!([]);
                plan["composition"]["step_transform_items"] = json!([]);
                plan["composition_digest"] = json!(
                    cortexkit_role_tool_provider::catalog::composition_digest(&plan["composition"])
                        .unwrap()
                );
                plan
            },
            json!({}),
            Box::new(|a| *a.stale_digest.lock().unwrap() = None),
        ),
    ];
    let head_digest =
        broca_fetch_plan::parse_plan(&plan, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap()
            .composition_digest;
    let stale: Vec<Value> = ["aft", "magic-context", "prefrontal-core"]
        .iter()
        .map(|provider| {
            json!({
                "kind": "composition_digest",
                "provider": provider,
                "expected": head_digest,
                "got": "0".repeat(64),
            })
        })
        .collect();
    let expected = [
        json!({"field": "tools"}),
        json!({"field": "system"}),
        json!({"provider": "aft", "reason": "refused", "provider_code": "catalog_refused"}),
        json!({ "differences": stale }),
        json!({"field": "plan.compaction_item"}),
        json!({"name": "web_search", "providers": ["plexus", "prefrontal-core"]}),
    ];
    let codes = [
        "invalid_params",
        "invalid_params",
        "fetch_unavailable",
        "plan_stale",
        "invalid_params",
        "tool_name_collision",
    ];
    for (((label, plan, extra, arrange), detail), code) in
        refusals.into_iter().zip(expected).zip(codes)
    {
        arrange(&admitter);
        let refused = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), extra).await;
        let body = error_body(&refused);
        assert_eq!(body["code"], code, "{label}");
        assert_eq!(body["detail"], detail, "{label}");
        assert!(
            rig.wal_bytes().is_empty(),
            "{label}: a refused first send writes nothing"
        );
    }

    // Admitted; then a later send whose plan reorders the system texts (a different plan
    // identity) is refused `plan_drift`, and the WAL's bytes are unchanged.
    rig.script.push(&[Step::Text("first")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    let mut reordered = plan.clone();
    reordered["tool_items"][0]["preset"] = json!("alternate");
    let drift = send_plan(&mut rig, 7, "turn-2", "next", Some(&reordered), json!({})).await;
    let body = error_body(&drift);
    assert_eq!(body["code"], "plan_drift");
    assert_eq!(
        body["detail"],
        json!({
            "frozen": broca_fetch_plan::plan_identity(&plan).unwrap(),
            "sent": broca_fetch_plan::plan_identity(&reordered).unwrap(),
        })
    );
    // Reusing the first send's `send_id` with that other plan is refused `plan_drift` too.
    let reused = send_plan(&mut rig, 7, "turn-1", "hi", Some(&reordered), json!({})).await;
    assert_eq!(error_body(&reused)["code"], "plan_drift");
    assert_eq!(
        rig.wal_bytes(),
        before,
        "a refused later send writes nothing"
    );
    rig.finish().await;

    // A plan on a session first admitted without one: `plan_drift`, with no `frozen` identity
    // because the session froze none.
    let (mut rig, _admitter) = plan_rig("plan-after-plan-less").await;
    rig.script.push(&[Step::Text("first")]);
    result(&rig.send("turn-1", "hi", None).await);
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    let drift = send_plan(&mut rig, 7, "turn-2", "next", Some(&plan), json!({})).await;
    assert_eq!(
        error_body(&drift)["detail"],
        json!({"sent": broca_fetch_plan::plan_identity(&plan).unwrap()})
    );
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

fn model_plan(model: Value) -> Value {
    let mut plan = vector("plans/broca-worker.json");
    plan["system_text_items"][0]["params"]["model"] = model;
    plan
}

async fn assert_model_refused(tag: &str, model: Value, send_model: Value) {
    let (mut rig, admitter) = worker_plan_rig(tag).await;
    let plan = model_plan(model.clone());
    let before = rig.wal_bytes();
    let frame = send_plan(
        &mut rig,
        7,
        "turn-1",
        "hi",
        Some(&plan),
        json!({"model": send_model}),
    )
    .await;
    let body = error_body(&frame);
    assert_eq!(body["code"], "plan_model_mismatch");
    let mut expected_send = json!({"provider": send_model["provider"], "id": send_model["model"]});
    if let Some(variant) = send_model.get("variant") {
        expected_send["variant"] = variant.clone();
    }
    assert_eq!(
        body["detail"],
        json!({"provider": "prefrontal-core", "plan_model": model, "send_model": expected_send})
    );
    assert_eq!(rig.wal_bytes(), before, "refusal must preserve WAL bytes");
    assert!(
        admitter.calls().is_empty(),
        "refusal must precede every provider fetch"
    );
    if tag == "plan-model-id" {
        let golden = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../broca-wire/tests/golden/plan_model_mismatch.json");
        assert_eq!(frame.body, std::fs::read(golden).unwrap());
    }
    rig.finish().await;
}

#[tokio::test]
async fn plan_model_different_id_refuses_before_fetch_or_write() {
    assert_model_refused(
        "plan-model-id",
        json!({"provider": "fake", "id": "other"}),
        json!({"provider": "fake", "model": "fake"}),
    )
    .await;
}

#[tokio::test]
async fn plan_model_different_provider_refuses_before_fetch_or_write() {
    assert_model_refused(
        "plan-model-provider",
        json!({"provider": "other", "id": "fake"}),
        json!({"provider": "fake", "model": "fake"}),
    )
    .await;
}

#[tokio::test]
async fn plan_model_variant_mismatch_refuses_before_fetch_or_write() {
    for (tag, send) in [
        (
            "plan-model-variant-low",
            json!({"provider": "fake", "model": "fake", "variant": "low"}),
        ),
        (
            "plan-model-variant-absent",
            json!({"provider": "fake", "model": "fake"}),
        ),
    ] {
        assert_model_refused(
            tag,
            json!({"provider": "fake", "id": "fake", "variant": "high"}),
            send,
        )
        .await;
    }
}

#[tokio::test]
async fn plan_model_without_variant_accepts_any_send_variant() {
    for variant in [None, Some("low"), Some("high")] {
        // The tag becomes a directory name, so it holds no quotes or parentheses, which Windows
        // refuses in a path.
        let tag = format!("plan-model-match-{}", variant.unwrap_or("none"));
        let (mut rig, admitter) = worker_plan_rig(&tag).await;
        let plan = model_plan(json!({"provider": "fake", "id": "fake"}));
        let mut send = json!({"provider": "fake", "model": "fake"});
        if let Some(variant) = variant {
            send["variant"] = json!(variant);
        }
        rig.script.push(&[Step::Text("answer")]);
        result(
            &send_plan(
                &mut rig,
                7,
                "turn-1",
                "hi",
                Some(&plan),
                json!({"model": send}),
            )
            .await,
        );
        rig.wait_for_finished(1).await;
        assert!(!admitter.calls().is_empty());
        rig.finish().await;
    }
}

#[tokio::test]
async fn plan_model_absent_accepts_unrelated_send_model() {
    let (mut rig, admitter) = worker_plan_rig("plan-model-absent").await;
    let plan = vector("plans/broca-worker.json");
    rig.script.push(&[Step::Text("answer")]);
    result(
        &send_plan(
            &mut rig,
            7,
            "turn-1",
            "hi",
            Some(&plan),
            json!({"model": {"provider": "unrelated", "model": "different", "variant": "low"}}),
        )
        .await,
    );
    rig.wait_for_finished(1).await;
    assert!(!admitter.calls().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn plan_model_malformed_refuses_with_field_path() {
    for (model, suffix) in [
        (Value::Null, ""),
        (json!("fake"), ""),
        (json!({"id": "fake"}), ".provider"),
        (json!({"provider": 1, "id": "fake"}), ".provider"),
        (json!({"provider": "fake"}), ".id"),
        (json!({"provider": "fake", "id": 1}), ".id"),
    ] {
        let (mut rig, admitter) = worker_plan_rig("plan-model-malformed").await;
        let before = rig.wal_bytes();
        let plan = model_plan(model);
        let body =
            error_body(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
        assert_eq!(body["code"], "invalid_params");
        assert_eq!(
            body["detail"],
            json!({"field": format!("plan.system_text_items[0].params.model{suffix}")})
        );
        assert_eq!(rig.wal_bytes(), before);
        assert!(admitter.calls().is_empty());
        rig.finish().await;
    }
}

#[tokio::test]
async fn plan_model_later_send_mismatch_refuses_before_fetch_or_write() {
    let (mut rig, admitter) = worker_plan_rig("plan-model-later").await;
    let plan = model_plan(json!({"provider": "fake", "id": "fake"}));
    rig.script.push(&[Step::Text("first")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    let calls = admitter.calls();
    assert!(!before.is_empty());
    assert!(!calls.is_empty());
    let body = error_body(
        &send_plan(
            &mut rig,
            7,
            "turn-2",
            "next",
            Some(&plan),
            json!({"model": {"provider": "fake", "model": "other"}}),
        )
        .await,
    );
    assert_eq!(body["code"], "plan_model_mismatch");
    assert_eq!(
        body["detail"],
        json!({
            "provider": "prefrontal-core",
            "plan_model": {"provider": "fake", "id": "fake"},
            "send_model": {"provider": "fake", "id": "other"},
        })
    );
    assert_eq!(
        rig.wal_bytes(),
        before,
        "later refusal must preserve WAL bytes"
    );
    assert_eq!(admitter.calls(), calls, "later refusal must fetch nothing");
    rig.finish().await;
}

#[tokio::test]
async fn invalid_presets_and_composition_compaction_refuse_before_any_wal_write() {
    let mut rig = Rig::start("preset-format-refusals").await;
    let original = vector("plans/broca-head-no-compaction.json");
    for (list, index) in [("tool_items", 0), ("system_text_items", 0)] {
        let field = format!("plan.{list}[{index}].preset");
        for preset in [
            String::new(),
            "a".repeat(65),
            "Worker".into(),
            "a.b".into(),
            "a b".into(),
            "é".into(),
        ] {
            let mut plan = original.clone();
            plan[list][index]["preset"] = json!(preset);
            let frame = send_plan(&mut rig, 7, "bad-preset", "hi", Some(&plan), json!({})).await;
            error_body(&frame);
            let expected = format!(
                r#"{{"code":"invalid_params","message":"invalid plan or send: {{\"field\":\"{field}\"}}","detail":{{"field":"{field}"}}}}"#
            );
            assert_eq!(String::from_utf8(frame.body).unwrap(), expected);
            assert!(rig.wal_bytes().is_empty());
        }
    }
    for compaction in [Value::Null, json!({"provider": "magic-context"})] {
        let mut plan = original.clone();
        plan["composition"]["compaction"] = compaction;
        let frame = send_plan(&mut rig, 7, "bad-compaction", "hi", Some(&plan), json!({})).await;
        error_body(&frame);
        assert_eq!(
            String::from_utf8(frame.body).unwrap(),
            r#"{"code":"invalid_params","message":"invalid plan or send: {\"field\":\"plan.composition.compaction\"}","detail":{"field":"plan.composition.compaction"}}"#
        );
        assert!(rig.wal_bytes().is_empty());
    }
    rig.finish().await;
}

#[tokio::test]
async fn approve_on_unavailable_pass_refuses_before_any_fetch_or_wal_write() {
    let (mut rig, admitter) = plan_rig("step-transform-refusal").await;
    let mut plan = vector("plans/broca-head-no-compaction.json");
    let step_item = json!({
        "provider": "magic-context",
        "preset": "head",
        "params": {},
        "subscriptions": [{
            "hook": "pre_tool",
            "phase": "approve",
            "ops": [],
            "on_unavailable": "pass",
            "budget_ms": 1000
        }]
    });
    plan["step_transform_items"] = json!([step_item.clone()]);
    plan["composition"]["step_transform_items"] = json!([step_item]);
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    let before = rig.wal_bytes();
    let frame = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let body = error_body(&frame);
    assert_eq!(body["code"], "invalid_params");
    assert_eq!(
        body["detail"],
        json!({"field": "plan.step_transform_items", "item":0, "subscription":0, "problem":"approve_on_unavailable_pass"})
    );
    assert_eq!(
        String::from_utf8(frame.body.clone()).unwrap(),
        r#"{"code":"invalid_params","message":"invalid plan or send: {\"field\":\"plan.step_transform_items\",\"item\":0,\"problem\":\"approve_on_unavailable_pass\",\"subscription\":0}","detail":{"field":"plan.step_transform_items","item":0,"problem":"approve_on_unavailable_pass","subscription":0}}"#
    );
    assert_eq!(rig.wal_bytes(), before, "refusal must preserve WAL bytes");
    assert!(
        admitter.calls().is_empty(),
        "refusal must precede every provider fetch"
    );
    rig.finish().await;
}

struct AdmissionPassHook;
#[async_trait]
impl broca_core::HookPlane for AdmissionPassHook {
    async fn hook(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: cortexkit_role_step_transform_provider::hook::HookCall,
    ) -> Result<
        cortexkit_role_step_transform_provider::answer::HookAnswer,
        broca_core::HookUnavailable,
    > {
        Ok(cortexkit_role_step_transform_provider::answer::HookAnswer::Pass)
    }
}

#[tokio::test]
async fn approve_with_refuse_is_admitted_and_declaration_is_frozen() {
    let (mut rig, admitter) = scoped_plan_rig("approve-refuse-admission", false).await;
    rig.set_hook_plane(Arc::new(AdmissionPassHook));
    rig.script.push(&[Step::Text("done")]);
    let mut plan = vector("plans/broca-head-no-compaction.json");
    let subscription = json!({"hook":"pre_tool", "phase":"approve", "ops":[], "on_unavailable":"refuse", "budget_ms":1000});
    let item = json!({"provider":"magic-context", "preset":"head", "params":{}, "subscriptions":[subscription.clone()]});
    plan["step_transform_items"] = json!([item.clone()]);
    plan["composition"]["step_transform_items"] = json!([item]);
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    let frame = send_plan(&mut rig, 8, "turn-1", "hi", Some(&plan), json!({})).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let records = rig.wait_for_finished(1).await;
    let manifest = manifest_of(run_started(&records)[0]);
    assert_eq!(manifest.declarations[0].provider, "magic-context");
    assert_eq!(
        serde_json::from_str::<Value>(manifest.declarations[0].answer.as_json_text()).unwrap(),
        json!({"subscriptions":[subscription]})
    );
    assert!(admitter
        .calls()
        .iter()
        .any(|call| call == "delivery:declare:magic-context"));
    let before = rig.wal_bytes();
    let frame = send_plan(&mut rig, 7, "turn-2", "hi", None, json!({})).await;
    assert_eq!(error_body(&frame)["code"], "approve_requires_scope");
    assert_eq!(
        rig.wal_bytes(),
        before,
        "a frozen approval plan still requires scope"
    );
    rig.finish().await;
}

#[tokio::test]
async fn approve_requires_scope_refuses_before_any_fetch_or_wal_write() {
    let mut plan = vector("plans/broca-head-no-compaction.json");
    let item = json!({"provider":"magic-context", "preset":"head", "params":{},
        "subscriptions":[{"hook":"pre_tool", "phase":"approve", "ops":[],
            "on_unavailable":"refuse", "budget_ms":1000}]});
    plan["step_transform_items"] = json!([item.clone()]);
    plan["composition"]["step_transform_items"] = json!([item]);
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    for daemon_scopes in [false, true] {
        let mut rig = if daemon_scopes {
            Rig::start_with_scopes("approval-unscoped").await
        } else {
            Rig::start("approval-no-scopes").await
        };
        let admitter = Arc::new(CountingAdmitter::default());
        rig.registry().set_plan_admitter(admitter.clone());
        rig.set_hook_plane(Arc::new(AdmissionPassHook));
        let frame = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
        assert_eq!(error_body(&frame)["code"], "approve_requires_scope");
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/approve_requires_scope.json");
        if std::env::var_os("BROCA_UPDATE_APPROVAL_GOLDEN").is_some() {
            std::fs::write(&path, &frame.body).unwrap();
        }
        assert_eq!(frame.body, std::fs::read(path).unwrap());
        assert!(
            rig.wal_bytes().is_empty(),
            "refusal must precede the first WAL byte"
        );
        assert!(
            admitter.calls().is_empty(),
            "refusal must precede provider fetches"
        );
        rig.finish().await;
    }
}

struct AdmissionTransform {
    answer: Arc<Mutex<Value>>,
    requests: Arc<Mutex<Vec<Value>>>,
    binds: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for AdmissionTransform {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.binds
            .lock()
            .unwrap()
            .push(json!({"roles":request.role_versions, "scope":request.scope}));
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        self.requests.lock().unwrap().push(request.clone());
        let answer = match request["method"].as_str() {
            Some("role.describe") => {
                json!({"majors":[{"version":"step-transform-provider/v1", "ops":["role.describe","transform.declare","transform.hook"], "stability":"alpha"}], "implementation_version":"0.0.0-test", "capabilities":[]})
            }
            Some("transform.declare") => self.answer.lock().unwrap().clone(),
            _ => panic!(
                "admission must use a role declaration, never a tool envelope or hook: {request}"
            ),
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

/// The real serve path freezes a role-routed declaration, but does not run hooks at admission.
/// A declaration that no longer covers the plan refuses before the first WAL byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn step_transforms_admit_through_the_role_route_and_freeze_the_declaration() {
    use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
    let source = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    if !source.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
            "real ck-subc required: {source:?}"
        );
        println!("SKIP step_transforms_admit: {source:?} is missing");
        return;
    }
    let mut rig = Rig::start("step-transform-role-admission").await;
    let runtime = rig.dir.join("subc-runtime");
    let config = rig.dir.join("config/cortexkit/subc.jsonc");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(&config, r#"{"version":1,"modules":{}}"#).unwrap();
    let daemon_bin = broca_test_support::ckdev_binary(source, &rig.dir);
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&daemon_bin, &rig.dir, "daemon")?
            .env("XDG_CONFIG_HOME", rig.dir.join("config"))
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", rig.dir.join("data-home"))
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
    })
    .unwrap();
    let mut daemon = broca_test_support::RigChild::new(daemon, &rig.dir, "daemon");
    let connection_file = runtime.join("subc-connection.json");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !connection_file.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon never published connection file"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let answer = Arc::new(Mutex::new(json!({"subscriptions":[]})));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let binds = Arc::new(Mutex::new(Vec::new()));
    let (_, serving) = subc_client_rs::serve_with_handle(
        &connection_file,
        ModuleManifest::builder("admission-transform", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![],
                identity_scope: vec![],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build(),
        AdmissionTransform {
            answer: answer.clone(),
            requests: requests.clone(),
            binds: binds.clone(),
        },
    )
    .await
    .unwrap();
    let serving = tokio::spawn(async move {
        serving.await.unwrap();
    });
    let conn = Arc::new(
        broca_subc::SubcConnection::connect(&connection_file)
            .await
            .unwrap(),
    );
    rig.registry()
        .set_plan_admitter(Arc::new(broca_subc::SubcPlanAdmitter::new(conn.clone())));
    let subscription = json!({"hook":"pre_tool", "phase":"validate", "ops":[], "on_unavailable":"pass", "budget_ms":1000});
    let steps = json!([{"provider":"admission-transform", "preset":"worker", "params":{"nested":[1,null]}, "subscriptions":[subscription.clone()]}]);
    let composition = json!({"providers":[{"provider":"admission-transform", "tools":[]}], "step_transform_items":steps});
    let plan = json!({"tool_items":[], "system_text_items":[], "step_transform_items":steps,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(), "composition":composition});
    let before = rig.wal_bytes();
    assert!(before.is_empty());
    let frame = send_plan(&mut rig, 7, "no-plane", "hi", Some(&plan), json!({})).await;
    let refusal = error_body(&frame);
    assert_eq!(refusal["code"], "invalid_params");
    assert_eq!(
        refusal["detail"],
        json!({"field":"plan.step_transform_items"})
    );
    assert_eq!(
        rig.wal_bytes(),
        before,
        "hookless admission must allocate no lineage header"
    );
    assert!(
        !rig.wal_path.exists(),
        "hookless admission must not open a WAL"
    );
    assert!(
        requests.lock().unwrap().is_empty(),
        "hookless refusal must precede every fetch"
    );
    rig.set_hook_plane(Arc::new(broca_subc::SubcHookPlane::new(conn)));
    let frame = send_plan(&mut rig, 7, "stale", "hi", Some(&plan), json!({})).await;
    assert_eq!(error_body(&frame)["code"], "plan_stale");
    assert_eq!(rig.wal_bytes(), before);
    let declaration =
        json!({"subscriptions":[subscription], "provider_extra":{"keep":[null,false]}});
    *answer.lock().unwrap() = declaration.clone();
    rig.script.push(&[Step::Text("done")]);
    let frame = send_plan(&mut rig, 7, "admitted", "hi", Some(&plan), json!({})).await;
    result(&frame);
    let records = rig.wait_for_finished(1).await;
    let started = run_started(&records);
    let manifest = manifest_of(started[0]);
    assert!(manifest.fetches.is_empty());
    assert_eq!(manifest.declarations.len(), 1);
    assert_eq!(manifest.declarations[0].provider, "admission-transform");
    assert_eq!(
        manifest.declarations[0].answer.as_json_text(),
        declaration.to_string()
    );
    let requests = requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "transform.declare")
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r["method"] == "transform.hook")
            .count(),
        0
    );
    for request in requests
        .iter()
        .filter(|r| r["method"] == "transform.declare")
    {
        assert_eq!(
            request,
            &json!({"method":"transform.declare", "params":{"preset":"worker", "params":{"nested":[1,null]}, "composition":composition}})
        );
    }
    assert_eq!(binds.lock().unwrap().len(), 2);
    for bind in binds.lock().unwrap().iter() {
        assert_eq!(bind["roles"], json!({"step-transform-provider":"v1"}));
        assert!(bind["scope"].is_null());
    }
    rig.finish().await;
    serving.abort();
    daemon.kill().await.unwrap();
    daemon.wait().await.unwrap();
}

/// The engine is staged but not advertised yet: admission refuses a compaction item
/// before opening a WAL or invoking Setup. The writer release removes this guard and
/// parse_plan's refusal together when the complete route and model-view groups ship.
#[tokio::test]
async fn a_reader_without_an_installed_compaction_plane_refuses_before_writing() {
    struct CountingCompaction(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl broca_core::compaction::CompactionPlane for CountingCompaction {
        async fn setup(
            &self,
            _: &str,
            _: &broca_protocol::BindIdentity,
            _: broca_core::compaction_provider::setup::SetupRequest,
        ) -> Result<
            broca_core::compaction_provider::setup::SetupAnswer,
            broca_core::compaction::CompactionUnavailable,
        > {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(broca_core::compaction::CompactionUnavailable)
        }
    }
    let (mut rig, admitter) = plan_rig("compaction-dormancy").await;
    let plane = Arc::new(CountingCompaction(std::sync::atomic::AtomicUsize::new(0)));
    *rig.script.compaction_plane.lock().unwrap() = Some(plane.clone());
    let mut plan = vector("plans/broca-head.json");
    plan["compaction_item"] = json!({"provider":"magic-context","preset":"head","params":{}});
    plan["composition"]["compaction"] = json!({"provider":"magic-context"});
    // A valid transform would require declaration and an installed hook plane if admitted.
    // The unsupported compaction item must refuse first, before either path can do work.
    let steps = json!([{
        "provider":"magic-context", "preset":"head", "params":{},
        "subscriptions":[{
            "hook":"pre_tool", "phase":"validate", "ops":[],
            "on_unavailable":"pass", "budget_ms":1000
        }]
    }]);
    plan["step_transform_items"] = steps.clone();
    plan["composition"]["step_transform_items"] = steps;
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    let refused = send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    let body = error_body(&refused);
    assert_eq!(body["code"], "invalid_params");
    assert_eq!(body["detail"], json!({"field":"plan.compaction_item"}));
    assert!(
        admitter.calls().is_empty(),
        "compaction must refuse before any hook declaration or catalog fetch"
    );
    assert!(
        rig.wal_bytes().is_empty(),
        "dormant writer must produce zero WAL bytes"
    );
    assert_eq!(
        plane.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "dormant writer must make zero Setup calls"
    );
    assert!(rig.script.prompts.lock().unwrap().is_empty());
    rig.finish().await;
}

/// A caller need not resend its plan. Reader admission must consult the frozen manifest
/// before an actor could resume the run or queue an untransformed input.
#[tokio::test]
async fn reader_refuses_frozen_step_transform_session_without_plan() {
    use broca_wal::{FileWal, Wal};
    let mut rig = Rig::start("frozen-reader-refusal").await;
    let value: Value = serde_json::from_str(include_str!(
        "../../../broca-wal/tests/golden/step_transforms/run_started.json"
    ))
    .unwrap();
    let started: WalRecord = serde_json::from_value(value["record"].clone()).unwrap();
    let mut wal = FileWal::open(&rig.wal_path)
        .unwrap()
        .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
    wal.append(1, &started, true).unwrap();
    drop(wal);
    let before = rig.wal_bytes();
    let frame = send_plan(&mut rig, 7, "reader-send", "hi", None, json!({})).await;
    let refusal = error_body(&frame);
    assert_eq!(refusal["code"], "invalid_params");
    assert_eq!(
        refusal["detail"],
        json!({"field":"plan.step_transform_items"})
    );
    assert_eq!(rig.wal_bytes(), before);
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    rig.finish().await;
}

fn compaction_only_plan(provider: &str) -> Value {
    let composition = json!({"providers":[{"provider":provider,"tools":[]}],
        "compaction":{"provider":provider}, "step_transform_items":[]});
    json!({
        "tool_items":[], "system_text_items":[], "step_transform_items":[],
        "compaction_item":{"provider":provider, "preset":"head", "params":{}},
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition":composition,
    })
}

/// The real daemon for the compaction tests: SUBC_CORE_BIN, else the sibling subconscious
/// checkout's debug build, which is where CI builds it (as every other real-daemon rig does).
fn subc_core_bin() -> std::path::PathBuf {
    broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN")
}

async fn compaction_daemon(
    dir: &std::path::Path,
    modules: Value,
) -> (broca_test_support::RigChild, std::path::PathBuf) {
    let source = subc_core_bin();
    assert!(source.exists(), "real ck-subc required: {source:?}");
    let runtime = dir.join("compaction-runtime");
    let config = dir.join("compaction-config/cortexkit/subc.jsonc");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({"version":1,"modules":modules})).unwrap(),
    )
    .unwrap();
    let daemon_bin = broca_test_support::ckdev_binary(source, dir);
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&daemon_bin, dir, "daemon")?
            .env("XDG_CONFIG_HOME", dir.join("compaction-config"))
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", dir.join("compaction-data"))
            .env("MAGIC_CONTEXT_STORAGE_DIR", dir.join("mc-state"))
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
    })
    .unwrap();
    let daemon = broca_test_support::RigChild::new(daemon, dir, "daemon");
    let connection = runtime.join("subc-connection.json");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !connection.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon did not publish connection file"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    (daemon, connection)
}

/// The module a compaction-admission test names as the plan's compaction provider.
const ADMISSION_COMPACTION: &str = "admission-compaction";

/// What the scripted module answers `role.describe` with.
enum DescribeReply {
    Answer(Value),
    Refuse(&'static str),
}

/// A module that records every bind and request, answers `role.describe` as scripted, and
/// answers Setup and steps like a compaction provider that changes nothing.
struct AdmissionCompaction {
    describe: DescribeReply,
    requests: Arc<Mutex<Vec<Value>>>,
    binds: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for AdmissionCompaction {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.binds
            .lock()
            .unwrap()
            .push(json!({"roles": request.role_versions, "scope": request.scope}));
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        self.requests.lock().unwrap().push(request.clone());
        let request_id = request["params"]["request_id"].clone();
        let answer = match (request["method"].as_str(), &self.describe) {
            (Some("role.describe"), DescribeReply::Answer(describe)) => describe.clone(),
            (Some("role.describe"), DescribeReply::Refuse(code)) => {
                return subc_client_rs::HandlerOutcome::Error {
                    code: (*code).into(),
                    message: "scripted describe refusal".into(),
                }
            }
            (Some("compaction.setup"), _) => json!({"answer": "ready", "request_id": request_id,
                "initial": {"compaction_id": "admission", "version": 1,
                    "range": {"from": 0, "to": 0}, "replacement": []}}),
            (Some("compaction.step"), _) => json!({"answer": "noop", "request_id": request_id}),
            _ => panic!("not a compaction-provider request: {request}"),
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

/// A rig whose plans are admitted by broca's own subc admitter against a real daemon, with
/// [`ADMISSION_COMPACTION`] answering `role.describe` with `describe`. `None` when the daemon
/// build is missing and the run does not require it.
struct RealCompactionAdmission {
    rig: Rig,
    daemon: broca_test_support::RigChild,
    provider: tokio::task::JoinHandle<()>,
    requests: Arc<Mutex<Vec<Value>>>,
    binds: Arc<Mutex<Vec<Value>>>,
}

impl RealCompactionAdmission {
    async fn start(tag: &str, describe: DescribeReply) -> Option<Self> {
        use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
        if !subc_core_bin().exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "real daemon required: {:?} is missing",
                subc_core_bin()
            );
            println!("SKIP {tag}: {:?} is missing", subc_core_bin());
            return None;
        }
        let rig = Rig::start(tag).await;
        let (daemon, connection) = compaction_daemon(&rig.dir, json!({})).await;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let binds = Arc::new(Mutex::new(Vec::new()));
        let (_, serving) = subc_client_rs::serve_with_handle(
            &connection,
            ModuleManifest::builder(ADMISSION_COMPACTION, env!("CARGO_PKG_VERSION"))
                .provides(vec![ProviderRole::ToolProvider {
                    tools: vec![],
                    identity_scope: vec![],
                    concurrency: Concurrency::ModuleManaged,
                    emits_push: false,
                    sub_supervises: false,
                }])
                .build(),
            AdmissionCompaction {
                describe,
                requests: requests.clone(),
                binds: binds.clone(),
            },
        )
        .await
        .unwrap();
        let provider = tokio::spawn(async move {
            let _ = serving.await;
        });
        let conn = Arc::new(
            broca_subc::SubcConnection::connect(&connection)
                .await
                .unwrap(),
        );
        rig.registry()
            .set_plan_admitter(Arc::new(broca_subc::SubcPlanAdmitter::new(conn.clone())));
        rig.set_compaction_plane(Arc::new(broca_subc::SubcCompactionPlane::new(conn)));
        Some(Self {
            rig,
            daemon,
            provider,
            requests,
            binds,
        })
    }

    fn methods(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_owned())
            .collect()
    }

    /// Every route the module was bound on asked for the compaction role alone.
    fn assert_compaction_routes_only(&self) {
        let binds = self.binds.lock().unwrap();
        assert!(!binds.is_empty());
        for bind in binds.iter() {
            assert_eq!(bind["roles"], json!({"compaction-provider": "v1"}));
            assert!(bind["scope"].is_null());
        }
    }

    /// The refused send wrote nothing: no WAL byte, no lineage file, no Setup.
    fn assert_refused_before_writing(&self) {
        assert!(
            self.rig.wal_bytes().is_empty(),
            "a refused send wrote WAL bytes"
        );
        assert!(
            !self.rig.wal_path.exists(),
            "a refused send must not create the lineage's WAL file"
        );
        assert!(
            !self
                .methods()
                .iter()
                .any(|method| method != "role.describe"),
            "admission must send only role.describe: {:?}",
            self.methods()
        );
    }

    async fn finish(self) {
        let Self {
            rig,
            mut daemon,
            provider,
            ..
        } = self;
        rig.finish().await;
        provider.abort();
        daemon.kill().await.unwrap();
        daemon.wait().await.unwrap();
    }
}

/// A module that serves the step-transform role but not the compaction role, as Magic Context
/// does before its compaction surface ships, is refused at admission: a permanent
/// `invalid_params` naming `plan.compaction_item` and the provider, before any WAL byte, lineage
/// file or Setup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_refuses_a_compaction_provider_that_does_not_serve_the_role_before_writing() {
    let describe = json!({"majors": [{"version": "step-transform-provider/v1",
        "ops": ["role.describe", "transform.declare", "transform.hook"], "stability": "alpha"}],
        "implementation_version": "0.0.0-test", "capabilities": []});
    let Some(mut real) = RealCompactionAdmission::start(
        "compaction-role-not-served",
        DescribeReply::Answer(describe),
    )
    .await
    else {
        return;
    };
    let plan = compaction_only_plan(ADMISSION_COMPACTION);
    let frame = send_plan(&mut real.rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    error_body(&frame);
    assert_eq!(
        String::from_utf8(frame.body).unwrap(),
        COMPACTION_ROLE_NOT_SERVED_REPLY
    );
    real.assert_refused_before_writing();
    assert_eq!(real.methods(), ["role.describe"]);
    real.assert_compaction_routes_only();
    real.finish().await;
}

const COMPACTION_ROLE_NOT_SERVED_REPLY: &str = r#"{"code":"invalid_params","message":"compaction provider admission-compaction does not serve compaction-provider/v1","detail":{"field":"plan.compaction_item","problems":[{"found":["step-transform-provider/v1"],"problem":"missing_major"}],"provider":"admission-compaction"}}"#;

/// A module that refuses `role.describe` itself with a typed code that does not clear on retry,
/// as live Magic Context answers `unknown_method`, has said it does not serve the role: the
/// same permanent refusal, naming the code, asked once and before any WAL byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_refuses_a_compaction_provider_that_refuses_its_describe_before_writing() {
    let Some(mut real) = RealCompactionAdmission::start(
        "compaction-describe-refused",
        DescribeReply::Refuse("unknown_method"),
    )
    .await
    else {
        return;
    };
    let plan = compaction_only_plan(ADMISSION_COMPACTION);
    let frame = send_plan(&mut real.rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    error_body(&frame);
    assert_eq!(
        String::from_utf8(frame.body).unwrap(),
        r#"{"code":"invalid_params","message":"compaction provider admission-compaction does not serve compaction-provider/v1","detail":{"field":"plan.compaction_item","problems":[{"code":"unknown_method","problem":"describe_refused"}],"provider":"admission-compaction"}}"#
    );
    real.assert_refused_before_writing();
    assert_eq!(
        real.methods(),
        ["role.describe"],
        "a definitive refusal is not retried"
    );
    real.assert_compaction_routes_only();
    real.finish().await;
}

/// A provider whose describe cannot be read has not said it lacks the role: the send is refused
/// with the retryable `fetch_unavailable` naming the provider, never the permanent refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_reports_an_unreadable_compaction_describe_as_fetch_unavailable() {
    let Some(mut real) = RealCompactionAdmission::start(
        "compaction-describe-unreadable",
        DescribeReply::Answer(json!("not a describe")),
    )
    .await
    else {
        return;
    };
    let plan = compaction_only_plan(ADMISSION_COMPACTION);
    let frame = send_plan(&mut real.rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    error_body(&frame);
    assert_eq!(
        String::from_utf8(frame.body).unwrap(),
        r#"{"code":"fetch_unavailable","message":"provider admission-compaction refused the fetch: invalid_answer","detail":{"op":"role.describe","provider":"admission-compaction","provider_code":"invalid_answer","reason":"refused","retryable":false}}"#
    );
    real.assert_refused_before_writing();
    real.assert_compaction_routes_only();
    real.finish().await;
}

/// A provider that keeps refusing its describe as `transient` is retried until the admission
/// deadline and then reported as the retryable `fetch_unavailable`, never as lacking the role.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_reports_a_transient_compaction_describe_as_fetch_unavailable() {
    let Some(mut real) = RealCompactionAdmission::start(
        "compaction-describe-transient",
        DescribeReply::Refuse("transient"),
    )
    .await
    else {
        return;
    };
    let plan = compaction_only_plan(ADMISSION_COMPACTION);
    let frame = send_plan(&mut real.rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    error_body(&frame);
    assert_eq!(
        String::from_utf8(frame.body).unwrap(),
        r#"{"code":"fetch_unavailable","message":"provider admission-compaction refused the fetch: transient","detail":{"message":"scripted describe refusal","op":"role.describe","provider":"admission-compaction","provider_code":"transient","reason":"refused","retryable":true}}"#
    );
    assert!(
        real.methods().len() > 1,
        "a transient describe refusal is retried within the deadline: {:?}",
        real.methods()
    );
    real.assert_refused_before_writing();
    real.assert_compaction_routes_only();
    real.finish().await;
}

/// A provider that describes the synchronous compaction op set (`compaction.setup` and
/// `compaction.step`) does not serve the published `compaction-provider/v1`, which also requires
/// `view.get` and `compaction.ride`. Admission refuses it permanently, naming each missing op,
/// after asking only `role.describe` and before any WAL byte, lineage file or Setup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_refuses_a_provider_describing_the_synchronous_compaction_ops_before_writing() {
    let describe = json!({"majors": [{"version": "compaction-provider/v1",
        "ops": ["role.describe", "compaction.setup", "compaction.step"], "stability": "alpha"}],
        "implementation_version": "0.0.0-test", "capabilities": []});
    let Some(mut real) = RealCompactionAdmission::start(
        "compaction-synchronous-ops",
        DescribeReply::Answer(describe),
    )
    .await
    else {
        return;
    };
    let plan = compaction_only_plan(ADMISSION_COMPACTION);
    let frame = send_plan(&mut real.rig, 7, "turn-1", "hi", Some(&plan), json!({})).await;
    error_body(&frame);
    assert_eq!(
        String::from_utf8(frame.body).unwrap(),
        r#"{"code":"invalid_params","message":"compaction provider admission-compaction does not serve compaction-provider/v1","detail":{"field":"plan.compaction_item","problems":[{"op":"view.get","problem":"missing_op"},{"op":"compaction.ride","problem":"missing_op"}],"provider":"admission-compaction"}}"#
    );
    real.assert_refused_before_writing();
    assert_eq!(real.methods(), ["role.describe"]);
    real.assert_compaction_routes_only();
    real.finish().await;
}

fn joint_exchanges() -> Value {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compaction-joint");
    let sums = std::fs::read_to_string(root.join("SHA256SUMS")).unwrap();
    for line in sums.lines() {
        let (expected, file) = line.split_once("  ").unwrap();
        assert_eq!(
            sha256_hex(&std::fs::read(root.join(file)).unwrap()),
            expected,
            "pinned joint fixture {file}"
        );
    }
    serde_json::from_slice(&std::fs::read(root.join("exchanges.json")).unwrap()).unwrap()
}

#[test]
fn joint_fixture_has_seven_ordered_cases_and_extensible_unknown_refusal() {
    use broca_core::compaction_provider::setup::SetupAnswer;
    let cases = joint_exchanges();
    let names: Vec<_> = cases
        .as_array()
        .unwrap()
        .iter()
        .map(|case| case["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "setup-initial",
            "tag-strip-mixed",
            "reminder-last-text-block",
            "noop-keeps-view",
            "known-refusal",
            "unknown-refusal-control",
            "oversized-text-keeps-view"
        ]
    );
    let answer: SetupAnswer = serde_json::from_value(cases[5]["answer"].clone()).unwrap();
    let SetupAnswer::Refuse {
        code,
        provider_code,
        ..
    } = answer
    else {
        panic!("unknown control is a refusal")
    };
    assert_eq!(code.as_str(), "future_provider_reason");
    assert!(!code.retryable());
    assert_eq!(provider_code.as_deref(), Some("joint_unknown_control"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_mc_compaction_client_uses_isolated_daemon_and_joint_setup() {
    use broca_core::CompactionPlane;
    // Magic Context is another module's build, which neither the gate nor CI provides, so it has
    // its own requirement switch: BROCA_REQUIRE_MC=1 makes a missing build fail instead of skip.
    let mc = std::env::var_os("MAGIC_CONTEXT_BIN").map(std::path::PathBuf::from);
    if mc.is_none() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_MC").is_none(),
            "real MC build required: set MAGIC_CONTEXT_BIN"
        );
        println!("SKIP real_mc_compaction_client: MAGIC_CONTEXT_BIN is not set");
        return;
    }
    if !subc_core_bin().exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
            "daemon fixture required: {:?} is missing",
            subc_core_bin()
        );
        println!(
            "SKIP real_mc_compaction_client: {:?} is missing",
            subc_core_bin()
        );
        return;
    }
    let rig = Rig::start("real-mc-joint").await;
    let database = std::env::var_os("MAGIC_CONTEXT_DB_FIXTURE")
        .map(std::path::PathBuf::from)
        .expect("pristine MC context.db fixture required");
    let bytes = std::fs::read(database).unwrap();
    // Each MC build ships with its own pristine fixture, so the caller names that fixture's
    // checksum beside the build; without it the test expects the schema-v97 fixture of the
    // 937cdbb4 build. Either way, the fixture is checked against the checksum before use.
    let expected_fixture = std::env::var("MAGIC_CONTEXT_DB_SHA256").unwrap_or_else(|_| {
        "5d5f427c3c4428357d4a685cab01f5b79fc95e265708a8426216381af76ab2e4".to_owned()
    });
    assert_eq!(sha256_hex(&bytes), expected_fixture);
    std::fs::create_dir_all(rig.dir.join("mc-state")).unwrap();
    std::fs::write(rig.dir.join("mc-state/context.db"), bytes).unwrap();
    let mc = broca_test_support::ckdev_binary(mc.unwrap(), &rig.dir);
    let modules = json!({"magic-context": {"program":mc, "args":[], "enabled":true, "env":{
        "MAGIC_CONTEXT_STORAGE_DIR":rig.dir.join("mc-state"),
        "XDG_DATA_HOME":rig.dir.join("mc-data"), "XDG_CONFIG_HOME":rig.dir.join("mc-config")
    }}});
    let (mut daemon, connection) = compaction_daemon(&rig.dir, modules).await;
    let conn = Arc::new(
        broca_subc::SubcConnection::connect(&connection)
            .await
            .unwrap(),
    );
    let cases = joint_exchanges();
    let bind = broca_protocol::BindIdentity::new(&rig.dir, "broca", "alfonso:joint");
    // A published daemon connection does not mean its supervised MC module is live.
    // The shared route-open policy retries only typed readiness refusals and preserves
    // the last refusal, including state/live details, rather than hiding it as Setup failure.
    let readiness_started = tokio::time::Instant::now();
    let route = conn
        .route_open_within(
            "magic-context",
            &bind,
            broca_subc::connection::RouteOpenRetry::within(
                std::time::Duration::from_secs(40),
                std::time::Duration::from_millis(250),
            ),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "MC readiness route.open failed after {:.3}s (40s readiness budget plus at most 15s for the final in-flight attempt); last daemon refusal or transport error: {error}",
                readiness_started.elapsed().as_secs_f64()
            )
        });
    let description = conn
        .route_request(
            &route,
            serde_json::to_vec(&json!({"method":"role.describe","params":{}})).unwrap(),
        )
        .await
        .unwrap();
    broca_core::compaction_provider::describe::check_describe(&description).unwrap();
    conn.route_close(&route).await.unwrap();
    let plane = broca_subc::SubcCompactionPlane::new(conn);
    let request = serde_json::from_value(cases[0]["request"]["params"].clone()).unwrap();
    let setup = plane
        .setup("magic-context", &bind, request)
        .await
        .expect("real MC Setup over shared role route");
    let mut actual = serde_json::to_value(&setup).unwrap();
    let generated_id = actual["initial"]["compaction_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(generated_id.starts_with("mc-") && generated_id.len() == 67);
    // The fixture's compaction_id is a stable label; Magic Context derives it from the live session.
    actual["initial"]["compaction_id"] = cases[0]["answer"]["initial"]["compaction_id"].clone();
    // compaction-provider 0.3 decodes and ignores the `stability` and `call_when` an earlier
    // draft's Setup carried, so they are not part of the answer broca keeps.
    let mut expected = cases[0]["answer"].clone();
    let fields = expected.as_object_mut().unwrap();
    fields.remove("stability");
    fields.remove("call_when");
    assert_eq!(actual, expected);
    rig.finish().await;
    daemon.kill().await.unwrap();
    daemon.wait().await.unwrap();
}

#[tokio::test]
async fn later_plan_tools_are_refused_in_every_delivery_without_writes() {
    let (mut rig, admitter) = plan_rig("later-plan-tools").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    rig.script.hold_model(true);
    send_plan(&mut rig, 7, "first", "freeze plan", Some(&plan), json!({})).await;
    rig.wait_entered().await;
    let calls = admitter.calls();
    let identical = rig
        .records()
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted { config, .. } => {
                Some(serde_json::to_value(config.tools.as_ref()).unwrap())
            }
            _ => None,
        })
        .unwrap();
    for (index, tools) in [json!([]), identical].into_iter().enumerate() {
        for delivery in ["queue", "steer", "interrupt"] {
            for repeat_plan in [false, true] {
                let before = rig.wal_bytes();
                let reply = send_plan(
                    &mut rig,
                    7,
                    &format!("{index}-{delivery}-{repeat_plan}"),
                    "change tools",
                    repeat_plan.then_some(&plan),
                    json!({"tools": tools, "delivery": delivery}),
                )
                .await;
                assert_eq!(error_body(&reply)["code"], "invalid_params");
                assert_eq!(error_body(&reply)["detail"]["field"], "tools");
                assert_eq!(rig.wal_bytes(), before, "refused tools must write nothing");
                assert_eq!(admitter.calls(), calls, "no provider fetch on refusal");
            }
        }
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    let before = rig.wal_bytes();
    let reply = send_plan(
        &mut rig,
        7,
        "idle",
        "change tools",
        None,
        json!({"tools": []}),
    )
    .await;
    assert_eq!(error_body(&reply)["code"], "invalid_params");
    assert_eq!(error_body(&reply)["detail"]["field"], "tools");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

/// A steer that omits `model` takes the session's frozen model, and a resent plan's
/// model-bearing items are checked against that model, not against an empty placeholder: the
/// frozen plan naming the session's own model is accepted, and once a later episode moved the
/// session to another variant, the same plan is refused `plan_model_mismatch` naming the
/// inherited model, with nothing written.
#[tokio::test]
async fn a_model_less_steer_checks_its_plan_against_the_inherited_model() {
    let (mut rig, _admitter) = worker_plan_rig("plan-model-inherited").await;
    let plan_model = json!({"provider": "fake", "id": "fake", "variant": "high"});
    let plan = model_plan(plan_model.clone());
    rig.script.push(&[
        Step::Text("first"),
        Step::Text("steered"),
        Step::Text("second"),
        Step::Text("spare"),
    ]);
    rig.script.hold_model(true);
    let first = send_plan(
        &mut rig,
        7,
        "turn-1",
        "hi",
        Some(&plan),
        json!({"model": {"provider": "fake", "model": "fake", "variant": "high"}}),
    )
    .await;
    let first_id = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let steered = send_plan(
        &mut rig,
        7,
        "turn-2",
        "and this",
        Some(&plan),
        json!({"model": null, "delivery": "steer"}),
    )
    .await;
    assert_eq!(
        steered.header.ty,
        FrameType::Response,
        "a model-less steer resending a plan that names the session's own model is accepted: {}",
        String::from_utf8_lossy(&steered.body)
    );
    assert_eq!(result(&steered)["run_id"], first_id);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;

    // A later plan-less episode moves the session to another variant.
    result(
        &send_plan(
            &mut rig,
            7,
            "turn-3",
            "next",
            None,
            json!({"model": {"provider": "fake", "model": "fake", "variant": "low"}}),
        )
        .await,
    );
    rig.wait_for_finished(2).await;

    let before = rig.wal_bytes();
    let refused = send_plan(
        &mut rig,
        7,
        "turn-4",
        "again",
        Some(&plan),
        json!({"model": null, "delivery": "steer"}),
    )
    .await;
    let body: Value = serde_json::from_slice(&refused.body).unwrap();
    assert!(
        refused.header.ty == FrameType::Error && body["code"] == "plan_model_mismatch",
        "a plan naming another model than the inherited one is still refused: {body}"
    );
    assert_eq!(
        body["detail"],
        json!({
            "provider": "prefrontal-core",
            "plan_model": plan_model,
            "send_model": {"provider": "fake", "id": "fake", "variant": "low"},
        })
    );
    assert_eq!(rig.wal_bytes(), before, "refusal must preserve WAL bytes");
    rig.finish().await;
}

/// The `requires` list of every gated `RunStarted` frame in `wal`, read from the raw frames
/// (a decoded record does not keep its frame's `requires`).
fn run_started_requires(wal: &[u8]) -> Vec<Vec<String>> {
    use broca_wal::{GATED_FRAMING_VERSION, HEADER_LEN};
    let mut lists = Vec::new();
    let mut at = 0;
    while at + HEADER_LEN <= wal.len() {
        let len = u32::from_le_bytes(wal[at..at + 4].try_into().unwrap()) as usize;
        let payload = &wal[at + HEADER_LEN..at + HEADER_LEN + len];
        if wal[at + 4] == GATED_FRAMING_VERSION {
            let gated: Value = serde_json::from_slice(payload).unwrap();
            if gated["record"]["type"] == "run_started" {
                lists.push(serde_json::from_value(gated["requires"].clone()).unwrap());
            }
        }
        at += HEADER_LEN + len;
    }
    lists
}

fn agent_run_stamp() -> subc_protocol::scope::ScopeStamp {
    use subc_protocol::scope::{ScopeAttributes, ScopeKind, ScopeStamp};
    ScopeStamp::new(scope_owner(), "head-1", 1, ScopeKind::Head)
        .with_attributes(
            ScopeAttributes::new()
                .with_agent_id(Some("agent-1".into()))
                .with_run_id(Some("agent-run-1".into())),
        )
        .with_owner_authorized(true)
}

/// A session with a session class, admitted with a plan whose providers declare result formats
/// and late results (every worker Core launches), writes its first `RunStarted` in a frame
/// that requires both the session-class and the provider-declarations features. The same
/// holds with an agent-run scope and with an input cap, the other two features added through
/// a precomputed union. The provider-declarations feature once reached the session-class
/// union, which did not list it, and the WAL writer panicked on every such send.
#[tokio::test]
async fn a_classed_session_with_declaring_providers_frames_its_run_started() {
    use broca_wal::framing::{
        AGENT_RUN_SCOPE_FEATURE, INPUT_TOKEN_CAP_FEATURE, PLAN_MANIFEST_FEATURE,
        PROVIDER_DECLARATIONS_FEATURE, SESSION_CLASS_FEATURE,
    };
    for case in ["classed", "agent-run", "input-cap"] {
        let tag = format!("declaring-{case}");
        let mut plan = vector("plans/broca-head-no-compaction.json");
        let (mut rig, admitter, channel) = if case == "agent-run" {
            // An agent-run scope admits model-only plans, so its providers answer for system
            // text alone and declare late results without any tool's result format.
            plan["tool_items"] = json!([]);
            // An agent-run scope accepts sends only from its basal carrier, never its owner.
            let carrier = subc_protocol::Principal::Reserved {
                module_id: "basal".into(),
            };
            let mut rig = Rig::start_with_scopes(&tag).await;
            rig.bind_with_scope(8, Some(carrier), Some(agent_run_stamp()))
                .await;
            let admitter = Arc::new(CountingAdmitter::default());
            rig.registry().set_plan_admitter(admitter.clone());
            (rig, admitter, 8)
        } else {
            let (rig, admitter) = plan_rig(&tag).await;
            (rig, admitter, 7)
        };
        *admitter.declare.lock().unwrap() = true;
        rig.script.push(&[Step::Text("done")]);
        let mut extra = json!({"session_class": "mason"});
        let mut expected = vec![
            PLAN_MANIFEST_FEATURE,
            PROVIDER_DECLARATIONS_FEATURE,
            SESSION_CLASS_FEATURE,
        ];
        match case {
            "agent-run" => expected.push(AGENT_RUN_SCOPE_FEATURE),
            "input-cap" => {
                extra["budget"] = json!({"max_input_tokens": 1_000_000});
                expected.push(INPUT_TOKEN_CAP_FEATURE);
            }
            _ => {}
        }
        let reply = send_plan(&mut rig, channel, "turn-1", "hi", Some(&plan), extra).await;
        result(&reply);
        let records = rig.wait_for_finished(1).await;

        // The catalog answers' declarations were frozen into the manifest.
        let manifest = serde_json::to_value(manifest_of(run_started(&records)[0])).unwrap();
        let fetches = manifest["fetches"].as_array().unwrap();
        assert!(!fetches.is_empty(), "{case}: {manifest}");
        for fetch in fetches {
            assert_eq!(
                fetch["declarations_frozen"]["late_results"], true,
                "{case}: {fetch}"
            );
        }
        if case != "agent-run" {
            assert!(
                fetches
                    .iter()
                    .any(|fetch| fetch["declarations_frozen"]["result_formats"]
                        .as_object()
                        .is_some_and(|formats| !formats.is_empty())),
                "{case}: {manifest}"
            );
        }

        let requires = run_started_requires(&rig.wal_bytes());
        assert_eq!(requires.len(), 1, "{case}: {requires:?}");
        for feature in expected {
            assert!(
                requires[0].iter().any(|name| name == feature),
                "{case}: the RunStarted frame requires {:?}, without {feature}",
                requires[0]
            );
        }
        rig.finish().await;
    }
}
