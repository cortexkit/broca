use super::*;
use broca_core::{compaction::{CompactionPlane, CompactionUnavailable}, compaction_provider::{notice::{AdvanceNotice, HistoryEditCost}, ride::{RideAnswer, RideRequest}, setup::{SetupAnswer, SetupRequest}}, RunControl};

struct SignalFamily {
    inner: Box<dyn WireFamily>,
    order: Arc<Mutex<Vec<&'static str>>>,
}
impl WireFamily for SignalFamily {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value { self.inner.shape(frozen, opts) }
    fn render_with_provenance(&self, frozen: &FrozenRenderConfig, opts: &CallOptions, sink: Option<&mut broca_protocol::render_provenance::RenderProvenance>) -> Vec<u8> {
        if sink.is_some() { self.order.lock().unwrap().push("render"); }
        self.inner.render_with_provenance(frozen, opts, sink)
    }
    fn endpoint(&self, frozen: &FrozenRenderConfig, auth: &AuthHandle, body: Vec<u8>) -> HttpRequest { self.inner.endpoint(frozen, auth, body) }
    fn normalizer(&self, frozen: &FrozenRenderConfig, step: StepId) -> Box<dyn StreamNormalizer> { self.inner.normalizer(frozen, step) }
}

#[test]
fn compaction_signals_notice_header_for_each_family_and_live_route_change() {
    use crate::spec::WireFamilyId::*;
    for (id, expected) in [(AnthropicMessages,"suffix"),(BedrockConverse,"suffix"),(OpenAiChat,"suffix"),(OpenAiResponses,"suffix"),(OpenAiResponsesChatgpt,"full_resend"),(Gemini,"suffix"),(GoogleCodeAssist,"suffix")] {
        let mut frozen = crate::families::frozen_config_for_family(id, "model");
        if id == OpenAiResponsesChatgpt {
            frozen.chatgpt_wire = Some(serde_json::from_str(include_str!("../tests/fixtures/chatgpt_codex_0_163_identity.json")).unwrap());
        }
        let provider = LiveProvider::new(SignalFamily { inner: crate::registry::wire_family(id).unwrap(), order: Arc::default() }, frozen, AuthHandle::None);
        let notice = AdvanceNotice::new("a".repeat(32),1,"lineage",0,"b".repeat(64),12,"model")
            .with_history_edit_cost(provider.history_edit_cost().unwrap());
        notice.check().unwrap();
        assert_eq!(notice.to_headers()["history_edit_cost"], expected, "{id:?}");
        if id == OpenAiResponsesChatgpt {
            provider.websocket.switch_to_https();
            assert_eq!(provider.history_edit_cost(), Some(HistoryEditCost::Suffix), "the same provider follows its current transport, not the session's initial route");
        }
    }
}

#[derive(Default)]
struct SignalsPlane {
    rides: Mutex<Vec<RideRequest>>,
    order: Arc<Mutex<Vec<&'static str>>>,
}
#[async_trait::async_trait]
impl CompactionPlane for SignalsPlane {
    async fn setup(&self, _: &str, _: &BindIdentity, request: SetupRequest) -> Result<SetupAnswer, CompactionUnavailable> {
        Ok(serde_json::from_value(serde_json::json!({"request_id":request.request_id,"answer":"ready","initial":{"compaction_id":"synthetic-context","version":1,"range":{"from":0,"to":0},"replacement":[]}})).unwrap())
    }
    async fn ride(&self, _: &str, _: &BindIdentity, request: RideRequest) -> Result<RideAnswer, CompactionUnavailable> {
        self.order.lock().unwrap().push("ride");
        self.rides.lock().unwrap().push(request);
        Ok(RideAnswer::nothing())
    }
}
fn signals_manifest() -> Arc<broca_wal::PlanManifest> {
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(serde_json::json!({"compaction_item":{"provider":"scripted-context","preset":"head","params":{}},"composition":{},"render_tags":true}).to_string()).unwrap(),
        plan_identity: "synthetic-plan".into(), composition_digest: "synthetic-composition".into(),
        tools: Arc::new(vec![]), system_text: String::new(), system_digest: String::new(), join_version: 1,
        role_versions: Default::default(), fetches: vec![], declarations: vec![],
    })
}
fn signal_sample(name: &str, value: &impl serde::Serialize) {
    if let Some(directory) = std::env::var_os("BROCA_COMPACTION_SIGNAL_SAMPLES") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut bytes = serde_json::to_vec_pretty(value).unwrap(); bytes.push(b'\n');
        std::fs::write(directory.join(name), bytes).unwrap();
    }
}

async fn signal_scenario(case: &str, enabled: bool) {
    let minutes: &[u64] = match case { "age" => &[0,9,18,27,36,41], "idle" => &[0,11], "run_start" => &[0], _ => &[0,0] };
    let mut replies = vec![text_events(); minutes.len()];
    if case == "retry" {
        replies.insert(1, vec![serde_json::json!({"type":"response.failed","response":{"error":{"code":"server_error","message":"synthetic retry"}}})]);
    }
    let mock = Mock::start(replies, false, false).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join(format!("compaction-signals-{case}-{enabled}"));
    let bind = BindIdentity::new("/synthetic-compaction-signals", "scripted-websocket", format!("{case}-{enabled}"));
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let plane = Arc::new(SignalsPlane::default());
    for (index, minute) in minutes.iter().enumerate() {
        let template = run_provider(&mock, &client, &index.to_string());
        let mut provider = LiveProvider::new(SignalFamily { inner: crate::registry::wire_family(crate::spec::WireFamilyId::OpenAiResponsesChatgpt).unwrap(), order: plane.order.clone() }, (*template.frozen).clone(), AuthHandle::None);
        provider.client = client.clone();
        provider.websocket.advance_clock(Duration::from_secs(minute * 60));
        if case == "https" && index > 0 { provider.websocket.switch_to_https(); }
        if index + 1 == minutes.len() {
            plane.order.lock().unwrap().clear();
            plane.rides.lock().unwrap().clear();
        }
        let mut request = tool_run_request(&bind);
        request.run_id = RunId(format!("signal-{case}-{enabled}-{index}"));
        request.tools.clear(); request.policy_allowed.clear();
        request.input[0].content = vec![ContentBlock::Text { text: format!("synthetic question {index}") }];
        let control = RunControl { manifest: Some(signals_manifest()), compaction_plane: Some(plane.clone()), compaction_signals: enabled, ..Default::default() };
        let (tx, mut rx) = mpsc::channel(256);
        let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let summary = broca_test_support::wait_for("compaction signal producer", broca_core::run_loop_with_control(request, &provider, &StubToolPlane::new(), &leases, &AllowAll, &wal, None, control, &tx), || format!("scenario {case} stalled")).await.unwrap();
        drop(tx); collect.await.unwrap();
        assert_eq!(summary.reason, RunFinishReason::Completed);
    }
    let rides = plane.rides.lock().unwrap().clone();
    let predicted = enabled && matches!(case, "age" | "idle" | "run_start");
    assert_eq!(rides.len(), usize::from(predicted), "{case}, enabled={enabled}");
    let order = plane.order.lock().unwrap().clone();
    assert_eq!(order.iter().filter(|event| **event == "render").count(), 1, "one final render even on a retry: {order:?}");
    if predicted {
        assert_eq!(rides[0].cause, broca_core::compaction_provider::ride::RideCause::ChainReset);
        assert!(order.iter().position(|e| *e == "ride").unwrap() < order.iter().position(|e| *e == "render").unwrap(), "{order:?}");
    }
    let frames = wal.read_records(&bind).unwrap();
    let notices: Vec<_> = frames.iter().filter_map(|frame| match &frame.record { WalRecord::CompactionNotice { headers, .. } => Some((&frame.record, headers)), _ => None }).collect();
    assert!(!notices.is_empty());
    for (position, (_, headers)) in notices.iter().enumerate() {
        let parsed = AdvanceNotice::from_headers(headers).unwrap(); parsed.check().unwrap();
        if enabled { assert_eq!(headers.get("history_edit_cost").map(String::as_str), Some(if case == "https" && position >= 2 { "suffix" } else { "full_resend" })); }
        else {
            assert!(!headers.contains_key("history_edit_cost"));
            let mut legacy = parsed.clone(); legacy.history_edit_cost = None;
            assert_eq!(serde_json::to_vec(headers).unwrap(), serde_json::to_vec(&legacy.to_headers()).unwrap(), "off must retain the old notice encoding and digest");
            assert_eq!(parsed.digest(), legacy.digest());
        }
    }
    let attempts: Vec<_> = frames.iter().filter_map(|frame| match &frame.record { WalRecord::ModelAttemptFinished { websocket_request, .. } => websocket_request.as_ref().and_then(|ws| ws.codex.as_ref()), _ => None }).collect();
    let expected_reason = match case { "age" => Some("new_connection_age"), "idle" => Some("new_connection_idle"), "run_start" => Some("new_connection_run_start"), "retry" => Some("previous_attempt_failed"), _ => None };
    if let Some(reason) = expected_reason { assert_eq!(attempts.last().unwrap().full_input_reason.as_deref(), Some(reason)); }
    if case == "retry" { assert_eq!(attempts.len(), 3); assert!(rides.is_empty(), "a send-time retry never re-enters the compaction boundary"); }
    if enabled && case == "age" {
        signal_sample("history-edit-cost-full-resend.json", notices.last().unwrap().0);
        signal_sample("chain-reset-age-ride.json", &rides[0]);
    }
    if enabled && case == "https" { signal_sample("history-edit-cost-suffix.json", notices.last().unwrap().0); }
}

#[tokio::test] async fn compaction_signals_run_start_is_before_render_and_send() { signal_scenario("run_start", true).await; }
#[tokio::test] async fn compaction_signals_age_is_the_attempts_retirement_decision() { signal_scenario("age", true).await; }
#[tokio::test] async fn compaction_signals_idle_is_the_attempts_retirement_decision() { signal_scenario("idle", true).await; }
#[tokio::test] async fn compaction_signals_send_time_retry_does_not_ride() { signal_scenario("retry", true).await; }
#[tokio::test] async fn compaction_signals_follow_a_mid_session_https_route_change() { signal_scenario("https", true).await; }
#[tokio::test] async fn compaction_signals_off_keeps_legacy_notices_and_no_rides() { signal_scenario("age", false).await; }

#[tokio::test]
async fn compaction_signals_active_socket_age_is_decided_before_the_next_render() {
    let mock = Mock::start(vec![text_events(), text_events()], false, false).await;
    let provider = codex_provider(&mock);
    provider.bind_run_transport(&bind("active-age"));
    let first = step(&provider, 1, &opts()).await;
    let next = follow_up(opts(), &first, "next question");
    provider.websocket.advance_clock(Duration::from_secs(40 * 60));
    assert!(provider.prepare_chain_reset());
    let second = step(&provider, 2, &next).await;
    assert!(!codex_trace(&second, 0).continuation);
    assert_eq!(codex_trace(&second, 0).full_input_reason.as_deref(), Some("new_connection_age"));
    assert!(!provider.prepare_chain_reset(), "the fresh completed chain must not announce the previous retirement again");
}
