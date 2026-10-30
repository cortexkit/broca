// Producer captures use the real core, WAL writer and HTTP transport with synthetic data.

use super::*;
use broca_core::{FileLeaseStore, FileWalStore, RunControl, RunRequest, StubToolPlane, WalStore};
use broca_protocol::{BindIdentity, RunFinishReason, RunId, SubmissionId};
use broca_wal::WalRecord;
use serde_json::json;

fn response(cache_read: u64) -> ScriptResponse {
    let body = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"synthetic\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":20,\"cache_read_input_tokens\":0,\"output_tokens\":0}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"synthetic answer\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    ).replace("\"cache_read_input_tokens\":0", &format!("\"cache_read_input_tokens\":{cache_read}"));
    ScriptResponse::Http { status: 200, headers: vec![], body: Box::leak(body.into_bytes().into_boxed_slice()) }
}

fn provider(server: &ScriptedServer, client: &ProviderHttpClient) -> LiveProvider<AnthropicMessages> {
    let mut frozen = crate::families::anthropic_apikey_frozen_config("capture-model");
    frozen.base_url = Some(server.url.clone());
    frozen.cache = Some(crate::wire::CachePolicy { version: 1, class: crate::wire::CacheClass::Markers {
        syntax: crate::wire::MarkerSyntax::AnthropicCacheControl,
        ttl: crate::wire::CacheTtl::OneHour,
        max_breakpoints: 4, lookback_blocks: 20, tail: None, omit_ttl: false,
    } });
    let mut provider = LiveProvider::new(AnthropicMessages, frozen, AuthHandle::None);
    provider.client = client.clone();
    provider
}

fn request(bind: &BindIdentity, index: usize, input: Value, tools: Value) -> RunRequest {
    let mut request = RunRequest::from_submission(serde_json::from_value(json!({
        "model":{"provider_module_id":"anthropic","model_id":"capture-model"},
        "tools":tools,"input":input
    })).unwrap(), RunId(format!("diagnostic-capture-{index}")), bind.clone(), SubmissionId(format!("capture-send-{index}")));
    request.policy_allowed = request.tools.iter().map(|tool| tool.name.clone()).collect();
    request
}

async fn run(request: RunRequest, provider: &LiveProvider<AnthropicMessages>, wal: &FileWalStore, leases: &FileLeaseStore, control: &RunControl) {
    let (tx, mut rx) = mpsc::channel(256);
    let collect = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = broca_test_support::wait_for("diagnostic producer capture", broca_core::run_loop_with_control(request, provider, &StubToolPlane::new(), leases, &broca_core::AllowAll, wal, None, control.clone(), &tx), || "scripted HTTP provider did not finish".into()).await.unwrap();
    drop(tx);
    collect.await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
}

fn capture(name: &str, record: &WalRecord) {
    if let Some(directory) = std::env::var_os("BROCA_CACHE_EVIDENCE_SAMPLES") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut bytes = serde_json::to_vec_pretty(record).unwrap();
        bytes.push(b'\n');
        std::fs::write(directory.join(name), bytes).unwrap();
    }
}

#[tokio::test]
async fn cache_evidence_captures_anthropic_explicit_tool_bust() {
    let server = ScriptedServer::start(vec![response(1000), response(0)]).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("anthropic-cache-evidence");
    let bind = BindIdentity::new(root.join("synthetic-project"), "scripted-http", "explicit-cache-conversation");
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("lease"));
    for index in 0..2 {
        let tools = json!([{"name":"lookup","description":if index == 0 {"first definition"} else {"changed definition"},"input_schema":{"type":"object","properties":{}}}]);
        let request = request(&bind, index, json!([{"role":"user","content":[{"type":"text","text":format!("synthetic question {index}")}]}]), tools);
        run(request, &provider(&server, &client), &wal, &leases, &RunControl::default()).await;
    }
    let frames = wal.read_records(&bind).unwrap();
    let reads: Vec<_> = frames.iter().filter_map(|f| match &f.record { WalRecord::ModelStepFinished { usage, .. } => usage.cached_input_tokens, _ => None }).collect();
    assert_eq!(reads, [1000, 0]);
    let attempts: Vec<_> = frames.iter().filter(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. })).collect();
    assert_eq!(attempts.len(), 2);
    let record = serde_json::to_value(&attempts[1].record).unwrap();
    let evidence = &record["cache_evidence"];
    assert_eq!(evidence["extends_previous"], false);
    assert_eq!(evidence["first_divergence"]["region"], "tools");
    assert!(evidence["first_divergence"].get("message_ordinal").is_none());
    assert!(!evidence["previous_last_breakpoint"].is_null());
    assert_eq!(evidence["cache_ttl_ms"], 3_600_000);
    assert_eq!(evidence["ttl_kind"], "explicit");
    capture("anthropic-explicit-breakpoint-bust.json", &attempts[1].record);
}

struct Views {
    wal: Arc<FileWalStore>,
    bind: BindIdentity,
    lineage: Mutex<String>,
}

impl Views {
    fn candidate(&self, session: &str, version: u64) -> broca_core::compaction_provider::view::CompactionView {
        use broca_core::compaction_provider::{lineage::MessageRef, view::{CompactionView, ViewRange}};
        let frames = self.wal.read_records(&self.bind).unwrap();
        let records: Vec<_> = frames.into_iter().map(|f| f.record).collect();
        let state = broca_core::replay(&records).unwrap();
        let covered = if version == 2 { 2 } else { state.extent - 1 };
        let ranges = if version == 2 {
            vec![ViewRange::keep(0, covered + 1)]
        } else {
            vec![ViewRange::replace(0, 2, "[]"), ViewRange::keep(2, covered + 1)]
        };
        CompactionView::new(session, version, self.lineage.lock().unwrap().clone(), MessageRef { ordinal: covered, mid: format!("m{covered}") }, ranges)
    }
}

impl broca_core::compaction::CompactionViews for Views {
    fn newest_ready(&self, session: &str, lineage: &str, after: Option<u64>) -> Option<broca_core::compaction_provider::notice::ViewReady> {
        *self.lineage.lock().unwrap() = lineage.into();
        let steps = self.wal.read_records(&self.bind).unwrap().iter().filter(|f| matches!(f.record, WalRecord::ModelStepFinished { .. })).count();
        let version = if steps >= 2 { 3 } else { 2 };
        after.is_none_or(|after| version > after).then(|| broca_core::compaction_provider::notice::ViewReady::for_view(&self.candidate(session, version)))
    }
}

#[async_trait::async_trait]
impl broca_core::compaction::CompactionPlane for Views {
    async fn setup(&self, _: &str, _: &BindIdentity, request: broca_core::compaction_provider::setup::SetupRequest) -> Result<broca_core::compaction_provider::setup::SetupAnswer, broca_core::compaction::CompactionUnavailable> {
        if let Some(lineage) = &request.lineage_id { *self.lineage.lock().unwrap() = lineage.clone(); }
        Ok(serde_json::from_value(json!({"request_id":request.request_id,"answer":"ready","initial":{"compaction_id":"synthetic-context","version":1,"range":{"from":0,"to":0},"replacement":[]}})).unwrap())
    }

    async fn view_get(&self, _: &str, _: &BindIdentity, request: broca_core::compaction_provider::view::ViewGetRequest) -> Result<broca_core::compaction_provider::view::ViewGetAnswer, broca_core::compaction::CompactionUnavailable> {
        Ok(broca_core::compaction_provider::view::ViewGetAnswer::view(self.candidate(&request.session_ref, request.view_version)))
    }
}

#[tokio::test]
async fn cache_evidence_captures_active_compaction_view_change_and_durable_identity() {
    let server = ScriptedServer::start(vec![response(1000), response(1000), response(0)]).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("view-cache-evidence");
    let bind = BindIdentity::new(root.join("synthetic-project"), "scripted-http", "view-change-conversation");
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let leases = FileLeaseStore::new(root.join("lease"));
    let views = Arc::new(Views { wal: wal.clone(), bind: bind.clone(), lineage: Mutex::new(String::new()) });
    let plan = broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(json!({"compaction_item":{"provider":"scripted-context","preset":"head","params":{}},"composition":{},"render_tags":true}).to_string()).unwrap(),
        plan_identity: "synthetic-plan".into(), composition_digest: "synthetic-composition".into(),
        tools: Arc::new(vec![]), system_text: String::new(), system_digest: String::new(), join_version: 1,
        role_versions: Default::default(), fetches: vec![], declarations: vec![],
    };
    let control = RunControl { manifest: Some(Arc::new(plan)), compaction_plane: Some(views.clone()), compaction_views: Some(views), ..Default::default() };
    for index in 0..3 {
        let input = if index == 0 {
            json!([{"role":"user","content":[{"type":"text","text":"first question"}]},{"role":"assistant","content":[{"type":"text","text":"first answer"}]},{"role":"user","content":[{"type":"text","text":"second question"}]}])
        } else { json!([{"role":"user","content":[{"type":"text","text":format!("next question {index}")}]}]) };
        run(request(&bind, index, input, json!([])), &provider(&server, &client), wal.as_ref(), &leases, &control).await;
    }
    let frames = wal.read_records(&bind).unwrap();
    let applied: Vec<_> = frames.iter().filter_map(|f| match f.record { WalRecord::CompactionViewApplied { view_version, .. } => Some(view_version), _ => None }).collect();
    let reads: Vec<_> = frames.iter().filter_map(|f| match &f.record { WalRecord::ModelStepFinished { usage, .. } => usage.cached_input_tokens, _ => None }).collect();
    assert_eq!(reads, [1000, 1000, 0]);
    assert_eq!(applied, [2, 3]);
    let attempts: Vec<_> = frames.iter().filter(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. })).collect();
    assert_eq!(attempts.len(), 3);
    let previous = serde_json::to_value(&attempts[1].record).unwrap();
    let current = serde_json::to_value(&attempts[2].record).unwrap();
    assert_eq!(current["cache_evidence"]["view"]["applied"], 3, "evidence must use the active boundary-applied view, not replay's prior view");
    assert_eq!(previous["cache_evidence"]["view"], json!({"previous":2,"applied":2}));
    assert_eq!(current["cache_evidence"]["view"], json!({"previous":2,"applied":3}));
    assert_eq!(current["cache_evidence"]["extends_previous"], false);
    assert_eq!(current["cache_evidence"]["first_divergence"]["region"], "messages");
    assert_eq!(current["cache_evidence"]["first_divergence"]["message_index"], 0);
    assert_eq!(current["cache_evidence"]["first_divergence"]["message_ordinal"], 2);
    assert!(current["cache_evidence"]["compared_to"].as_str().unwrap().ends_with(&format!(":{}", attempts[1].seq)));
    capture("magic-context-view-change-previous.json", &attempts[1].record);
    capture("magic-context-view-change.json", &attempts[2].record);
}
