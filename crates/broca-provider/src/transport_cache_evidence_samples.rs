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

#[tokio::test]
async fn cache_evidence_warm_between_model_attempts_keeps_the_model_predecessor() {
    use broca_core::Provider;
    let warm = ScriptResponse::Http { status: 200, headers: vec![], body: br#"{"id":"synthetic-warm","type":"message","role":"assistant","model":"capture-model","content":[],"stop_reason":"end_turn","usage":{"input_tokens":20,"cache_read_input_tokens":1000,"output_tokens":1}}"# };
    let server = ScriptedServer::start(vec![response(1000), warm, response(1000)]).await;
    let client = build_provider_http_client();
    let root = broca_test_support::scratch_root().join("warm-cache-evidence-predecessor");
    let bind = BindIdentity::new(root.join("synthetic-project"), "scripted-http", "warm-predecessor-conversation");
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("lease"));
    let provider = provider(&server, &client);
    for index in 0..2 {
        run(request(&bind, index, json!([{"role":"user","content":[{"type":"text","text":format!("question {index}")}]}]), json!([])), &provider, &wal, &leases, &RunControl::default()).await;
        if index == 0 { provider.warm(&server.request_bodies()[0]).await.unwrap(); }
    }
    let bodies = server.request_bodies();
    assert_eq!(bodies.len(), 3);
    let warm: Value = serde_json::from_slice(&bodies[1]).unwrap();
    assert!(warm.get("stream").is_none(), "the established warm format omits streaming");
    let frames = wal.read_records(&bind).unwrap();
    let attempts: Vec<_> = frames.iter().filter(|f| matches!(f.record, WalRecord::ModelAttemptFinished { .. })).collect();
    assert_eq!(attempts.len(), 2, "warming is not a model attempt");
    let second = serde_json::to_value(&attempts[1].record).unwrap();
    assert_eq!(second["cache_evidence"]["extends_previous"], true, "a warm's changed stream/output settings must not become the model predecessor");
    assert!(second["cache_evidence"]["first_divergence"].is_null());
    assert!(second["cache_evidence"]["compared_to"].as_str().unwrap().ends_with(&format!(":{}", attempts[0].seq)));
}
