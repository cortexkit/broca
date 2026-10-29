//! Person uploads and captured tool bytes cross the real daemon on each native wire shape.
use super::*;
use broca_provider::spec::WireFamilyId;

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
const ANTHROPIC_TOOL: &str = concat!(
    "data: {\"type\":\"message_start\",\"message\":{\"id\":\"capture\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":5}}}\n\n",
    "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"capture\",\"name\":\"read\",\"input\":{}}}\n\n",
    "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"fixture\\\":\\\"image\\\"}\"}}\n\n",
    "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":1}}\n\n",
    "data: {\"type\":\"message_stop\"}\n\n",
);
const RESPONSES_TOOL: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"capture\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"capture\",\"name\":\"read\",\"arguments\":\"\"},\"output_index\":0}\n\n",
    "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"fixture\\\":\\\"image\\\"}\",\"output_index\":0}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"capture\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
);
const RESPONSES_TEXT: &str = concat!(
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"answer\",\"status\":\"in_progress\"}}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\",\"output_index\":0,\"content_index\":0}\n\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"answer\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}\n\n",
);
const GEMINI_TOOL: &str = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"read\",\"args\":{\"fixture\":\"image\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n";

fn schema() -> Value {
    json!({"type":"object","properties":{"fixture":{"type":"string"}},"required":["fixture"]})
}
fn plan() -> Value {
    let composition = json!({"providers":[{"provider":"media-capture","tools":[{"name":"read","capabilities":[]}]}],"step_transform_items":[]});
    json!({"tool_items":[{"provider":"media-capture","preset":"reader","params":{}}],"system_text_items":[],"step_transform_items":[],
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

struct ImageTool;
#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for ImageTool {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let method = request
            .get("method")
            .or_else(|| request.get("name"))
            .and_then(Value::as_str)
            .unwrap();
        let reply = match method {
            "role.describe" => {
                json!({"majors":[{"version":"tool-provider/v1","ops":["role.describe","tool.catalog"],"stability":"alpha"}],"implementation_version":"fixture","capabilities":[]})
            }
            "tool.catalog" => {
                json!({"generation":"1","catalog_digest":"media-capture","composition_digest":plan()["composition_digest"],"tools":[{
                "name":"read","description":"Capture a synthetic PNG","schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema()).unwrap(),"semantics":1,"capabilities":[],"input_schema":schema(),"result_format":"tool-result/v1"}]})
            }
            "tools/list" => {
                json!({"tools":[{"name":"read","description":"Capture a synthetic PNG","inputSchema":schema()}]})
            }
            "read" | "tools/call" => {
                json!({"content":[{"type":"text","text":"synthetic tool capture"},{"type":"image","mimeType":"image/png","data":PNG,"width":1,"height":1}],"isError":false})
            }
            _ => panic!("unexpected media tool operation: {request}"),
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&reply).unwrap())
    }
}

async fn image_tool(rig: &Rig) -> LiveCatalog {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    let manifest = subc_protocol::manifest::ModuleManifest::builder(
        "media-capture",
        env!("CARGO_PKG_VERSION"),
    )
    .provides(vec![ProviderRole::ToolProvider {
        tools: vec![Tool {
            name: "read".into(),
            description: Some("Capture a synthetic PNG".into()),
            execution_mode: ExecutionMode::Pure,
            schema: schema(),
        }],
        identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
        concurrency: Concurrency::ModuleManaged,
        emits_push: false,
        sub_supervises: false,
    }])
    .build();
    let (_, serving) = subc_client_rs::serve_with_handle(&rig.connection_file, manifest, ImageTool)
        .await
        .unwrap();
    LiveCatalog(tokio::spawn(async move {
        let _ = serving.await;
    }))
}

fn catalog_for(provider: &str, model: &str, route: &str) -> Value {
    let routes: Value =
        serde_json::from_str(include_str!("../fixtures/fusiform_rig_routing.json")).unwrap();
    let mut facts = routes["models"][route].as_object().unwrap().clone();
    facts.extend(json!({"existence":"present","capability.input_modalities":["text","image"],"capability.tool_call":true,
        "capability.reasoning":false,"limit.context":1000000,"limit.output":8192}).as_object().unwrap().clone());
    crate::routing_fixture::with_served_routing(
        json!({"source":"media-wire-acceptance","resolved_at_ms":1,"catalog_version":1,"models":{format!("{provider}/{model}"):facts}}),
    )
}

async fn send_family(
    rig: &Rig,
    provider: &str,
    model: &str,
    id: &str,
    content: Value,
    with_tool: bool,
) {
    let mut params = json!({"prompt":"Inspect the media", "send_id":id,"model":{"provider":provider,"model":model},"content":content});
    if with_tool {
        params["plan"] = plan();
    }
    request(rig, "media", "session.send", params).await.unwrap();
    let records = wait_records(rig, "media", |records| {
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count()
            == records
                .iter()
                .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                .count()
    })
    .await;
    for record in records {
        if let WalRecord::RunFinished { reason, error, .. } = record {
            assert_eq!(reason, RunFinishReason::Completed, "{error:?}");
        }
    }
}

fn samples() -> [(
    WireFamilyId,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
); 3] {
    [
        (
            WireFamilyId::AnthropicMessages,
            "anthropic",
            CLAUDE,
            "anthropic/claude-opus-5",
            ANTHROPIC_TOOL,
            ANTHROPIC_SSE,
        ),
        (
            WireFamilyId::OpenAiResponses,
            "openai",
            "gpt-5.5",
            "openai/gpt-5.5",
            RESPONSES_TOOL,
            RESPONSES_TEXT,
        ),
        (
            WireFamilyId::Gemini,
            "google",
            "gemini-3.0-flash",
            "google/gemini-2.5-flash",
            GEMINI_TOOL,
            TEXT_SSE,
        ),
    ]
}

#[test]
fn media_wire_response_samples_produce_tools_and_answers() {
    for (id, _, model, _, tool, text) in samples() {
        let family = broca_provider::registry::wire_family(id).unwrap();
        let frozen = broca_provider::families::frozen_config_for_family(id, model);
        for (response, expect_tool) in [(tool, true), (text, false)] {
            let mut normalizer = family.normalizer(&frozen, broca_protocol::StepId(1));
            let mut events = normalizer.push(response.as_bytes()).unwrap();
            events.extend(normalizer.finish().unwrap());
            let message = events
                .iter()
                .find_map(|event| match event {
                    broca_protocol::NormalizedEvent::Control(
                        broca_protocol::ProviderControl::AssistantMessage { message },
                    ) => Some(message),
                    _ => None,
                })
                .expect("sample assembles a model message");
            assert_eq!(
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, broca_protocol::ContentBlock::ToolCall(_))),
                expect_tool,
                "{id:?}"
            );
        }
    }
}

fn payloads(id: WireFamilyId, value: &Value) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        Value::Object(object) => {
            let data = match id {
                WireFamilyId::AnthropicMessages if object.get("type") == Some(&json!("image")) => {
                    value["source"]["data"].as_str()
                }
                WireFamilyId::OpenAiResponses
                    if object.get("type") == Some(&json!("input_image")) =>
                {
                    value["image_url"]
                        .as_str()
                        .and_then(|url| url.split_once(";base64,").map(|(_, data)| data))
                }
                WireFamilyId::Gemini => value["inlineData"]["data"].as_str(),
                _ => None,
            };
            if let Some(data) = data {
                out.push(STANDARD.decode(data).unwrap());
            }
            for value in object.values() {
                out.extend(payloads(id, value));
            }
        }
        Value::Array(values) => {
            for value in values {
                out.extend(payloads(id, value));
            }
        }
        _ => {}
    }
    out
}

async fn daemon_round_trip(id: WireFamilyId) {
    let (_, provider, model, route, tool, text) =
        samples().into_iter().find(|sample| sample.0 == id).unwrap();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let url = serve_model_responses(bodies.clone(), vec![text, tool, text]).await;
    let Some(rig) = Rig::launch(&url, Some(catalog_for(provider, model, route))).await else {
        return;
    };
    let _tool = image_tool(&rig).await;
    assert!(rig.wait_for_module_live().await);
    send_family(&rig, provider, model, "warmup", json!([]), true).await;
    let image = upload(&rig, "image/jpeg", JPEG).await;
    send_family(
        &rig,
        provider,
        model,
        "capture",
        json!([{"type":"upload","upload_id":image}]),
        true,
    )
    .await;
    let records = session_records(&rig.wal_dir(), "media").unwrap();
    assert!(records.iter().any(|record| matches!(record,WalRecord::ToolResult { result,.. }
        if matches!(&result.output,broca_protocol::ToolOutput::Parts { parts } if parts.iter().any(|part| matches!(part,broca_protocol::ToolOutputPart::Media { media } if media.mime=="image/png"))))));
    let stored = serde_json::to_string(&records).unwrap();
    assert!(!stored.contains(PNG));
    assert!(!stored.contains(&STANDARD.encode(JPEG)));
    let bodies = bodies.lock().unwrap();
    assert_eq!(
        bodies.len(),
        3,
        "warmup, person media, then captured tool media"
    );
    let person: Value = serde_json::from_slice(&bodies[1]).unwrap();
    assert!(payloads(id, &person).contains(&JPEG.to_vec()));
    let followup: Value = serde_json::from_slice(&bodies[2]).unwrap();
    let shown = payloads(id, &followup);
    assert!(shown.contains(&JPEG.to_vec()));
    assert!(
        shown.contains(&STANDARD.decode(PNG).unwrap()),
        "{id:?}: tool media is a real native part"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_person_and_tool_media_anthropic() {
    daemon_round_trip(WireFamilyId::AnthropicMessages).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_person_and_tool_media_openai_responses() {
    daemon_round_trip(WireFamilyId::OpenAiResponses).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_person_and_tool_media_gemini() {
    daemon_round_trip(WireFamilyId::Gemini).await;
}

fn restore_media_and_wal(source: &Path, destination: &Path) {
    fn copy_tree(source: &Path, destination: &Path, wal: bool) {
        if !source.exists() {
            return;
        }
        std::fs::create_dir_all(destination).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &target, wal);
            } else if !wal
                || entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "wal")
            {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    for tree in [
        "wal",
        "wal-durable",
        "wal-ephemeral",
        "media",
        "media-durable",
        "media-ephemeral",
        "media-uploads",
    ] {
        copy_tree(
            &source.join(tree),
            &destination.join(tree),
            tree.starts_with("wal"),
        );
    }
    let floor = source.join(broca_wal::FEATURE_FLOOR_FILENAME);
    if floor.exists() {
        std::fs::copy(floor, destination.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_portable_media_recovery_after_kill_preserves_request_bytes() {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(tokio::sync::Notify::new());
    let url = serve_model_responses_paused(
        bodies.clone(),
        vec![ANTHROPIC_SSE, ANTHROPIC_TOOL, ANTHROPIC_TOOL, ANTHROPIC_SSE],
        Some((1, gate.clone())),
    )
    .await;
    let catalog = catalog_for("anthropic", CLAUDE, "anthropic/claude-opus-5");
    let Some(mut original) = Rig::launch(&url, Some(catalog.clone())).await else {
        return;
    };
    let _original_tool = image_tool(&original).await;
    assert!(original.wait_for_module_live().await);
    send_family(&original, "anthropic", CLAUDE, "warmup", json!([]), true).await;
    let image = upload(&original, "image/jpeg", JPEG).await;
    request(&original,"media","session.send",json!({"prompt":"Inspect the media","send_id":"capture","on_restart":"resume",
        "model":{"provider":"anthropic","model":CLAUDE},"content":[{"type":"upload","upload_id":image}],"plan":plan()})).await.unwrap();
    broca_test_support::wait_until("media request before its model response", || async {
        if bodies.lock().unwrap().len() == 2 {
            Ok(())
        } else {
            Err("request not captured".to_string())
        }
    })
    .await;
    broca_test_support::kill_and_reap_tree(&mut original.daemon);
    gate.notify_waiters();
    // An active-run index intentionally seals interrupted runs. Omitting that disposable
    // index exercises recovery of the admitted step from its portable WAL, media and bind.
    let Some(mut restored) = Rig::launch(&url, Some(catalog)).await else {
        return;
    };
    restored.project = original.project.clone();
    restore_media_and_wal(
        original.wal_dir().parent().unwrap(),
        restored.wal_dir().parent().unwrap(),
    );
    let _restored_tool = image_tool(&restored).await;
    assert!(restored.wait_for_module_live().await);
    request(
        &restored,
        "media",
        "session.send",
        json!({"prompt":"resume","send_id":"resume","on_restart":"resume",
        "model":{"provider":"anthropic","model":CLAUDE}}),
    )
    .await
    .unwrap();
    wait_records(&restored, "media", |records| {
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .count()
            == records
                .iter()
                .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                .count()
    })
    .await;
    let bodies = bodies.lock().unwrap();
    assert_eq!(bodies.len(), 4);
    assert_eq!(
        bodies[1], bodies[2],
        "the resumed request equals the request sent before SIGKILL"
    );
    let followup: Value = serde_json::from_slice(&bodies[3]).unwrap();
    assert!(payloads(WireFamilyId::AnthropicMessages, &followup)
        .contains(&STANDARD.decode(PNG).unwrap()));
}
