//! Attachment ingress and model-switch acceptance through a real daemon. The local
//! Fusiform module serves modalities over catalog.get, not models.dev or map_model.
//! The HTTP receiver checks provider-visible bytes, not a model's claim to see them.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use sha2::{Digest, Sha256};

const CLAUDE: &str = "claude-haiku-5-5";
const TEXT_ONLY: &str = "claude-text-only";
const PDF: &[u8] = b"%PDF-1.7\n1 0 obj<</Type/Catalog>>endobj\n%%EOF\n";
// A minimal JPEG with start-of-image, baseline-frame, scan, and end-of-image
// markers, plus one scan byte. Capture validates the container and dimensions
// without decoding pixels.
const JPEG: &[u8] = &[
    0xff, 0xd8, 0xff, 0xc0, 0, 11, 8, 0, 1, 0, 1, 1, 1, 0x11, 0, 0xff, 0xda, 0, 8, 1, 1, 0, 0, 63,
    0, 0, 0xff, 0xd9,
];
const ANTHROPIC_SSE: &str = concat!(
    "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"acceptance\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-haiku-5-5\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
    "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
    "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n",
    "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
    "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
);

#[path = "media_live_round_trip.rs"]
mod media_live_round_trip;

#[test]
fn fixtures_are_capturable() {
    for (kind, mime, bytes) in [
        (broca_protocol::MediaKind::Image, "image/jpeg", JPEG),
        (broca_protocol::MediaKind::Document, "application/pdf", PDF),
    ] {
        let reference = broca_core::media_validation::validate_media(bytes, mime).unwrap();
        assert_eq!(reference.kind, kind);
        assert_eq!(reference.mime, mime);
        assert_eq!(reference.sha256, format!("{:x}", Sha256::digest(bytes)));
    }
    let cache = broca_catalog::CatalogCache::empty();
    cache
        .refresh(serde_json::from_value(catalog_response()).unwrap())
        .unwrap();
    for model in [CLAUDE, TEXT_ONLY] {
        assert_eq!(
            cache
                .snapshot()
                .unwrap()
                .resolve_route("anthropic", model)
                .unwrap()
                .0,
            broca_provider::spec::WireFamilyId::AnthropicMessages
        );
    }
}

struct Catalog {
    calls: Arc<Mutex<Vec<Value>>>,
}

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for Catalog {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["name"], "catalog.get");
        self.calls
            .lock()
            .unwrap()
            .push(request["arguments"].clone());
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&catalog_response()).unwrap())
    }
}

fn catalog_response() -> Value {
    let routes: Value =
        serde_json::from_str(include_str!("../fixtures/fusiform_rig_routing.json")).unwrap();
    let mut models = serde_json::Map::new();
    for (model, modalities) in [
        (CLAUDE, json!(["text", "image", "pdf"])),
        (TEXT_ONLY, json!(["text"])),
    ] {
        models.insert(
            format!("anthropic/{model}"),
            json!({
                "existence":"present", "capability.attachment":false,
                "capability.input_modalities":modalities,
                "capability.reasoning":false, "capability.tool_call":true,
                "limit.context":200000, "limit.output":8192
            }),
        );
        // These authored modality rows use the captured first-party Anthropic route.
        models
            .get_mut(&format!("anthropic/{model}"))
            .unwrap()
            .as_object_mut()
            .unwrap()
            .extend(
                routes["models"]["anthropic/claude-opus-5"]
                    .as_object()
                    .unwrap()
                    .clone(),
            );
    }
    crate::routing_fixture::with_served_routing(json!({
        "source":"media-acceptance-live", "resolved_at_ms":1,
        "catalog_version":1, "models":models
    }))
}

struct LiveCatalog(tokio::task::JoinHandle<()>);
impl Drop for LiveCatalog {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn catalog(rig: &Rig, calls: Arc<Mutex<Vec<Value>>>) -> LiveCatalog {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    let manifest =
        subc_protocol::manifest::ModuleManifest::builder("fusiform", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![Tool {
                    name: "catalog.get".into(),
                    description: Some("Serves model facts.".into()),
                    execution_mode: ExecutionMode::Pure,
                    schema: json!({"type":"object"}),
                }],
                identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build();
    let (_, serving) =
        subc_client_rs::serve_with_handle(&rig.connection_file, manifest, Catalog { calls })
            .await
            .unwrap();
    LiveCatalog(tokio::spawn(async move {
        let _ = serving.await;
    }))
}

async fn request(rig: &Rig, session: &str, method: &str, params: Value) -> Result<Value, String> {
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .unwrap();
    let route = conn
        .route_open_management("broca", &rig.key(session))
        .await
        .unwrap();
    conn.route_request(
        &route,
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
    )
    .await
    .map(|reply| {
        reply
            .get("result")
            .expect("management response carries result")
            .clone()
    })
    .map_err(|error| error.to_string())
}

async fn send(rig: &Rig, id: &str, model: &str, content: Value) {
    request(
        rig,
        "media",
        "session.send",
        json!({
            "prompt":"inspect attachments", "send_id":id,
            "model":{"provider":"anthropic","model":model}, "content":content,
        }),
    )
    .await
    .unwrap();
    let records = super::wait_send_finished(rig, "media", id).await;
    for record in &records {
        if let WalRecord::RunFinished { reason, error, .. } = record {
            assert_eq!(*reason, RunFinishReason::Completed, "{record:?}");
            assert!(error.is_none(), "{record:?}");
        }
    }
}

fn media_files(state: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(state: &Path, path: &Path, files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_file() {
            files.insert(
                path.strip_prefix(state).unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            );
        } else if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                visit(state, &entry.unwrap().path(), files);
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    for name in ["media", "media-durable", "media-ephemeral", "media-uploads"] {
        visit(state, &state.join(name), &mut files);
    }
    files
}

async fn read_modes(rig: &Rig) {
    #[derive(serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum LegacyBlock {
        Text { text: String },
    }
    let inline = request(rig, "media", "session.read", json!({}))
        .await
        .unwrap();
    let blocks = request(rig, "media", "session.read", json!({"media":"blocks"}))
        .await
        .unwrap();
    let descriptors = request(rig, "media", "session.read", json!({"media":"descriptors"}))
        .await
        .unwrap();
    assert_eq!(
        inline["messages"].as_array().unwrap().len(),
        blocks["messages"].as_array().unwrap().len()
    );
    assert_eq!(
        descriptors["messages"].as_array().unwrap().len(),
        blocks["messages"].as_array().unwrap().len()
    );
    let mut references = 0;
    for ((inline, stored), described) in inline["messages"]
        .as_array()
        .unwrap()
        .iter()
        .zip(blocks["messages"].as_array().unwrap())
        .zip(descriptors["messages"].as_array().unwrap())
    {
        for block in inline["message"]["content"].as_array().unwrap() {
            let LegacyBlock::Text { text } = serde_json::from_value(block.clone())
                .expect("strict text reader accepts the default read");
            assert!(!text.is_empty());
        }
        references += stored["message"]["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|block| block["type"] == "media")
            .count();
        // Content identities are descriptor-mode metadata, computed from the block-form
        // message before inline markers or descriptor references replace its media.
        assert!(inline.get("digest").is_none());
        assert!(stored.get("digest").is_none());
        assert_eq!(
            described["digest"],
            cortexkit_role_llm_runner::content_digest::content_digest(&stored["message"])
        );
        cortexkit_role_llm_runner::read::descriptors::validate_references(&described["message"])
            .unwrap();
    }
    assert_eq!(references, 2);
    for read in [inline, blocks, descriptors] {
        let encoded = read.to_string();
        assert!(!encoded.contains(&STANDARD.encode(JPEG)));
        assert!(!encoded.contains(&STANDARD.encode(PDF)));
    }
}

async fn upload(rig: &Rig, mime: &str, bytes: &[u8]) -> String {
    let begin = cortexkit_role_llm_runner::attachments::UploadBeginRequest::new(
        mime,
        bytes.len() as u64,
        format!("{:x}", Sha256::digest(bytes)),
    );
    let reply = request(
        rig,
        "media",
        "session.upload_begin",
        serde_json::to_value(begin).unwrap(),
    )
    .await
    .unwrap();
    let id = reply["upload_id"].as_str().expect("upload id").to_owned();
    let chunk = json!({"upload_id":id,"offset":0,"data":STANDARD.encode(bytes)});
    let first = request(rig, "media", "session.upload_append", chunk.clone())
        .await
        .unwrap();
    assert_eq!(first, json!({"received":bytes.len()}));
    let staged = media_files(rig.wal_dir().parent().unwrap());
    let retry = request(rig, "media", "session.upload_append", chunk)
        .await
        .unwrap_err();
    assert!(
        retry.starts_with("subc rejected: upload_offset_mismatch —"),
        "{retry}"
    );
    assert_eq!(
        media_files(rig.wal_dir().parent().unwrap()),
        staged,
        "a lost-reply retry must not change staging"
    );
    assert_eq!(
        request(
            rig,
            "media",
            "session.upload_status",
            json!({"upload_id":id})
        )
        .await
        .unwrap(),
        json!({"state":"open","received":bytes.len()})
    );
    let commit = json!({"upload_id":id});
    let first = request(rig, "media", "session.upload_commit", commit.clone())
        .await
        .unwrap();
    assert_eq!(
        first,
        json!({"state":"committed","sha256":format!("{:x}",Sha256::digest(bytes)),"mime":mime,"bytes":bytes.len()})
    );
    assert_eq!(
        request(rig, "media", "session.upload_commit", commit)
            .await
            .unwrap(),
        first
    );
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jpeg_and_pdf_reach_claude_from_live_fusiform_and_switch_to_modality_markers() {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let url = serve_model_sse(bodies.clone(), ANTHROPIC_SSE).await;
    let Some(rig) = Rig::launch_media(&url, None).await else {
        return;
    };
    let calls = Arc::new(Mutex::new(Vec::new()));
    let _catalog = catalog(&rig, calls.clone()).await;
    assert!(rig.wait_for_module_live().await);
    let lkg = rig
        .wal_dir()
        .parent()
        .unwrap()
        .join("catalog/fusiform-lkg.json");
    broca_test_support::wait_until("the live Fusiform catalog is installed", || async {
        let installed = std::fs::read(&lkg)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        if installed
            .as_ref()
            .is_some_and(|value| value["source"] == "media-acceptance-live")
        {
            Ok(())
        } else {
            Err(format!("catalog not installed: {installed:?}"))
        }
    })
    .await;
    // Uploads belong to an existing session; the first text turn establishes it.
    send(&rig, "warmup", CLAUDE, json!([])).await;
    let image = upload(&rig, "image/jpeg", JPEG).await;
    let document = upload(&rig, "application/pdf", PDF).await;
    send(
        &rig,
        "attached",
        CLAUDE,
        json!([
            {"type":"upload","upload_id":image}, {"type":"upload","upload_id":document}
        ]),
    )
    .await;
    let records = session_records(&rig.wal_dir(), "media").unwrap();
    let input = records
        .iter()
        .rev()
        .find_map(|record| match record {
            WalRecord::RunStarted { input, .. } => Some(input),
            _ => None,
        })
        .unwrap();
    let stored: Vec<_> = broca_provider::person_media::references(input)
        .map(|reference| (reference.mime.clone(), reference.sha256.clone()))
        .collect();
    assert_eq!(
        stored,
        vec![
            ("image/jpeg".into(), format!("{:x}", Sha256::digest(JPEG))),
            (
                "application/pdf".into(),
                format!("{:x}", Sha256::digest(PDF))
            ),
        ]
    );
    {
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 2, "{}", model_request_summary(&bodies));
        let body: Value = serde_json::from_slice(&bodies[1]).unwrap();
        let parts = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["content"].as_array().unwrap())
            .collect::<Vec<_>>();
        for (kind, mime, bytes) in [
            ("image", "image/jpeg", JPEG),
            ("document", "application/pdf", PDF),
        ] {
            let part = parts
                .iter()
                .find(|part| part["type"] == kind)
                .expect("native media part");
            assert_eq!(part["source"]["type"], "base64");
            assert_eq!(part["source"]["media_type"], mime);
            assert_eq!(
                STANDARD
                    .decode(part["source"]["data"].as_str().unwrap())
                    .unwrap(),
                bytes
            );
        }
    }
    assert!(calls
        .lock()
        .unwrap()
        .iter()
        .any(|arguments| arguments["fact_prefixes"]
            .as_array()
            .unwrap()
            .contains(&json!("capability.input_modalities"))));
    read_modes(&rig).await;
    send(&rig, "switch", TEXT_ONLY, json!([])).await;
    {
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3, "{}", model_request_summary(&bodies));
        let body = String::from_utf8(bodies[2].clone()).unwrap();
        assert!(body.contains("kind=image mime=image/jpeg"), "{body}");
        assert!(body.contains("reason=modality"), "{body}");
        assert!(!body.contains(&STANDARD.encode(JPEG)));
        assert!(!body.contains(&STANDARD.encode(PDF)));
    }
    let mut new_image = JPEG.to_vec();
    let scan_byte = new_image.len() - 3;
    new_image[scan_byte] = 1;
    let refused_image = upload(&rig, "image/jpeg", &new_image).await;
    let wal = broca_core::FileWalStore::new(rig.wal_dir());
    let before = std::fs::read(wal.wal_path(&rig.key("media"))).unwrap();
    let state = rig.wal_dir().parent().unwrap().to_owned();
    let stores_before = media_files(&state);
    let error = request(&rig, "media", "session.send", json!({
        "prompt":"new image", "send_id":"refused", "model":{"provider":"anthropic","model":TEXT_ONLY},
        "content":[{"type":"upload","upload_id":refused_image}]
    })).await.unwrap_err();
    for expected in ["media_unsupported", "image", "modality", TEXT_ONLY] {
        assert!(error.contains(expected), "{error}");
    }
    assert_eq!(
        std::fs::read(wal.wal_path(&rig.key("media"))).unwrap(),
        before
    );
    assert_eq!(media_files(&state), stores_before);
    let bodies = bodies.lock().unwrap();
    assert_eq!(bodies.len(), 3, "{}", model_request_summary(&bodies));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_waits_for_its_queued_turn_after_the_previous_run_finishes() {
    use crate::step_transform_rig::{plan, register, subscription, State};
    use std::future::Future;
    use std::task::Poll;

    let bodies = Arc::new(Mutex::new(Vec::new()));
    let model_gate = Arc::new(tokio::sync::Notify::new());
    let url = super::serve_model_responses_paused(
        bodies.clone(),
        vec![ANTHROPIC_SSE],
        Some((1, model_gate.clone())),
    )
    .await;
    let Some(rig) = Rig::launch(&url, Some(catalog_response())).await else {
        return;
    };
    let rig = Arc::new(rig);
    let subscriptions = vec![subscription("pre_user", None, "pass", 30_000)];
    let state = State::new(subscriptions.clone());
    let _hooks = LiveCatalog(register(&rig.connection_file, state.clone()).await);
    assert!(rig.wait_for_module_live().await);
    request(
        &rig,
        "media",
        "session.send",
        json!({
            "prompt":"warmup", "send_id":"warmup", "plan":plan(&subscriptions, false),
            "model":{"provider":"anthropic", "model":CLAUDE}
        }),
    )
    .await
    .unwrap();
    super::wait_send_finished(&rig, "media", "warmup").await;
    request(
        &rig,
        "media",
        "session.send",
        json!({
            "prompt":"attached", "send_id":"attached",
            "model":{"provider":"anthropic", "model":CLAUDE}
        }),
    )
    .await
    .unwrap();
    broca_test_support::wait_until("the held second model request", || async {
        let count = bodies.lock().unwrap().len();
        if count == 2 {
            Ok(())
        } else {
            Err(format!("received {count} requests"))
        }
    })
    .await;
    let queued = request(
        &rig,
        "media",
        "session.send",
        json!({
            "prompt":"switch", "send_id":"switch",
            "model":{"provider":"anthropic", "model":TEXT_ONLY}
        }),
    )
    .await
    .unwrap();
    assert_eq!(queued["state"], "pending", "{queued}");

    // Keep the actor in a later send's hook while the earlier run writes its terminal.
    // The switch is durable in the queue but cannot start until the actor is released.
    state.held.lock().unwrap().insert("pre_user".into());
    let blocker_rig = rig.clone();
    let blocker = tokio::spawn(async move {
        request(
            &blocker_rig,
            "media",
            "session.send",
            json!({
                "prompt":"blocker", "send_id":"blocker",
                "model":{"provider":"anthropic", "model":TEXT_ONLY}
            }),
        )
        .await
        .unwrap()
    });
    broca_test_support::wait_until("the actor held in the later input hook", || async {
        let count = state.hooks().len();
        if count == 4 {
            Ok(())
        } else {
            Err(format!("observed {count} hooks"))
        }
    })
    .await;
    model_gate.notify_one();
    super::wait_send_finished(&rig, "media", "attached").await;
    assert_eq!(bodies.lock().unwrap().len(), 2);
    let mut waiting = Box::pin(super::wait_send_finished(&rig, "media", "switch"));
    let pending =
        std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await;
    assert!(
        pending,
        "the queued switch returned before request #2 arrived:\n{}",
        super::model_request_summary(&bodies.lock().unwrap())
    );

    state.held.lock().unwrap().clear();
    state.release.notify_waiters();
    blocker.await.unwrap();
    waiting.await;
    super::wait_send_finished(&rig, "media", "blocker").await;
    let bodies = bodies.lock().unwrap();
    assert_eq!(bodies.len(), 4, "{}", super::model_request_summary(&bodies));
    let switched: Value = serde_json::from_slice(&bodies[2]).unwrap();
    assert_eq!(switched["model"], TEXT_ONLY);
}
