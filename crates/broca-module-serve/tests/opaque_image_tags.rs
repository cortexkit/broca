//! Image-only numbering through real opaque-endpoint admission and media rendering.
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_catalog::live::{
    resolve_catalog_episode_at_base_url, AuthMode, EpisodeRenderConfig, ProviderResolution,
};
use broca_core::{
    tool_plane::{BatchCtx, DispatchResult, ToolPlane},
    AllowAll, FileLeaseStore, FileWalStore, Provider, RunControl, RunRequest, WalStore,
};
use broca_protocol::{
    BindIdentity, ContentBlock, Message, ModelHandle, Role, RunId, ToolCall, ToolOutput,
    ToolOutputPart, ToolResult,
};
use broca_provider::{media_request::LiveMedia, wire::FrozenRenderConfig};
use broca_wal::{OpaqueJson, PlanManifest, WalRecord};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct ImageOnlyTool;
#[async_trait::async_trait]
impl ToolPlane for ImageOnlyTool {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &broca_core::ToolManifest,
    ) -> Result<(), broca_core::ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), broca_core::ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        // The image-only counting fixture locates its failed capture through a MIME
        // name, not an extra text block: its payload is deliberately not a PNG.
        let reply = json!({"content":[{"type":"image","mimeType":"image/png; name=kit-image-only","data":"aGk="}]});
        let block = &reply["content"][0];
        let bytes = STANDARD.decode(block["data"].as_str().unwrap()).unwrap();
        let mime = block["mimeType"].as_str().unwrap().to_owned();
        let failure = ctx
            .media_capture
            .as_ref()
            .expect("capture is always available")
            .capture(
                &ctx.batch_id,
                &call.tool_call_id,
                bytes.clone(),
                mime.clone(),
            )
            .await
            .expect_err("the counting fixture deliberately fails image validation");
        let reference = broca_protocol::MediaRef {
            kind: broca_protocol::MediaKind::Image,
            mime,
            byte_size: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            width: None,
            height: None,
        };
        let text = broca_protocol::content::media_not_shown_marker(&reference, failure.reason);
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Parts {
                parts: vec![ToolOutputPart::Text { text }],
            },
            is_error: false,
        })
    }
}

fn response(with_call: bool) -> String {
    let text = if with_call {
        "Inspecting the image."
    } else {
        "done"
    };
    let mut chunks = vec![
        json!({"id":"image-tags","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":text},"finish_reason":null}]}),
    ];
    if with_call {
        chunks.push(json!({"id":"image-tags","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"photo","type":"function","function":{"name":"photo","arguments":"{}"}}]},"finish_reason":null}]}));
    }
    chunks.push(json!({"id":"image-tags","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":if with_call {"tool_calls"} else {"stop"}}]}));
    chunks
        .into_iter()
        .map(|chunk| format!("data: {chunk}\n\n"))
        .collect::<String>()
        + "data: [DONE]\n\n"
}

struct Model {
    url: String,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
}
async fn model(responses: Vec<String>) -> Model {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let task = tokio::spawn(async move {
        for response in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            let header_end = loop {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while request.len() < header_end + length {
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
            }
            captured
                .lock()
                .unwrap()
                .push(request[header_end..header_end + length].to_vec());
            let header = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len());
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    Model {
        url,
        requests,
        task,
    }
}

fn request(root: &Path) -> RunRequest {
    RunRequest { route_decision_id: None,
    input_written_at_ms: None, directives: None, session_class: None,
    retention: None, on_restart: None, keep_warm: None, project_id: None,
    principal: None, run_id: RunId("run".into()),
    session: BindIdentity::new(root, "test", "image-tags"),
    model: ModelHandle::new("mock", "mock-model"),
    input: vec![Message::new(Role::User, vec![ContentBlock::Text { text: "Look at this image".into() }])],
    tools: vec![serde_json::from_value(json!({"name":"photo","description":"Returns an image","input_schema":{"type":"object"}})).unwrap()],
    policy_allowed: ["photo".into()].into_iter().collect(),
    tool_choice: Default::default(), generation: Default::default(), cache: None,
    budget: None, context_limit: None, work_class: None, variant: None,
    service_tier: None, stop_when: vec![broca_protocol::StopCondition::StepCount { n: 4 }],
    context_management: None, submission_id: None, send_id: None,
    request_identity_digest: None, title: None, scope: None,
    inherit_frozen_cache: false, structured_output: None,
    }
}

fn resolve(url: &str, recorded: Option<&FrozenRenderConfig>) -> ProviderResolution {
    resolve_catalog_episode_at_base_url(
        "mock/mock-model",
        url,
        Some(2048),
        &AuthMode::None,
        &Default::default(),
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            recorded_config: recorded,
            ..Default::default()
        },
    )
    .unwrap()
}

fn attach_media(provider: &dyn Provider, wal: Arc<FileWalStore>, session: &BindIdentity) {
    let frozen: FrozenRenderConfig = serde_json::from_value(provider.render_config_blob()).unwrap();
    assert!(
        frozen.media.is_none(),
        "an opaque model has no catalog media profile"
    );
    let media_store = wal.uploads().unwrap().session_media_store(session).unwrap();
    let key = session.clone();
    let media = Arc::new(
        LiveMedia::from_checkpoint(
            media_store,
            broca_core::media_request::MediaCheckpoint::empty(),
            None,
            frozen.media,
            Box::new(move |after| {
                let range = wal
                    .read_records_range(&key, after, None, None)
                    .map_err(|error| error.to_string())?;
                match range.into_records() {
                    broca_wal::WalRangeRecords::Complete(records) => Ok(records),
                    _ => Err("the image fixture requires a complete live WAL suffix".into()),
                }
            }),
        )
        .unwrap(),
    );
    provider.set_media_request_renderer(Arc::new(move |config, opts, tags, commit, provenance| {
        media.render_with_provenance(config, opts, tags, commit, provenance)
    }));
}

async fn drive(root: &Path, req: RunRequest, resolved: &ProviderResolution, wal: &FileWalStore) {
    let manifest = PlanManifest {
        rich_ui: None,
        plan: OpaqueJson::from_json_text(r#"{"render_tags":true}"#).unwrap(),
        plan_identity: "image-tags".into(),
        composition_digest: "fixture".into(),
        tools: Arc::new(req.tools.clone()),
        system_text: String::new(),
        system_digest: "fixture".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    };
    let (out, mut events) = tokio::sync::mpsc::channel(128);
    let reader = tokio::spawn(async move { while events.recv().await.is_some() {} });
    let result = broca_core::run::run_loop_with_control(
        req,
        &resolved.provider,
        &ImageOnlyTool,
        &FileLeaseStore::new(root.join("lease")),
        &AllowAll,
        wal,
        None,
        RunControl {
            manifest: Some(Arc::new(manifest)),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    assert_eq!(result.reason, broca_protocol::RunFinishReason::Completed);
    drop(out);
    reader.await.unwrap();
}

fn root(name: &str) -> std::path::PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("opaque-image-tags-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    broca_core::media_store::create_media_directories(&root).unwrap();
    root
}

fn tool_text(bytes: &[u8]) -> String {
    let body: Value = serde_json::from_slice(bytes).unwrap();
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap()["content"]
        .as_str()
        .unwrap()
        .into()
}

#[tokio::test]
async fn fresh_opaque_model_image_only_tool_result_carries_its_numbered_label() {
    let root = root("fresh");
    let http = model(vec![response(true), response(false)]).await;
    let req = request(&root);
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    drop(wal.open(&req.session).unwrap());
    let resolved = resolve(&http.url, None);
    attach_media(resolved.provider.as_ref(), wal.clone(), &req.session);
    drive(&root, req.clone(), &resolved, wal.as_ref()).await;
    http.task.await.unwrap();
    let requests = http.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        2,
        "the first request and the tool-result followup"
    );
    // User and assistant text have numbers 1 and 2; the tool call is not counted.
    let text = tool_text(&requests[1]);
    assert!(
        text.starts_with("§3§ [image]\n"),
        "image-only result must carry its one image label; actual={text}"
    );
    assert_eq!(text.matches("§3§").count(), 1);
    assert!(
        text.contains("mime=image/png; name=kit-image-only bytes=2 sha256=8f434346648f reason=")
    );
    let records = wal.read_records(&req.session).unwrap();
    let replay = broca_core::replay(
        &records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let message = replay
        .prompt
        .iter()
        .find(|message| message.role == Role::Tool)
        .unwrap();
    assert_eq!(message.part_tags().unwrap().parts, vec![Some(3)]);
    let ContentBlock::ToolResult(result) = &message.content[0] else {
        panic!("stored tool result")
    };
    let ToolOutput::Parts { parts } = &result.output else {
        panic!("captured output stays multipart")
    };
    assert!(
        matches!(parts.as_slice(), [ToolOutputPart::Text { text }] if text.starts_with("[media not shown: kind=image mime=image/png; name=kit-image-only bytes=2 sha256=8f434346648f reason="))
    );
    let frozen = replay.run_config.unwrap().frozen_render_config;
    assert_eq!(frozen["image_notice_labels"], true);
    assert!(frozen.get("media").is_none());
    drop(resolved);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn recorded_opaque_image_only_run_resumes_with_exact_legacy_request_bytes() {
    assert_recorded_rule_resume(false).await;
}

#[tokio::test]
async fn recorded_opaque_image_notice_rule_keeps_its_label_bytes_on_resume() {
    assert_recorded_rule_resume(true).await;
}

async fn assert_recorded_rule_resume(notice_labels: bool) {
    let root = root(if notice_labels { "new-rule" } else { "legacy" });
    let http = model(vec![response(true), response(false), response(false)]).await;
    let req = request(&root);
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    drop(wal.open(&req.session).unwrap());
    let fresh = resolve(&http.url, None);
    let mut recorded: FrozenRenderConfig =
        serde_json::from_value(fresh.provider.render_config_blob()).unwrap();
    recorded.image_notice_labels = notice_labels;
    assert_eq!(
        serde_json::to_value(&recorded)
            .unwrap()
            .get("image_notice_labels")
            .cloned(),
        notice_labels.then_some(json!(true))
    );
    let admitted = resolve(&http.url, Some(&recorded));
    attach_media(admitted.provider.as_ref(), wal.clone(), &req.session);
    drive(&root, req.clone(), &admitted, wal.as_ref()).await;
    let expected = http.requests.lock().unwrap()[1].clone();
    if notice_labels {
        assert!(tool_text(&expected).starts_with("§3§ [image]\n"));
    } else {
        assert!(tool_text(&expected).starts_with("§3§ [media not shown:"));
        assert!(!tool_text(&expected).contains("§3§ [image]"));
    }
    drop(admitted);
    drop(fresh);
    let frames = wal.read_records(&req.session).unwrap();
    let cut = frames
        .iter()
        .find(|frame| matches!(frame.record, WalRecord::StepStarted { step_id } if step_id.0 == 2))
        .unwrap()
        .seq;
    let end = wal
        .read_records_range(&req.session, 0, Some(cut), None)
        .unwrap()
        .next_anchor
        .unwrap()
        .byte_end;
    // Lose only the unfinished model step; preserve every byte and capture in its prefix.
    std::fs::OpenOptions::new()
        .write(true)
        .open(wal.wal_path(&req.session))
        .unwrap()
        .set_len(end)
        .unwrap();
    let resumed = resolve(&http.url, Some(&recorded));
    assert_eq!(
        resumed.provider.render_config_blob(),
        serde_json::to_value(&recorded).unwrap()
    );
    attach_media(resumed.provider.as_ref(), wal.clone(), &req.session);
    drive(&root, req, &resumed, wal.as_ref()).await;
    http.task.await.unwrap();
    let sent = http.requests.lock().unwrap().clone();
    assert_eq!(sent.len(), 3);
    assert_eq!(
        sent[2], expected,
        "resume retains the recorded literal notice bytes"
    );
    drop(resumed);
    std::fs::remove_dir_all(root).unwrap();
}
