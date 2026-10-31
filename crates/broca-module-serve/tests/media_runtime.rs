//! Shipping-feature serve coverage for model-based media admission.
mod delivery_rig;
#[path = "media_runtime/head_cost.rs"]
mod head_cost;
#[path = "media_runtime/idle_compaction.rs"]
mod idle_compaction;
#[path = "media_runtime/refusals.rs"]
mod refusals;

use base64::{engine::general_purpose::STANDARD, Engine};
use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane};
use broca_protocol::{ToolCall, ToolOutput, ToolOutputPart, ToolResult};
use broca_provider::{
    families::frozen_config_for_family,
    media_profile::select_media_profile,
    spec::{InputModalities, WireFamilyId},
};
use delivery_rig::{result, Rig, Step};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use subc_protocol::Frame;

const PNG: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/tool-image.png");
const PERSON_PNG: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/image.png");
const TOOL_PDF: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/tool-document.pdf");

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

struct ImageTool;
#[async_trait::async_trait]
impl ToolPlane for ImageTool {
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
        let output = if let Some(capture) = &ctx.media_capture {
            let media = capture
                .capture(
                    &ctx.batch_id,
                    &call.tool_call_id,
                    PNG.to_vec(),
                    "image/png".into(),
                )
                .await
                .unwrap();
            let pdf = capture
                .capture(
                    &ctx.batch_id,
                    &call.tool_call_id,
                    TOOL_PDF.to_vec(),
                    "application/pdf".into(),
                )
                .await
                .unwrap();
            ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: "before-media".into(),
                    },
                    ToolOutputPart::Media { media },
                    ToolOutputPart::Media { media: pdf },
                    ToolOutputPart::Text {
                        text: "after-media".into(),
                    },
                ],
            }
        } else {
            let image = broca_core::media_validation::validate_media(PNG, "image/png").unwrap();
            let reply = serde_json::from_value(json!({"content":[
                {"type":"text","text":"before-media"},
                {"type":"image","mimeType":"image/png","data":STANDARD.encode(PNG),"width":image.width,"height":image.height},
                {"type":"resource","resource":{"uri":"fixture:///tool.pdf","mimeType":"application/pdf","blob":STANDARD.encode(TOOL_PDF)}},
                {"type":"text","text":"after-media"}
            ]})).unwrap();
            broca_subc::content_v1::map_content_v1_result(&call.tool_call_id, &reply)
                .unwrap()
                .output
        };
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output,
            is_error: false,
        })
    }
}

fn frozen(family: WireFamilyId) -> broca_provider::wire::FrozenRenderConfig {
    let mut frozen = frozen_config_for_family(family, "gemini-3.8-flash");
    use broca_provider::media_profile::MediaQuirk;
    let quirk = match family {
        WireFamilyId::AnthropicMessages => Some(MediaQuirk::Anthropic),
        WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses => Some(MediaQuirk::Openai),
        WireFamilyId::Gemini => Some(MediaQuirk::Google),
        _ => None,
    };
    if family == WireFamilyId::GoogleCodeAssist {
        frozen.project_id = Some("synthetic-media-project".into());
    }
    frozen.media = Some(select_media_profile(
        family,
        quirk,
        &frozen.wire_model_id,
        InputModalities::from_value(&json!(["text", "image", "pdf", "audio", "video"])),
        Some(128000),
    ));
    frozen
}

async fn call(rig: &mut Rig, method: &str, params: Value) -> Frame {
    rig.request(7, json!({"method":method,"params":params}))
        .await
}

async fn upload(rig: &mut Rig, mime: &str, bytes: &[u8]) -> String {
    let id = result(
        &call(
            rig,
            "session.upload_begin",
            json!({"mime":mime,"bytes":bytes.len(),"sha256":digest(bytes)}),
        )
        .await,
    )["upload_id"]
        .as_str()
        .unwrap()
        .to_owned();
    result(
        &call(
            rig,
            "session.upload_append",
            json!({"upload_id":id,"offset":0,"data":STANDARD.encode(bytes)}),
        )
        .await,
    );
    result(&call(rig, "session.upload_commit", json!({"upload_id":id})).await);
    id
}

fn sample(mode: &str, name: &str, frames: &[Frame]) {
    let Some(dir) = std::env::var_os("BROCA_MEDIA_SAMPLE_DIR") else {
        return;
    };
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let bytes: Vec<_> = frames
        .iter()
        .flat_map(|frame| frame.body.iter().copied().chain(*b"\n"))
        .collect();
    std::fs::write(dir.join(format!("{mode}-{name}.jsonl")), bytes).unwrap();
}

fn family_sample(enabled: bool, family: WireFamilyId, kind: &str, sent: &Frame, rig: &Rig) {
    use std::io::Write;
    let Some(dir) = std::env::var_os("BROCA_MEDIA_SAMPLE_DIR") else {
        return;
    };
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(dir);
    let mode = if enabled { "on" } else { "off" };
    let path = dir.join(format!("{mode}-family-matrix.jsonl"));
    let requests: Vec<Value> = rig
        .script
        .rendered
        .lock()
        .unwrap()
        .iter()
        .map(|body| serde_json::from_slice(body).unwrap())
        .collect();
    let row = json!({"family":format!("{family:?}"), "person_kind":kind,
        "tool_kinds":["image/png","application/pdf"],
        "send_reply":serde_json::from_slice::<Value>(&sent.body).unwrap(),
        "rendered_requests": requests});
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{row}").unwrap();
}

fn refusal(frame: &Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

fn child_case(case: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "media_runtime_child", "--nocapture"])
        .env("BROCA_MEDIA_TEST_CASE", case)
        .status()
        .unwrap();
    assert!(status.success(), "media case {case} child failed");
}

#[tokio::test]
async fn media_capable_model_without_config_sends_tool_png_on_next_request() {
    tool_png_on_next_request(true).await;
}

#[tokio::test]
async fn text_only_model_without_config_keeps_tool_png_as_marker() {
    tool_png_on_next_request(false).await;
}

async fn tool_png_on_next_request(accepts_images: bool) {
    let mut rig = Rig::start(if accepts_images {
        "tool-image"
    } else {
        "tool-marker"
    })
    .await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let mut config = frozen(WireFamilyId::AnthropicMessages);
    if !accepts_images {
        config.media = None;
    }
    *rig.script.media_frozen.lock().unwrap() = Some(config);
    *rig.script.tool_plane.lock().unwrap() = Some(Arc::new(ImageTool));
    rig.script.push(&[Step::Call("photo"), Step::Text("seen")]);
    result(&rig.send("tool-only", "look", None).await);
    rig.wait_idle().await;
    let requests = rig.script.rendered.lock().unwrap().clone();
    assert_eq!(requests.len(), 2, "first request and tool followup");
    let first: Value = serde_json::from_slice(&requests[0]).unwrap();
    let next: Value = serde_json::from_slice(&requests[1]).unwrap();
    assert!(!first.to_string().contains(&STANDARD.encode(PNG)));
    let blocks = next["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .filter(|block| block["type"] == "tool_result")
        .flat_map(|block| block["content"].as_array().into_iter().flatten())
        .collect::<Vec<_>>();
    assert_eq!(
        blocks.iter().any(
            |block| block["type"] == "image" && block["source"]["data"] == STANDARD.encode(PNG)
        ),
        accepts_images,
        "{next}"
    );
    if !accepts_images {
        assert!(next.to_string().contains("reason=modality"), "{next}");
        assert!(!next.to_string().contains(&STANDARD.encode(PNG)));
    }
    let inline = result(&call(&mut rig, "session.read", json!({})).await);
    let blocks = result(&call(&mut rig, "session.read", json!({"media":"blocks"})).await);
    let output = |page: &Value| {
        page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|entry| entry["message"]["content"].as_array().unwrap())
            .find(|block| block["tool_call_id"] == "photo" && block["type"] == "tool_result")
            .unwrap()["output"]
            .clone()
    };
    assert_eq!(output(&inline)["kind"], "text");
    assert!(output(&inline)["text"]
        .as_str()
        .unwrap()
        .contains("reason=not_requested"));
    assert_eq!(output(&blocks)["kind"], "parts");
    assert_eq!(output(&blocks)["parts"][1]["media"]["sha256"], digest(PNG));
    let configs = rig
        .records()
        .into_iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(configs.len(), 1);
    assert_eq!(
        configs[0].frozen_render_config.get("media").is_some(),
        accepts_images
    );
    rig.finish().await;
}

#[test]
fn shipping_runtime_media_on() {
    child_case("full");
}

#[test]
fn media_on_upload_refusals_keep_contract_codes() {
    child_case("upload-refusals");
}
#[test]
fn media_on_append_replay_is_offset_mismatch() {
    child_case("strict-append");
}
#[test]
fn media_on_model_view_defaults_to_safe_markers() {
    child_case("model-media");
}
#[test]
fn media_on_head_mime_facts_match_admission() {
    child_case("head-mime");
}
#[test]
fn media_on_blocks_identity_matches_content() {
    child_case("blocks-identity");
}
#[test]
#[ignore = "explicit 1k/150k active-head cost measurement"]
fn measure_active_media_head_cost() {
    child_case("head-cost");
}
#[test]
fn media_on_active_head_avoids_replay() {
    child_case("head-bounded");
}
#[test]
fn media_free_model_view_bytes_match_master() {
    child_case("model-free");
}

#[tokio::test]
#[ignore = "launched in isolation by the media runtime cases"]
async fn media_runtime_child() {
    let case = std::env::var("BROCA_MEDIA_TEST_CASE").unwrap_or_else(|_| "full".into());
    if case == "head-cost" || case == "head-bounded" {
        let sizes: &[usize] = if case == "head-cost" {
            &[1_000, 150_000]
        } else {
            &[1_000]
        };
        head_cost::measure(
            sizes,
            case == "head-bounded" || std::env::var_os("BROCA_HEAD_COST_ASSERT_BOUNDED").is_some(),
        )
        .await;
        return;
    }
    let mode = "on";
    if let Some(dir) = std::env::var_os("BROCA_MEDIA_SAMPLE_DIR") {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{mode}-family-matrix.jsonl")), []).unwrap();
    }
    let mut rig = Rig::start(&format!("runtime-media-{mode}")).await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    *rig.script.media_frozen.lock().unwrap() = Some(frozen(WireFamilyId::AnthropicMessages));
    *rig.script.tool_plane.lock().unwrap() = Some(Arc::new(ImageTool));
    refusals::configure(&rig, &case);
    rig.script.push(&[Step::Text("ready")]);
    rig.send("init", "ready", None).await;
    rig.wait_idle().await;
    if refusals::run(&mut rig, &case).await {
        rig.finish().await;
        return;
    }
    let describe = call(&mut rig, "role.describe", json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert!(checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS));
    assert!(result(&describe)["majors"][0]["ops"]
        .as_array()
        .unwrap()
        .contains(&json!("session.media_read")));
    sample(mode, "describe", &[describe]);
    let id = upload(&mut rig, "image/png", PERSON_PNG).await;
    rig.script.push(&[Step::Call("photo"), Step::Text("seen")]);
    rig.script.hold_model(true);
    let sent = rig
        .send_with(
            "media",
            "look",
            None,
            json!({"content":[{"type":"upload","upload_id":id}]}),
        )
        .await;
    result(&sent);
    let script = rig.script.clone();
    let live = rig
        .subscribe_collect_after_attach(11, json!({}), Duration::from_millis(100), move || {
            script.hold_model(false)
        })
        .await;
    rig.wait_idle().await;
    sample(mode, "subscribe-live", &live);
    let read = call(&mut rig, "session.read", json!({"media":"blocks"})).await;
    let read_json = result(&read).to_string();
    assert!(read_json.contains(&digest(PNG)), "{read_json}");
    assert!(read_json.contains(&digest(TOOL_PDF)), "{read_json}");
    assert!(read_json.contains(&digest(PERSON_PNG)), "{read_json}");
    sample(mode, "session-read", &[read]);
    let descriptors = call(&mut rig, "session.read", json!({"media":"descriptors"})).await;
    let descriptors_json = result(&descriptors).to_string();
    assert!(descriptors_json.contains("media_ref"), "{descriptors_json}");
    assert!(
        descriptors_json.contains(&digest(PERSON_PNG)),
        "{descriptors_json}"
    );
    assert!(
        descriptors_json.contains(&digest(PNG)),
        "{descriptors_json}"
    );
    assert!(!descriptors_json.contains(&STANDARD.encode(PNG)));
    sample(mode, "session-read-descriptors", &[descriptors]);
    let history = call(&mut rig, "session.history", json!({"form":"display"})).await;
    result(&history);
    sample(mode, "session-history", &[history]);
    default_reader_samples(&mut rig, &result(&sent)["run_id"]).await;
    let replay = rig
        .subscribe_collect(13, json!({"from":"start"}), Duration::from_millis(100))
        .await;
    sample(mode, "subscribe-replay", &replay);
    let media = call(
        &mut rig,
        "session.media_read",
        json!({"sha256":digest(PNG),"offset":0,"length":2097152}),
    )
    .await;
    assert_eq!(
        result(&media),
        json!({"data":STANDARD.encode(PNG),"size":PNG.len(),"mime":"image/png"})
    );
    assert!(rig
        .script
        .rendered
        .lock()
        .unwrap()
        .iter()
        .any(|bytes| String::from_utf8_lossy(bytes).contains(&STANDARD.encode(PNG))));
    sample(mode, "media-read-success", &[media]);
    let missing = call(
        &mut rig,
        "session.media_read",
        json!({"sha256":"0".repeat(64),"offset":0,"length":0}),
    )
    .await;
    assert_eq!(refusal(&missing)["code"], "unknown_media");
    sample(mode, "media-read-refusal", &[missing]);
    media_read_boundaries(&mut rig).await;
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    *rig.script.media_frozen.lock().unwrap() = Some(frozen(WireFamilyId::AnthropicMessages));
    let media = call(
        &mut rig,
        "session.media_read",
        json!({"sha256":digest(PERSON_PNG),"offset":0,"length":2097152}),
    )
    .await;
    assert_eq!(result(&media)["data"], STANDARD.encode(PERSON_PNG));
    result(&rig.send("restarted", "look again", None).await);
    rig.wait_idle().await;
    assert!(
        rig.script
            .rendered
            .lock()
            .unwrap()
            .iter()
            .any(|bytes| String::from_utf8_lossy(bytes).contains(&STANDARD.encode(PNG))),
        "rendered={:?}; failures={:?}",
        rig.script
            .rendered
            .lock()
            .unwrap()
            .iter()
            .map(|body| String::from_utf8_lossy(body).into_owned())
            .collect::<Vec<_>>(),
        rig.script.failures.lock().unwrap()
    );
    rig.finish().await;
    for family in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        family_roundtrip(family).await;
    }
}

async fn default_reader_samples(rig: &mut Rig, run_id: &Value) {
    let read = call(rig, "session.read", json!({})).await;
    let read_json = result(&read).to_string();
    let mut markers = Vec::new();
    for (kind, mime, bytes) in [
        ("image", "image/png", PERSON_PNG),
        ("image", "image/png", PNG),
        ("document", "application/pdf", TOOL_PDF),
    ] {
        let marker = cortexkit_role_llm_runner::attachments::marker(
            kind,
            mime,
            bytes.len() as u64,
            &digest(bytes),
            "not_requested",
        );
        assert!(read_json.contains(&marker), "{read_json}");
        markers.push(marker);
    }
    sample("on", "session-read-default", &[read]);
    let history = call(rig, "session.history", json!({})).await;
    let history_json = result(&history).to_string();
    assert!(
        markers.iter().all(|marker| history_json.contains(marker)),
        "{history_json}"
    );
    sample("on", "session-history-default", &[history]);
    let status = call(rig, "run.status", json!({"run_id":run_id})).await;
    let status_json = result(&status);
    assert_eq!(status_json["state"], "completed");
    assert!(!status_json.to_string().contains("media"));
    sample("on", "run-status", &[status]);
    let result_frame = call(rig, "run.result", json!({"run_id":run_id})).await;
    let result_json = result(&result_frame);
    assert_eq!(result_json["final_message"]["text"], "seen");
    assert!(!result_json.to_string().contains("media"));
    sample("on", "run-result", &[result_frame]);
}

async fn family_roundtrip(family: WireFamilyId) {
    use broca_provider::media_profile::MediaCarrier;
    let config = frozen(family);
    let mut rig = Rig::start(&format!("media-family-{family:?}")).await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    *rig.script.media_frozen.lock().unwrap() = Some(config.clone());
    *rig.script.tool_plane.lock().unwrap() = Some(Arc::new(ImageTool));
    rig.send("init", "ready", None).await;
    rig.wait_idle().await;
    for (mime, bytes, kind) in [
        ("image/png", PERSON_PNG, broca_protocol::MediaKind::Image),
        (
            "application/pdf",
            include_bytes!("../../broca-provider/examples/media-live/fixtures/document.pdf")
                .as_slice(),
            broca_protocol::MediaKind::Document,
        ),
    ] {
        let id = upload(&mut rig, mime, bytes).await;
        let before = rig.wal_bytes();
        let supported =
            config.media.as_ref().unwrap().kind(kind).user_message != MediaCarrier::Marker;
        rig.script
            .push(&[Step::Call("image"), Step::Text("answer")]);
        let sent = rig
            .send_with(
                mime,
                "inspect",
                None,
                json!({"content":[{"type":"upload","upload_id":id}]}),
            )
            .await;
        if supported {
            result(&sent);
            rig.wait_idle().await;
            assert!(
                rig.script
                    .rendered
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|body| String::from_utf8_lossy(body).contains(&STANDARD.encode(bytes))),
                "{family:?}: missing {mime}"
            );
        } else {
            assert_eq!(refusal(&sent)["code"], "media_unsupported");
            assert_eq!(rig.wal_bytes(), before);
            assert!(refusal(&sent)["message"]
                .as_str()
                .unwrap()
                .contains("model"));
        }
        family_sample(true, family, mime, &sent, &rig);
    }
    if family == WireFamilyId::OpenAiResponsesChatgpt {
        let bytes = include_bytes!("../../broca-provider/examples/media-live/fixtures/user.wav");
        let id = upload(&mut rig, "audio/wav", bytes).await;
        let before = rig.wal_bytes();
        let sent = rig
            .send_with(
                "audio",
                "listen",
                None,
                json!({"content":[{"type":"upload","upload_id":id}]}),
            )
            .await;
        assert_eq!(refusal(&sent)["code"], "media_unsupported");
        assert_eq!(rig.wal_bytes(), before);
        assert!(refusal(&sent)["message"]
            .as_str()
            .unwrap()
            .contains("audio"));
        family_sample(true, family, "audio/wav", &sent, &rig);
    }
    rig.finish().await;
}

async fn media_read_boundaries(rig: &mut Rig) {
    for (offset, length, data) in [
        (0, 0, None),
        (PNG.len(), 1, Some(String::new())),
        (
            PNG.len() - 1,
            100,
            Some(STANDARD.encode(&PNG[PNG.len() - 1..])),
        ),
    ] {
        let reply = result(
            &call(
                rig,
                "session.media_read",
                json!({"sha256":digest(PNG),"offset":offset,"length":length}),
            )
            .await,
        );
        assert_eq!(reply["size"], PNG.len());
        assert_eq!(reply.get("data").and_then(Value::as_str), data.as_deref());
    }
    for (offset, length, field) in [(PNG.len() + 1, 1, "offset"), (0, 2097153, "length")] {
        let before = rig.wal_bytes();
        let reply = call(
            rig,
            "session.media_read",
            json!({"sha256":digest(PNG),"offset":offset,"length":length}),
        )
        .await;
        assert_eq!(refusal(&reply)["code"], "invalid_params");
        assert_eq!(refusal(&reply)["detail"]["field"], field);
        assert_eq!(rig.wal_bytes(), before);
    }
    let other = b"unreferenced committed bytes";
    upload(rig, "image/png", other).await;
    let reply = call(
        rig,
        "session.media_read",
        json!({"sha256":digest(other),"offset":0,"length":0}),
    )
    .await;
    assert_eq!(refusal(&reply)["code"], "unknown_media");
    rig.bind_session(19, "other").await;
    let reply = rig.request(19, json!({"method":"session.media_read","params":{"sha256":digest(PNG),"offset":0,"length":0}})).await;
    assert_eq!(refusal(&reply)["code"], "unknown_media");
}

#[test]
fn obsolete_media_does_not_refuse_boot_before_connecting() {
    let root =
        broca_test_support::scratch_root().join(format!("media-boot-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let config = root.join("broca.jsonc");
    for text in [
        r#"{"media":{"enabeld":true}}"#,
        r#"{"media":{"enabled":"yes"}}"#,
        r#"{"media":null}"#,
    ] {
        std::fs::write(&config, text).unwrap();
        let data_home = root.join(format!("data-{}", digest(text.as_bytes())));
        let logs = data_home.join("cortexkit/broca/logs");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_ck-broca"))
            .args(["--subc", "missing-connection.json"])
            .env("BROCA_CONFIG_PATH", &config)
            .env("BROCA_STATE_ROOT", root.join("state"))
            .env("XDG_DATA_HOME", &data_home)
            .env("HOME", root.join("home"))
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_MODULE_ID")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!error.contains("parse media"), "{error}");
        assert!(error.contains("missing-connection.json"), "{error}");
        let log_text = std::fs::read_dir(&logs)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<String>();
        assert_eq!(
            log_text
                .matches("key 'media' is obsolete and ignored")
                .count(),
            1,
            "{log_text}"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
