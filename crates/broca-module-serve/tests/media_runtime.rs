//! Shipping-feature serve coverage. Each boot decision lives in its own process.
mod delivery_rig;

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

fn child(enabled: bool) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "media_runtime_child", "--nocapture"])
        .env("BROCA_MEDIA_TEST_ENABLED", enabled.to_string())
        .status()
        .unwrap();
    assert!(status.success(), "media.enabled={enabled} child failed");
}

#[test]
fn shipping_runtime_media_on() {
    child(true);
}
#[test]
fn shipping_runtime_media_off() {
    child(false);
}

#[tokio::test]
#[ignore = "launched with an isolated boot setting by shipping_runtime_media_on/off"]
async fn media_runtime_child() {
    let enabled: bool = std::env::var("BROCA_MEDIA_TEST_ENABLED")
        .unwrap()
        .parse()
        .unwrap();
    let config = broca_module_serve::config::MediaConfig::parse(&format!(
        r#"{{"media":{{"enabled":{enabled}}}}}"#
    ))
    .unwrap();
    broca_protocol::configure_media(config.enabled).unwrap();
    assert!(broca_protocol::configure_media(!enabled).is_err());
    let mode = if enabled { "on" } else { "off" };
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
    rig.script.push(&[Step::Text("ready")]);
    rig.send("init", "ready", None).await;
    rig.wait_idle().await;
    let describe = call(&mut rig, "role.describe", json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert_eq!(
        checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS),
        enabled
    );
    assert_eq!(
        result(&describe)["majors"][0]["ops"]
            .as_array()
            .unwrap()
            .contains(&json!("session.media_read")),
        enabled
    );
    sample(mode, "describe", &[describe]);
    let id = upload(&mut rig, "image/png", PERSON_PNG).await;
    let before = rig.wal_bytes();
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
    if !enabled {
        assert_eq!(refusal(&sent)["code"], "invalid_params");
        assert_eq!(rig.wal_bytes(), before);
        rig.send("tool-only", "look", None).await;
    } else {
        result(&sent);
    }
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
    assert_eq!(read_json.contains(&digest(PNG)), enabled, "{read_json}");
    assert_eq!(
        read_json.contains(&digest(TOOL_PDF)),
        enabled,
        "{read_json}"
    );
    assert_eq!(
        read_json.contains(&digest(PERSON_PNG)),
        enabled,
        "{read_json}"
    );
    if !enabled {
        assert!(read_json.contains("not shown to the model"));
    }
    sample(mode, "session-read", &[read]);
    let history = call(&mut rig, "session.history", json!({"form":"display"})).await;
    result(&history);
    sample(mode, "session-history", &[history]);
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
    if enabled {
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
    } else {
        assert!(!refusal(&media).is_null());
    }
    sample(mode, "media-read-success", &[media]);
    let missing = call(
        &mut rig,
        "session.media_read",
        json!({"sha256":"0".repeat(64),"offset":0,"length":0}),
    )
    .await;
    if enabled {
        assert_eq!(refusal(&missing)["code"], "unknown_media");
    }
    sample(mode, "media-read-refusal", &[missing]);
    if enabled {
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
    } else {
        rig.finish().await;
    }
    for family in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        family_roundtrip(family, enabled).await;
    }
}

async fn family_roundtrip(family: WireFamilyId, enabled: bool) {
    use broca_provider::media_profile::MediaCarrier;
    let config = frozen(family);
    let mut rig = Rig::start(&format!("media-family-{family:?}-{enabled}")).await;
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
        if enabled && supported {
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
            assert_eq!(
                refusal(&sent)["code"],
                if enabled {
                    "media_unsupported"
                } else {
                    "invalid_params"
                }
            );
            assert_eq!(rig.wal_bytes(), before);
            if enabled {
                assert!(refusal(&sent)["message"]
                    .as_str()
                    .unwrap()
                    .contains("model"));
            }
        }
        family_sample(enabled, family, mime, &sent, &rig);
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
        assert_eq!(
            refusal(&sent)["code"],
            if enabled {
                "media_unsupported"
            } else {
                "invalid_params"
            }
        );
        assert_eq!(rig.wal_bytes(), before);
        if enabled {
            assert!(refusal(&sent)["message"]
                .as_str()
                .unwrap()
                .contains("audio"));
        }
        family_sample(enabled, family, "audio/wav", &sent, &rig);
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
fn malformed_media_refuses_boot_before_connecting_or_opening_state() {
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
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_ck-broca"))
            .args(["--subc", "missing-connection.json"])
            .env("BROCA_CONFIG_PATH", &config)
            .env("BROCA_STATE_ROOT", root.join("state"))
            .env("CK_LOG_DIR", root.join("logs"))
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_MODULE_ID")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("media"), "{error}");
        if text.contains("enabeld") {
            assert!(error.contains("enabeld"), "{error}");
        }
        assert!(!root.join("state").exists());
    }
    std::fs::remove_dir_all(root).unwrap();
}
