//! What each wire family sends the model for each kind of stored tool output.
//!
//! One run per family and model kind (image-capable, text-only): the model calls the tool once
//! per case in a single step, the tool answers each call with that case's output, and the run's
//! next request carries every result. With `BROCA_TOOL_OUTPUT_SAMPLES` set to a folder, the test
//! writes the stored results as `session.read {"media": "blocks"}` serves them and each family's
//! next request body, as reader samples for a shared conversion of stored outputs.

use super::*;

/// The cases, named by their tool call ids, in the order the model calls them. A success
/// follows each failure, so no tool fails three times in a row and the run adds no repetition
/// guidance to an error's text.
const CASES: &[&str] = &[
    "text",
    "text_error",
    "text_empty",
    "denied",
    "json",
    "error",
    "parts_text",
    "error_outage",
    "parts_media",
    "parts_media_only",
];

const FAMILIES: [WireFamilyId; 7] = [
    WireFamilyId::AnthropicMessages,
    WireFamilyId::OpenAiChat,
    WireFamilyId::OpenAiResponses,
    WireFamilyId::OpenAiResponsesChatgpt,
    WireFamilyId::Gemini,
    WireFamilyId::GoogleCodeAssist,
    WireFamilyId::BedrockConverse,
];

/// Answers each call with the output its call id names.
struct KindsTool;

#[async_trait::async_trait]
impl ToolPlane for KindsTool {
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
        let image = || async {
            let capture = ctx
                .media_capture
                .as_ref()
                .expect("a store run captures tool media");
            capture
                .capture(
                    &ctx.batch_id,
                    &call.tool_call_id,
                    PNG.to_vec(),
                    "image/png".into(),
                )
                .await
                .expect("the image is captured")
        };
        let text = |text: &str| ToolOutputPart::Text { text: text.into() };
        let (output, is_error) = match call.tool_call_id.0.as_str() {
            "text" => (
                ToolOutput::Text {
                    text: "line one\nline two".into(),
                },
                false,
            ),
            "text_empty" => (ToolOutput::Text { text: "".into() }, false),
            "text_error" => (
                ToolOutput::Text {
                    text: "No such file: notes.md".into(),
                },
                true,
            ),
            "json" => (
                ToolOutput::Json {
                    value: broca_protocol::json::Json::from_value(
                        &json!({"count": 2, "files": ["a.rs", "b.rs"]}),
                    ),
                },
                false,
            ),
            "denied" => (
                ToolOutput::Denied {
                    reason: "the person declined".into(),
                },
                true,
            ),
            "error" => (
                ToolOutput::Error {
                    message: "exit status 1".into(),
                    module: None,
                    code: None,
                    detail: None,
                    nothing_sent: None,
                    retry: None,
                },
                true,
            ),
            "error_outage" => (
                ToolOutput::Error {
                    message: "the tool's module is unavailable".into(),
                    module: Some("aft".into()),
                    code: Some("target_unavailable".into()),
                    detail: Some("module restarting".into()),
                    nothing_sent: Some(true),
                    retry: Some("safe".into()),
                },
                true,
            ),
            "parts_text" => (
                ToolOutput::Parts {
                    parts: vec![text("first part"), text("second part")],
                },
                false,
            ),
            "parts_media" => (
                ToolOutput::Parts {
                    parts: vec![
                        text("the screen"),
                        ToolOutputPart::Media {
                            media: image().await,
                        },
                        text("after the image"),
                    ],
                },
                false,
            ),
            "parts_media_only" => (
                ToolOutput::Parts {
                    parts: vec![ToolOutputPart::Media {
                        media: image().await,
                    }],
                },
                false,
            ),
            other => panic!("no case named {other}"),
        };
        DispatchResult::Completed(ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output,
            is_error,
        })
    }
}

/// The family's wire name, as a sample file names it.
fn family_name(family: WireFamilyId) -> String {
    serde_json::to_value(family)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned()
}

fn sample_dir() -> Option<std::path::PathBuf> {
    let dir = std::env::var_os("BROCA_TOOL_OUTPUT_SAMPLES")?;
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(dir);
    std::fs::create_dir_all(&dir).unwrap();
    Some(dir)
}

/// Whether `family`'s image-capable profile carries a tool's image to the model at all: a
/// family whose request format has no place for it sends a `carrier` marker instead.
fn carries_tool_images(family: WireFamilyId) -> bool {
    frozen(family)
        .media
        .expect("the image-capable profile")
        .kind(broca_protocol::MediaKind::Image)
        .tool_result
        != broca_provider::media_profile::MediaCarrier::Marker
}

/// Runs one session on `family` whose tool answers every case, and returns the request the
/// model was sent after the results, with the session's `blocks` read.
async fn run_cases(family: WireFamilyId, accepts_images: bool) -> (Vec<u8>, Frame) {
    let mode = if accepts_images { "image" } else { "text" };
    let mut rig = Rig::start(&format!("tool-outputs-{}-{mode}", family_name(family))).await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let mut config = frozen(family);
    if !accepts_images {
        config.media = None;
    }
    *rig.script.media_frozen.lock().unwrap() = Some(config);
    *rig.script.tool_plane.lock().unwrap() = Some(Arc::new(KindsTool));
    // Each call carries its own arguments, so no failure repeats another's call.
    let calls = CASES
        .iter()
        .map(|case| {
            broca_protocol::ContentBlock::ToolCall(ToolCall {
                tool_call_id: broca_protocol::ToolCallId((*case).into()),
                tool_name: "work".into(),
                input: json!({ "case": case }).into(),
                thought_signature: None,
                provider_state: None,
            })
        })
        .collect();
    rig.script.push(&[Step::Stream(calls), Step::Text("seen")]);
    result(&rig.send("cases", "use the tool", None).await);
    rig.wait_idle().await;
    let requests = rig.script.rendered.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        2,
        "{family:?}: the first request and the follow-up"
    );
    let blocks = call(&mut rig, "session.read", json!({"media": "blocks"})).await;
    rig.finish().await;
    (requests[1].clone(), blocks)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_family_sends_every_stored_tool_output_kind() {
    let dir = sample_dir();
    let png = STANDARD.encode(PNG);
    for family in FAMILIES {
        for accepts_images in [true, false] {
            let (body, blocks) = run_cases(family, accepts_images).await;
            let text = String::from_utf8_lossy(&body);
            assert!(
                !text.contains("Tool repetition guidance"),
                "{family:?}: the samples hold the conversion alone"
            );
            for expected in [
                "line one",
                "No such file: notes.md",
                "a.rs",
                "the person declined",
                "exit status 1",
                "the tool's module is unavailable",
                "first part",
                "second part",
                "the screen",
                "after the image",
            ] {
                assert!(
                    text.contains(expected),
                    "{family:?} images={accepts_images}: {expected:?} missing"
                );
            }
            let shown = accepts_images && carries_tool_images(family);
            assert_eq!(
                text.contains(&png),
                shown,
                "{family:?} images={accepts_images}: the image's bytes are sent exactly when the \
                 model takes images and the request format has a place for them"
            );
            if !shown {
                assert!(
                    text.contains("[media not shown"),
                    "{family:?} images={accepts_images}: the image not sent leaves a marker"
                );
            }
            let Some(dir) = &dir else { continue };
            let mode = if accepts_images { "image" } else { "text" };
            std::fs::write(
                dir.join(format!("request-{}-{mode}.json", family_name(family))),
                &body,
            )
            .unwrap();
            if family == WireFamilyId::AnthropicMessages && accepts_images {
                std::fs::write(dir.join("stored-session-read-blocks.json"), &blocks.body).unwrap();
            }
        }
    }
}
