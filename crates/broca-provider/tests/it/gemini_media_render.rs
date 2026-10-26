use broca_protocol::{
    canonical_json_bytes, CallOptions, ContentBlock, MediaKind, MediaRef, Message, Role,
    StepContext, ToolCall, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_provider::{
    families::{frozen_config_for_family, Gemini, GoogleCodeAssist},
    media_profile::{select_media_profile, MediaCarrier, MediaQuirk},
    media_render::InlineMedia,
    spec::{InputModalities, WireFamilyId},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

fn frozen(family: WireFamilyId, model: &str, live_checked: bool) -> FrozenRenderConfig {
    let mut frozen = frozen_config_for_family(family, model);
    frozen.project_id = Some("project".into());
    let mut profile = select_media_profile(
        family,
        Some(MediaQuirk::Google),
        model,
        InputModalities::from_value(&json!(["image", "pdf", "audio", "video"])),
        Some(1_000_000),
    );
    if family == WireFamilyId::GoogleCodeAssist && live_checked {
        // Model a future live-checked admission without enabling the real endpoint's gates.
        let native = select_media_profile(
            WireFamilyId::Gemini,
            Some(MediaQuirk::Google),
            model,
            profile.modalities,
            Some(1_000_000),
        );
        profile.live_gated.code_assist_media = true;
        profile.image = native.image;
        profile.document = native.document;
        profile.audio = native.audio;
        profile.video = native.video;
    }
    frozen.media = Some(profile);
    frozen
}

fn reference(kind: MediaKind, mime: &str) -> MediaRef {
    MediaRef {
        kind,
        mime: mime.into(),
        byte_size: 3,
        sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        width: (kind == MediaKind::Image).then_some(1),
        height: (kind == MediaKind::Image).then_some(1),
    }
}

fn payload(media: &MediaRef, tool: bool) -> [InlineMedia<'_>; 1] {
    [InlineMedia {
        reference: media,
        base64: "YWJj",
        tool,
    }]
}

fn request(role: Role, content: Vec<ContentBlock>) -> CallOptions {
    let mut opts = CallOptions::new(vec![Message::new(role, content)]);
    opts.step_context = Some(StepContext {
        run_id: "run".into(),
        step_index: 1,
        session_address: None,
        credential_id: None,
        account_ref: None,
    });
    opts
}

fn tool_result(media: &MediaRef, is_error: bool) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId("call".into()),
        is_error,
        output: ToolOutput::Parts {
            parts: vec![
                ToolOutputPart::Text {
                    text: "before".into(),
                },
                ToolOutputPart::Media {
                    media: media.clone(),
                },
                ToolOutputPart::Text {
                    text: "after".into(),
                },
            ],
        },
    })
}

fn with_call(mut opts: CallOptions) -> CallOptions {
    opts.prompt.insert(
        0,
        Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call".into()),
                tool_name: "screenshot".into(),
                input: json!({}),
                thought_signature: None,
            })],
        ),
    );
    opts
}

fn body(
    family: WireFamilyId,
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
    payloads: &[InlineMedia<'_>],
) -> Value {
    if family == WireFamilyId::GoogleCodeAssist {
        let envelope = GoogleCodeAssist.shape_with_media(frozen, opts, payloads);
        let gemini = Gemini.render_with_media(frozen, opts, payloads);
        assert_eq!(canonical_json_bytes(&envelope["request"]), gemini);
        envelope["request"].clone()
    } else {
        Gemini.shape_with_media(frozen, opts, payloads)
    }
}

#[test]
fn gemini_inline_user_media_keeps_all_kinds_and_part_order() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        let frozen = frozen(family, "gemini-3.0-flash", true);
        for (kind, mime) in [
            (MediaKind::Image, "image/png"),
            (MediaKind::Document, "application/pdf"),
            (MediaKind::Audio, "audio/wav"),
            (MediaKind::Video, "video/mp4"),
        ] {
            let media = reference(kind, mime);
            let opts = request(
                Role::User,
                vec![
                    ContentBlock::Text {
                        text: "before".into(),
                    },
                    ContentBlock::Media(media.clone()),
                    ContentBlock::Text {
                        text: "after".into(),
                    },
                ],
            );
            let body = body(family, &frozen, &opts, &payload(&media, false));
            assert_eq!(
                body["contents"],
                json!([{"role":"user", "parts":[
                    {"text":"before"},
                    {"inlineData":{"mimeType":mime, "data":"YWJj"}},
                    {"text":"after"}
                ]}])
            );
        }
    }
}

#[test]
fn gemini_tool_media_obeys_profile_generation_and_kind() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        for model in ["gemini-3.0-flash", "gemini-2.5-flash"] {
            let frozen = frozen(family, model, true);
            for (kind, mime) in [
                (MediaKind::Image, "image/webp"),
                (MediaKind::Document, "application/pdf"),
                (MediaKind::Audio, "audio/mpeg"),
                (MediaKind::Video, "video/mp4"),
            ] {
                let media = reference(kind, mime);
                for is_error in [false, true] {
                    for role in [Role::Tool, Role::User] {
                        let opts = with_call(request(role, vec![tool_result(&media, is_error)]));
                        let body = body(family, &frozen, &opts, &payload(&media, true));
                        let response = &body["contents"][1]["parts"][0]["functionResponse"];
                        let native = model == "gemini-3.0-flash"
                            && matches!(kind, MediaKind::Image | MediaKind::Document);
                        let text = if native {
                            "before\nafter".to_owned()
                        } else {
                            format!("before\n[media not shown: kind={} mime={mime} bytes=3 sha256=0123456789ab reason=carrier]\nafter", match kind {
                                MediaKind::Image => "image", MediaKind::Document => "document", MediaKind::Audio => "audio", MediaKind::Video => "video",
                            })
                        };
                        let key = if is_error { "error" } else { "result" };
                        let mut expected = json!({"name":"screenshot", "response":{key:text}});
                        if native {
                            expected["parts"] =
                                json!([{"inlineData":{"mimeType":mime,"data":"YWJj"}}]);
                        }
                        assert_eq!(response, &expected, "{family:?} {model} {kind:?}");
                    }
                }
            }
        }
    }
}

#[test]
fn gemini_media_only_and_marker_only_tool_results_are_not_dropped() {
    let image = reference(MediaKind::Image, "image/png");
    let pdf = reference(MediaKind::Document, "application/pdf");
    let payloads = [
        InlineMedia {
            reference: &image,
            base64: "aW1n",
            tool: true,
        },
        InlineMedia {
            reference: &pdf,
            base64: "cGRm",
            tool: true,
        },
    ];
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        let frozen = frozen(family, "gemini-3.0-flash", true);
        for parts in [
            vec![
                ToolOutputPart::Media {
                    media: image.clone(),
                },
                ToolOutputPart::Media { media: pdf.clone() },
            ],
            vec![ToolOutputPart::Text {
                text: "capture failed".into(),
            }],
        ] {
            let opts = with_call(request(
                Role::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call".into()),
                    is_error: true,
                    output: ToolOutput::Parts {
                        parts: parts.clone(),
                    },
                })],
            ));
            let body = body(family, &frozen, &opts, &payloads);
            let response = &body["contents"][1]["parts"][0]["functionResponse"];
            let expected = if parts.len() == 2 {
                json!({"name":"screenshot", "response":{"error":""}, "parts":[
                    {"inlineData":{"mimeType":"image/png","data":"aW1n"}},
                    {"inlineData":{"mimeType":"application/pdf","data":"cGRm"}}
                ]})
            } else {
                json!({"name":"screenshot", "response":{"error":"capture failed"}})
            };
            assert_eq!(response, &expected);
        }
    }
}

#[test]
fn gemini_payloads_require_a_profile_modality_mime_size_and_exact_reference() {
    let media = reference(MediaKind::Image, "image/png");
    let opts = request(Role::User, vec![ContentBlock::Media(media.clone())]);
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        let mut frozen = frozen(family, "gemini-3.0-flash", true);
        let good = frozen.clone();
        for reason in ["carrier", "modality", "mime", "too_large"] {
            frozen = good.clone();
            match reason {
                "carrier" => frozen.media = None,
                "modality" => frozen.media.as_mut().unwrap().modalities.image = Some(false),
                "mime" => {}
                "too_large" => {
                    frozen.media.as_mut().unwrap().image.max_item_bytes =
                        Some(broca_provider::media_profile::ByteLimit::Decoded(2))
                }
                _ => unreachable!(),
            }
            let mut reference = media.clone();
            if reason == "mime" {
                reference.mime = "image/gif".into();
            }
            let request = request(Role::User, vec![ContentBlock::Media(reference.clone())]);
            let rendered = body(family, &frozen, &request, &payload(&reference, false));
            let text = rendered["contents"][0]["parts"][0]["text"]
                .as_str()
                .unwrap();
            assert!(text.ends_with(&format!("reason={reason}]")), "{text}");
            assert!(!rendered.to_string().contains("YWJj"));
        }
        let mut foreign = media.clone();
        foreign.byte_size = 4;
        for payloads in [
            &payload(&foreign, false)[..],
            &payload(&media, true)[..],
            &[],
        ] {
            let denied = body(family, &good, &opts, payloads);
            assert!(!denied.to_string().contains("YWJj"));
            assert!(denied.to_string().contains("reason=carrier"));
        }
        let assistant = request(Role::Assistant, vec![ContentBlock::Media(media.clone())]);
        assert!(!body(family, &good, &assistant, &payload(&media, false))
            .to_string()
            .contains("YWJj"));
    }
}

#[test]
fn codeassist_unchecked_media_and_undocumented_gemini_mimes_stay_markers() {
    for (kind, mime) in [
        (MediaKind::Image, "image/png"),
        (MediaKind::Document, "application/pdf"),
        (MediaKind::Audio, "audio/wav"),
        (MediaKind::Video, "video/mp4"),
    ] {
        let media = reference(kind, mime);
        let frozen = frozen(WireFamilyId::GoogleCodeAssist, "gemini-3.0-flash", false);
        for tool in [false, true] {
            let opts = if tool {
                with_call(request(Role::Tool, vec![tool_result(&media, false)]))
            } else {
                request(Role::User, vec![ContentBlock::Media(media.clone())])
            };
            let body = body(
                WireFamilyId::GoogleCodeAssist,
                &frozen,
                &opts,
                &payload(&media, tool),
            );
            assert!(body.to_string().contains("reason=carrier"));
            assert!(!body.to_string().contains("YWJj"));
        }
    }
    let mut frozen = frozen(WireFamilyId::Gemini, "gemini-3.0-flash", true);
    for (kind, mime) in [
        (MediaKind::Audio, "audio/unknown"),
        (MediaKind::Image, "image/gif"),
    ] {
        let media = reference(kind, mime);
        let opts = request(Role::User, vec![ContentBlock::Media(media.clone())]);
        let body = Gemini.shape_with_media(&frozen, &opts, &payload(&media, false));
        assert!(body.to_string().contains("reason=mime"));
        assert!(!body.to_string().contains("YWJj"));
    }
    // Even a native carrier cannot put audio/video into FunctionResponsePart's image/PDF union.
    frozen.media.as_mut().unwrap().audio.tool_result = MediaCarrier::Native;
    frozen.media.as_mut().unwrap().video.tool_result = MediaCarrier::Native;
    for (kind, mime) in [
        (MediaKind::Audio, "audio/wav"),
        (MediaKind::Video, "video/mp4"),
    ] {
        let media = reference(kind, mime);
        let opts = with_call(request(Role::Tool, vec![tool_result(&media, false)]));
        let body = Gemini.shape_with_media(&frozen, &opts, &payload(&media, true));
        assert!(body.to_string().contains("reason=carrier"));
        assert!(!body.to_string().contains("YWJj"));
    }
}
