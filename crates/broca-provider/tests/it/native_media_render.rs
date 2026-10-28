use broca_protocol::{
    CallOptions, ContentBlock, MediaKind, MediaRef, Message, Role, ToolCallId, ToolOutput,
    ToolOutputPart, ToolResult,
};
use broca_provider::{
    families::{frozen_config_for_family, AnthropicMessages, BedrockConverse},
    media_profile::{select_media_profile, MediaQuirk},
    media_render::InlineMedia,
    spec::{InputModalities, WireFamilyId},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::json;

fn frozen(id: WireFamilyId) -> FrozenRenderConfig {
    let mut frozen = frozen_config_for_family(id, "model");
    frozen.media = Some(select_media_profile(
        id,
        Some(MediaQuirk::Anthropic),
        "model",
        InputModalities::from_value(&json!(["image", "pdf", "audio", "video"])),
        Some(1_000_000),
    ));
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

fn request(media: &MediaRef, role: Role, is_error: bool) -> CallOptions {
    let content = if role == Role::Tool {
        vec![ContentBlock::ToolResult(ToolResult {
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
        })]
    } else {
        vec![
            ContentBlock::Text {
                text: "before".into(),
            },
            ContentBlock::Media(media.clone()),
            ContentBlock::Text {
                text: "after".into(),
            },
        ]
    };
    CallOptions::new(vec![Message::new(role, content)])
}

fn payload(media: &MediaRef, tool: bool) -> [InlineMedia<'_>; 1] {
    [InlineMedia {
        reference: media,
        base64: "YWJj",
        tool,
    }]
}

#[test]
fn anthropic_inline_image_and_pdf_keep_order_identity_and_error() {
    let frozen = frozen(WireFamilyId::AnthropicMessages);
    for (kind, mime, block_type) in [
        (MediaKind::Image, "image/png", "image"),
        (MediaKind::Document, "application/pdf", "document"),
    ] {
        let media = reference(kind, mime);
        for role in [Role::User, Role::Tool] {
            for is_error in [false, true] {
                let body = AnthropicMessages.shape_with_media(
                    &frozen,
                    &request(&media, role, is_error),
                    &payload(&media, role == Role::Tool),
                );
                let mut content = &body["messages"][0]["content"];
                if role == Role::Tool {
                    assert_eq!(content[0]["tool_use_id"], "call");
                    assert_eq!(content[0]["is_error"], is_error);
                    content = &content[0]["content"];
                }
                assert_eq!(
                    content,
                    &json!([
                        {"type":"text", "text":"before"},
                        {"type":block_type, "source":{"type":"base64", "media_type":mime, "data":"YWJj"}},
                        {"type":"text", "text":"after"}
                    ])
                );
            }
        }
    }
}

#[test]
fn bedrock_inline_kinds_follow_the_profile_and_keep_tool_status() {
    let frozen = frozen(WireFamilyId::BedrockConverse);
    for (kind, mime, name, format) in [
        (MediaKind::Image, "image/jpeg", "image", "jpeg"),
        (MediaKind::Document, "application/pdf", "document", "pdf"),
        (MediaKind::Audio, "audio/wav", "audio", "wav"),
        (MediaKind::Video, "video/mp4", "video", "mp4"),
    ] {
        let media = reference(kind, mime);
        for role in [Role::User, Role::Tool] {
            for is_error in [false, true] {
                let body = BedrockConverse.shape_with_media(
                    &frozen,
                    &request(&media, role, is_error),
                    &payload(&media, role == Role::Tool),
                );
                let mut content = &body["messages"][0]["content"];
                if role == Role::Tool {
                    assert_eq!(content[0]["toolResult"]["toolUseId"], "call");
                    assert_eq!(
                        content[0]["toolResult"]["status"],
                        if is_error { "error" } else { "success" }
                    );
                    content = &content[0]["toolResult"]["content"];
                }
                let expected = if kind == MediaKind::Audio && role == Role::Tool {
                    json!({"text":broca_protocol::content::media_not_shown_marker(&media, broca_protocol::MediaMarkerReason::Carrier)})
                } else {
                    let mut block = json!({"format":format,"source":{"bytes":"YWJj"}});
                    if kind == MediaKind::Document {
                        block["name"] = json!("document");
                    }
                    json!({name:block})
                };
                assert_eq!(
                    content,
                    &json!([{"text":"before"}, expected, {"text":"after"}])
                );
            }
        }
    }
}

#[test]
fn prepared_payloads_require_the_frozen_gate_and_exact_reference() {
    let media = reference(MediaKind::Image, "image/png");
    let opts = request(&media, Role::User, false);
    for (id, family) in [
        (
            WireFamilyId::AnthropicMessages,
            &AnthropicMessages as &dyn WireFamily,
        ),
        (
            WireFamilyId::BedrockConverse,
            &BedrockConverse as &dyn WireFamily,
        ),
    ] {
        let mut frozen = frozen(id);
        frozen.media = None;
        assert_eq!(
            family.render_with_media(&frozen, &opts, &payload(&media, false)),
            family.render(&frozen, &opts)
        );
        frozen = self::frozen(id);
        frozen.media.as_mut().unwrap().modalities = InputModalities::default();
        let denied = family.render_with_media(&frozen, &opts, &payload(&media, false));
        assert!(String::from_utf8(denied)
            .unwrap()
            .contains("reason=modality"));
        frozen = self::frozen(id);
        let mut foreign = media.clone();
        foreign.mime = "image/jpeg".into();
        let denied = family.render_with_media(&frozen, &opts, &payload(&foreign, false));
        assert!(!String::from_utf8(denied).unwrap().contains("YWJj"));
        let assistant = request(&media, Role::Assistant, false);
        assert!(!String::from_utf8(family.render_with_media(
            &frozen,
            &assistant,
            &payload(&media, false)
        ))
        .unwrap()
        .contains("YWJj"));
    }
}

#[test]
fn media_only_and_marker_only_results_keep_their_parts() {
    let media = reference(MediaKind::Image, "image/webp");
    for (id, family) in [
        (
            WireFamilyId::AnthropicMessages,
            &AnthropicMessages as &dyn WireFamily,
        ),
        (
            WireFamilyId::BedrockConverse,
            &BedrockConverse as &dyn WireFamily,
        ),
    ] {
        let frozen = frozen(id);
        for parts in [
            vec![
                ToolOutputPart::Media {
                    media: media.clone()
                };
                2
            ],
            vec![ToolOutputPart::Text {
                text: "capture failed".into(),
            }],
        ] {
            let opts = CallOptions::new(vec![Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call".into()),
                    output: ToolOutput::Parts {
                        parts: parts.clone(),
                    },
                    is_error: true,
                })],
            )]);
            let body = family.shape_with_media(&frozen, &opts, &payload(&media, true));
            let result = &body["messages"][0]["content"][0];
            let content = if id == WireFamilyId::AnthropicMessages {
                &result["content"]
            } else {
                &result["toolResult"]["content"]
            };
            assert_eq!(content.as_array().unwrap().len(), parts.len());
            if parts.len() == 2 {
                assert_eq!(
                    serde_json::to_string(content)
                        .unwrap()
                        .matches("YWJj")
                        .count(),
                    2
                );
            } else {
                assert_eq!(content[0]["text"], "capture failed");
            }
        }
    }
}

#[cfg(feature = "live")]
#[test]
fn no_bundle_keeps_legacy_carrier_markers_even_with_a_profile() {
    use broca_provider::families::{
        Gemini, GoogleCodeAssist, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt,
    };
    let media = reference(MediaKind::Image, "image/png");
    for (id, family) in [
        (
            WireFamilyId::AnthropicMessages,
            &AnthropicMessages as &dyn WireFamily,
        ),
        (
            WireFamilyId::BedrockConverse,
            &BedrockConverse as &dyn WireFamily,
        ),
        (WireFamilyId::OpenAiChat, &OpenAiChat as &dyn WireFamily),
        (
            WireFamilyId::OpenAiResponses,
            &OpenAiResponses as &dyn WireFamily,
        ),
        (
            WireFamilyId::OpenAiResponsesChatgpt,
            &OpenAiResponsesChatgpt as &dyn WireFamily,
        ),
        (WireFamilyId::Gemini, &Gemini as &dyn WireFamily),
        (
            WireFamilyId::GoogleCodeAssist,
            &GoogleCodeAssist as &dyn WireFamily,
        ),
    ] {
        let mut frozen = frozen(id);
        frozen.project_id = Some("project".into());
        for role in [Role::User, Role::Tool] {
            let mut opts = request(&media, role, true);
            opts.step_context = Some(broca_protocol::StepContext {
                run_id: "run".into(),
                step_index: 1,
                max_output_tokens: None,
                session_address: None,
                credential_id: None,
                account_ref: None,
            });
            let original = family.render(&frozen, &opts);
            assert!(String::from_utf8(original.clone())
                .unwrap()
                .contains("reason=carrier"));
            assert_eq!(
                broca_provider::media_request::render_prepared(
                    family,
                    &frozen,
                    &opts,
                    None,
                    &|_| panic!("no projection without a bundle")
                )
                .unwrap(),
                original
            );
        }
    }
}
