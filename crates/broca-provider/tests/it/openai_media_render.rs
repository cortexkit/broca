use broca_protocol::{
    CallOptions, ContentBlock, MediaKind, MediaRef, Message, Role, ToolCallId, ToolOutput,
    ToolOutputPart, ToolResult,
};
use broca_provider::{
    families::{frozen_config_for_family, OpenAiChat, OpenAiResponses, OpenAiResponsesChatgpt},
    media_profile::{select_media_profile, MediaCarrier, MediaQuirk},
    media_render::InlineMedia,
    spec::{InputModalities, WireFamilyId},
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

fn frozen(id: WireFamilyId, quirk: MediaQuirk) -> FrozenRenderConfig {
    let mut frozen = frozen_config_for_family(id, "model");
    frozen.media = Some(select_media_profile(
        id,
        Some(quirk),
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

fn request(media: &[MediaRef], tool: bool) -> CallOptions {
    let mut parts = vec![ToolOutputPart::Text {
        text: "before".into(),
    }];
    parts.extend(
        media
            .iter()
            .cloned()
            .map(|media| ToolOutputPart::Media { media }),
    );
    parts.push(ToolOutputPart::Text {
        text: "after".into(),
    });
    let content = if tool {
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call".into()),
            is_error: true,
            output: ToolOutput::Parts { parts },
        })]
    } else {
        parts
            .into_iter()
            .map(|part| match part {
                ToolOutputPart::Text { text } => ContentBlock::Text { text },
                ToolOutputPart::Media { media } => ContentBlock::Media(media),
            })
            .collect()
    };
    CallOptions::new(vec![Message::new(
        if tool { Role::Tool } else { Role::User },
        content,
    )])
}

fn payloads(media: &[MediaRef], tool: bool) -> Vec<InlineMedia<'_>> {
    media
        .iter()
        .map(|reference| InlineMedia {
            reference,
            base64: "YWJj",
            tool,
        })
        .collect()
}

fn render(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
    media: &[MediaRef],
    tool: bool,
) -> Value {
    serde_json::from_slice(&family.render_with_media(frozen, opts, &payloads(media, tool))).unwrap()
}

#[test]
fn responses_image_and_pdf_render_in_user_and_function_output() {
    let frozen = frozen(WireFamilyId::OpenAiResponses, MediaQuirk::Openai);
    let media = [
        reference(MediaKind::Image, "image/png"),
        reference(MediaKind::Document, "application/pdf"),
    ];
    for tool in [false, true] {
        let opts = request(&media, tool);
        let stored = serde_json::to_value(&opts.prompt).unwrap();
        let body = render(&OpenAiResponses, &frozen, &opts, &media, tool);
        let content = if tool {
            assert_eq!(body["input"][0]["type"], "function_call_output");
            assert_eq!(body["input"][0]["call_id"], "call");
            &body["input"][0]["output"]
        } else {
            assert_eq!(body["input"][0]["role"], "user");
            &body["input"][0]["content"]
        };
        assert_eq!(
            content,
            &json!([
                {"type":"input_text", "text":"before"},
                {"type":"input_image", "image_url":"data:image/png;base64,YWJj"},
                {"type":"input_file", "filename":"document.pdf", "file_data":"data:application/pdf;base64,YWJj"},
                {"type":"input_text", "text":"after"}
            ])
        );
        assert_eq!(serde_json::to_value(&opts.prompt).unwrap(), stored);
    }
}

#[test]
fn chatgpt_audio_remains_mime_blocked_until_live_types_are_known() {
    let media = [reference(MediaKind::Audio, "audio/wav")];
    for tool in [false, true] {
        let mut frozen = frozen(WireFamilyId::OpenAiResponsesChatgpt, MediaQuirk::Openai);
        let opts = request(&media, tool);
        let denied = render(&OpenAiResponsesChatgpt, &frozen, &opts, &media, tool);
        assert!(denied.to_string().contains("reason=carrier"));
        assert!(!denied.to_string().contains("YWJj"));
        let profile = frozen.media.as_mut().unwrap();
        profile.live_gated.chatgpt_input_audio = true;
        profile.audio.user_message = MediaCarrier::Native;
        profile.audio.tool_result = MediaCarrier::Native;
        let body = render(&OpenAiResponsesChatgpt, &frozen, &opts, &media, tool);
        let content = &body["input"][0][if tool { "output" } else { "content" }];
        assert!(content[1]["text"].as_str().unwrap().contains("reason=mime"));
        assert!(!body.to_string().contains("YWJj"));
        assert_eq!(body["store"], false);
        assert!(body.get("max_output_tokens").is_none());
        frozen
            .media
            .as_mut()
            .unwrap()
            .live_gated
            .chatgpt_input_audio = false;
        let denied = render(&OpenAiResponsesChatgpt, &frozen, &opts, &media, tool);
        assert!(!denied.to_string().contains("YWJj"));
    }
}

#[test]
fn chat_user_image_file_and_audio_use_the_chat_grammar() {
    let frozen = frozen(WireFamilyId::OpenAiChat, MediaQuirk::Openai);
    let media = [
        reference(MediaKind::Image, "image/jpeg"),
        reference(MediaKind::Document, "application/pdf"),
        reference(MediaKind::Audio, "audio/mpeg"),
    ];
    let body = render(&OpenAiChat, &frozen, &request(&media, false), &media, false);
    assert_eq!(
        body["messages"][0]["content"],
        json!([
            {"type":"text", "text":"before"},
            {"type":"image_url", "image_url":{"url":"data:image/jpeg;base64,YWJj"}},
            {"type":"file", "file":{"filename":"document.pdf", "file_data":"data:application/pdf;base64,YWJj"}},
            {"type":"input_audio", "input_audio":{"data":"YWJj", "format":"mp3"}},
            {"type":"text", "text":"after"}
        ])
    );
}

#[test]
fn chat_follow_up_waits_for_all_tool_results_and_keeps_the_stored_transcript() {
    let mut frozen = frozen(WireFamilyId::OpenAiChat, MediaQuirk::Openai);
    let media = [
        reference(MediaKind::Image, "image/png"),
        reference(MediaKind::Document, "application/pdf"),
        reference(MediaKind::Audio, "audio/wav"),
    ];
    let mut opts = request(&media, true);
    let mut second = opts.prompt[0].clone();
    if let ContentBlock::ToolResult(result) = &mut second.content[0] {
        result.tool_call_id = ToolCallId("second".into());
        result.is_error = false;
    }
    opts.prompt.push(second);
    opts.prompt.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue".into(),
        }],
    ));
    let stored = serde_json::to_value(&opts.prompt).unwrap();
    let denied = render(&OpenAiChat, &frozen, &opts, &media, true);
    assert_eq!(denied["messages"].as_array().unwrap().len(), 3);
    assert!(denied["messages"][0]["content"]
        .as_str()
        .unwrap()
        .contains("reason=carrier"));
    let profile = frozen.media.as_mut().unwrap();
    profile.live_gated.follow_up_user_message = true;
    profile.image.tool_result = MediaCarrier::FollowUpUserMessage;
    profile.document.tool_result = MediaCarrier::FollowUpUserMessage;
    profile.audio.tool_result = MediaCarrier::FollowUpUserMessage;
    let body = render(&OpenAiChat, &frozen, &opts, &media, true);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 4);
    assert_eq!(messages[0], denied["messages"][0]);
    assert_eq!(messages[1]["role"], "tool");
    assert_eq!(messages[1]["tool_call_id"], "second");
    assert_eq!(messages[2]["role"], "user");
    let follow_up = messages[2]["content"].as_array().unwrap();
    assert_eq!(follow_up.len(), 8);
    assert_eq!(
        follow_up[0],
        json!({"type":"text", "text":"Media from tool call call:"})
    );
    assert_eq!(follow_up[1]["type"], "image_url");
    assert_eq!(follow_up[2]["type"], "file");
    assert_eq!(follow_up[3]["type"], "input_audio");
    assert_eq!(follow_up[4]["text"], "Media from tool call second:");
    assert_eq!(messages[3]["content"], "continue");
    assert_eq!(serde_json::to_value(&opts.prompt).unwrap(), stored);
}

#[test]
fn compatible_chat_providers_obey_their_frozen_profiles() {
    let media = [
        reference(MediaKind::Image, "image/png"),
        reference(MediaKind::Document, "application/pdf"),
        reference(MediaKind::Audio, "audio/wav"),
        reference(MediaKind::Video, "video/mp4"),
    ];
    for (quirk, allowed) in [
        (MediaQuirk::Openrouter, [true, true, true, true]),
        (MediaQuirk::Xai, [true, false, false, false]),
        (MediaQuirk::Deepseek, [true, false, false, false]),
        (MediaQuirk::Kimi, [true, false, false, true]),
        (MediaQuirk::Meta, [true, true, true, true]),
        (MediaQuirk::Anthropic, [false, false, false, false]),
    ] {
        let frozen = frozen(WireFamilyId::OpenAiChat, quirk);
        let body = render(&OpenAiChat, &frozen, &request(&media, false), &media, false);
        for (index, allowed) in allowed.into_iter().enumerate() {
            let block = &body["messages"][0]["content"][index + 1];
            assert_eq!(
                block.to_string().contains("YWJj"),
                allowed,
                "{quirk:?} {block}"
            );
            if !allowed {
                assert!(block["text"].as_str().unwrap().contains("reason=carrier"));
            }
        }
        if quirk == MediaQuirk::Kimi || quirk == MediaQuirk::Openrouter || quirk == MediaQuirk::Meta
        {
            assert_eq!(
                body["messages"][0]["content"][4],
                json!({"type":"video_url", "video_url":{"url":"data:video/mp4;base64,YWJj"}})
            );
        }
    }
}

#[test]
fn openai_media_requires_an_exact_payload_profile_and_position() {
    let media = [reference(MediaKind::Image, "image/png")];
    for (id, family) in [
        (WireFamilyId::OpenAiChat, &OpenAiChat as &dyn WireFamily),
        (
            WireFamilyId::OpenAiResponses,
            &OpenAiResponses as &dyn WireFamily,
        ),
        (
            WireFamilyId::OpenAiResponsesChatgpt,
            &OpenAiResponsesChatgpt as &dyn WireFamily,
        ),
    ] {
        let mut frozen = frozen(id, MediaQuirk::Openai);
        let opts = request(&media, false);
        let mut foreign = media.clone();
        foreign[0].width = Some(2);
        for denied in [
            render(family, &frozen, &opts, &foreign, false),
            render(family, &frozen, &opts, &media, true),
        ] {
            assert!(!denied.to_string().contains("YWJj"));
        }
        frozen.media.as_mut().unwrap().modalities = InputModalities::default();
        let denied = render(family, &frozen, &opts, &media, false);
        assert!(denied.to_string().contains("reason=modality"));
        let denied = render(family, &frozen, &request(&media, true), &media, true);
        assert!(denied.to_string().contains("reason=modality"));
        frozen.media = None;
        assert_eq!(
            family.render_with_media(&frozen, &opts, &payloads(&media, false)),
            family.render(&frozen, &opts)
        );
        let mut assistant = opts.clone();
        assistant.prompt[0].role = Role::Assistant;
        assert!(!render(family, &frozen, &assistant, &media, false)
            .to_string()
            .contains("YWJj"));
    }
}

#[test]
fn responses_chatgpt_image_is_native_but_unevidenced_kinds_are_markers() {
    let frozen = frozen(WireFamilyId::OpenAiResponsesChatgpt, MediaQuirk::Openai);
    let media = [
        reference(MediaKind::Image, "image/webp"),
        reference(MediaKind::Document, "application/pdf"),
        reference(MediaKind::Video, "video/mp4"),
    ];
    for tool in [false, true] {
        let body = render(
            &OpenAiResponsesChatgpt,
            &frozen,
            &request(&media, tool),
            &media,
            tool,
        );
        let content = &body["input"][0][if tool { "output" } else { "content" }];
        assert_eq!(
            content[1],
            json!({"type":"input_image", "image_url":"data:image/webp;base64,YWJj"})
        );
        for index in [2, 3] {
            assert!(content[index]["text"]
                .as_str()
                .unwrap()
                .contains("reason=carrier"));
        }
    }
}

#[test]
fn openai_profile_mime_and_item_limits_are_enforced_before_encoding() {
    for (id, family) in [
        (WireFamilyId::OpenAiChat, &OpenAiChat as &dyn WireFamily),
        (
            WireFamilyId::OpenAiResponses,
            &OpenAiResponses as &dyn WireFamily,
        ),
    ] {
        let frozen = frozen(id, MediaQuirk::Openai);
        let mut media = [reference(MediaKind::Document, "application/pdf")];
        media[0].byte_size = 50_000_000;
        let body = render(family, &frozen, &request(&media, false), &media, false);
        assert!(body.to_string().contains("reason=too_large"));
        assert!(!body.to_string().contains("YWJj"));
        media[0].byte_size = 3;
        media[0].mime = "text/plain".into();
        let body = render(family, &frozen, &request(&media, false), &media, false);
        assert!(body.to_string().contains("reason=mime"));
        assert!(!body.to_string().contains("YWJj"));
    }
}

#[test]
fn responses_media_only_and_marker_only_tool_results_retain_all_parts() {
    let frozen = frozen(WireFamilyId::OpenAiResponses, MediaQuirk::Openai);
    let media = [reference(MediaKind::Image, "image/png")];
    for parts in [
        vec![
            ToolOutputPart::Media {
                media: media[0].clone()
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
                tool_call_id: ToolCallId("only".into()),
                is_error: true,
                output: ToolOutput::Parts {
                    parts: parts.clone(),
                },
            })],
        )]);
        let body = render(&OpenAiResponses, &frozen, &opts, &media, true);
        assert_eq!(body["input"][0]["call_id"], "only");
        assert_eq!(
            body["input"][0]["output"].as_array().unwrap().len(),
            parts.len()
        );
        let expected = if matches!(parts[0], ToolOutputPart::Text { .. }) {
            json!({"type":"input_text", "text":"capture failed"})
        } else {
            json!({"type":"input_image", "image_url":"data:image/png;base64,YWJj"})
        };
        assert_eq!(body["input"][0]["output"][0], expected);
    }
}
