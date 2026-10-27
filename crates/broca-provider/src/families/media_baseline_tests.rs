use super::*;
use crate::{spec::WireFamilyId, wire::WireFamily};
use broca_protocol::{
    CallOptions, ContentBlock, MediaKind, MediaRef, Message, Role, ToolCall, ToolCallId,
    ToolOutput, ToolOutputPart, ToolResult,
};
use serde_json::json;

const MARKER: &str =
    "[media not shown: kind=image mime=image/png bytes=42 sha256=0123456789ab reason=carrier]";

fn media() -> MediaRef {
    MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 42,
        sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        width: Some(3),
        height: Some(2),
    }
}

fn options(messages: Vec<Message>) -> CallOptions {
    let mut options = CallOptions::new(messages);
    options.step_context = Some(broca_protocol::StepContext {
        run_id: "r".into(),
        step_index: 1,
        session_address: None,
        credential_id: None,
        account_ref: None,
    });
    options
}

fn families() -> Vec<(WireFamilyId, Box<dyn WireFamily>)> {
    vec![
        (WireFamilyId::AnthropicMessages, Box::new(AnthropicMessages)),
        (WireFamilyId::OpenAiChat, Box::new(OpenAiChat)),
        (WireFamilyId::OpenAiResponses, Box::new(OpenAiResponses)),
        (
            WireFamilyId::OpenAiResponsesChatgpt,
            Box::new(OpenAiResponsesChatgpt),
        ),
        (WireFamilyId::Gemini, Box::new(Gemini)),
        (WireFamilyId::GoogleCodeAssist, Box::new(GoogleCodeAssist)),
        (WireFamilyId::BedrockConverse, Box::new(BedrockConverse)),
    ]
}

#[test]
fn every_family_renders_parts_as_ordered_carrier_markers() {
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        frozen.project_id = Some("project".into());
        for is_error in [false, true] {
            for (parts, expected) in [
                (
                    vec![
                        ToolOutputPart::Text {
                            text: "before".into(),
                        },
                        ToolOutputPart::Media { media: media() },
                        ToolOutputPart::Text {
                            text: "after".into(),
                        },
                    ],
                    format!("before\n{MARKER}\nafter"),
                ),
                (
                    vec![
                        ToolOutputPart::Media { media: media() },
                        ToolOutputPart::Media { media: media() },
                    ],
                    format!("{MARKER}\n{MARKER}"),
                ),
                (
                    vec![ToolOutputPart::Text {
                        text: "capture failed".into(),
                    }],
                    "capture failed".into(),
                ),
            ] {
                let transcript = |output| {
                    options(vec![
                        Message::new(
                            Role::Assistant,
                            vec![ContentBlock::ToolCall(ToolCall {
                                tool_call_id: ToolCallId("call".into()),
                                tool_name: "screenshot".into(),
                                input: json!({}),
                                thought_signature: None,
                            })],
                        ),
                        Message::new(
                            Role::Tool,
                            vec![ContentBlock::ToolResult(ToolResult {
                                tool_call_id: ToolCallId("call".into()),
                                output,
                                is_error,
                            })],
                        ),
                    ])
                };
                let expected =
                    family.render(&frozen, &transcript(ToolOutput::Text { text: expected }));
                let actual = family.render(&frozen, &transcript(ToolOutput::Parts { parts }));
                assert_eq!(actual, expected, "{id:?} is_error={is_error}");
            }
        }
    }
}

#[test]
fn every_family_renders_person_media_as_marker_without_reordering_text() {
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        frozen.project_id = Some("project".into());
        let input = |middle| {
            options(vec![Message::new(
                Role::User,
                vec![
                    ContentBlock::Text {
                        text: "before".into(),
                    },
                    middle,
                    ContentBlock::Text {
                        text: "after".into(),
                    },
                ],
            )])
        };
        assert_eq!(
            family.render(&frozen, &input(ContentBlock::Media(media()))),
            family.render(
                &frozen,
                &input(ContentBlock::Text {
                    text: MARKER.into()
                })
            ),
            "{id:?}"
        );
    }
}
