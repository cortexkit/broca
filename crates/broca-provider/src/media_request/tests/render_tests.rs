use super::*;
use crate::{
    families::{frozen_config_for_family, AnthropicMessages, BedrockConverse},
    wire::WireFamily,
};
use broca_protocol::{
    tags::{number_message, render_tags_into, ComputedRules, TagRendering},
    CallOptions,
};

fn families() -> [(WireFamilyId, &'static dyn WireFamily); 2] {
    [
        (WireFamilyId::AnthropicMessages, &AnthropicMessages),
        (WireFamilyId::BedrockConverse, &BedrockConverse),
    ]
}

#[test]
fn native_requests_measure_complete_bytes_and_fail_closed_at_the_cap() {
    let fixture = Fixture::new();
    let mut person = fixture.person();
    person.content.insert(
        0,
        ContentBlock::Text {
            text: "large envelope".repeat(100),
        },
    );
    let prompt = vec![person];
    let session = fixture.session(&[start(&prompt), step(1)]);
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let mut profile = profile();
        if id == WireFamilyId::BedrockConverse {
            profile = select_media_profile(id, None, "model", profile.modalities, Some(1_000_000));
        }
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let opts = CallOptions::new(prompt.clone());
        let bytes = render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| {
            prompt.to_vec()
        })
        .unwrap();
        assert!(bytes.len() as u64 > bundle.encoded_media_bytes());
        frozen.media.as_mut().unwrap().request_cap_bytes = bytes.len() as u64;
        assert_eq!(
            render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| prompt
                .to_vec())
            .unwrap(),
            bytes
        );
        frozen.media.as_mut().unwrap().request_cap_bytes -= 1;
        assert_eq!(
            render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| prompt
                .to_vec())
            .unwrap_err()
            .code(),
            "request_too_large"
        );
    }
}

#[test]
fn tagged_text_image_text_stays_numbered_once_across_retry_and_replay() {
    let fixture = Fixture::new();
    let mut tool = fixture.tool("call");
    let ContentBlock::ToolResult(result) = &mut tool.content[0] else {
        unreachable!()
    };
    let ToolOutput::Parts { parts } = &mut result.output else {
        unreachable!()
    };
    parts.insert(
        0,
        ToolOutputPart::Text {
            text: "before".into(),
        },
    );
    parts.push(ToolOutputPart::Text {
        text: "after".into(),
    });
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let records = [start(&[]), step(1), tool_record(&tool), step(2)];
    let prompt = vec![tool];
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let profile = select_media_profile(
            id,
            Some(crate::media_profile::MediaQuirk::Anthropic),
            "model",
            profile().modalities,
            Some(1_000_000),
        );
        frozen.media = Some(profile.clone());
        let render_tags = |prompt: &[Message]| {
            let mut prompt = prompt.to_vec();
            render_tags_into(&mut prompt, TagRendering::default());
            prompt
        };
        let run = || {
            let session = fixture.session(&records);
            let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
            let opts = CallOptions::new(prompt.clone());
            let first =
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap();
            assert_eq!(
                first,
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap()
            );
            let body: serde_json::Value = serde_json::from_slice(&first).unwrap();
            let result = &body["messages"][0]["content"][0];
            let content = if id == WireFamilyId::AnthropicMessages {
                &result["content"]
            } else {
                &result["toolResult"]["content"]
            };
            assert_eq!(content[0]["text"], "§7§ before");
            assert_eq!(content[2]["text"], "after");
            assert_eq!(
                serde_json::to_string(&body).unwrap().matches("§7§").count(),
                1
            );
            assert!(serde_json::to_string(&content[1])
                .unwrap()
                .contains(&base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &fixture.bytes
                )));
            // Limit marking replaces only the render copy; the stored tag and later text stay put.
            bundle.mark(0, MediaMarkerReason::Aged);
            let aged =
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap();
            let aged = String::from_utf8(aged).unwrap();
            assert_eq!(aged.matches("§7§").count(), 1);
            assert!(aged.contains("reason=aged"));
            first
        };
        assert_eq!(run(), run());
    }
}

#[test]
fn image_only_tag_label_does_not_detach_the_prepared_payload() {
    let fixture = Fixture::new();
    let mut tool = fixture.tool("call");
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let prompt = vec![tool];
    let session = fixture.session(&[start(&[]), step(1), tool_record(&prompt[0]), step(2)]);
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let profile = select_media_profile(
            id,
            Some(crate::media_profile::MediaQuirk::Anthropic),
            "model",
            profile().modalities,
            Some(1_000_000),
        );
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let bytes = render_prepared(
            family,
            &frozen,
            &CallOptions::new(prompt.clone()),
            Some(&mut bundle),
            &|prompt| {
                let mut prompt = prompt.to_vec();
                render_tags_into(&mut prompt, TagRendering::default());
                prompt
            },
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let result = &body["messages"][0]["content"][0];
        let content = if id == WireFamilyId::AnthropicMessages {
            &result["content"]
        } else {
            &result["toolResult"]["content"]
        };
        assert_eq!(content.as_array().unwrap().len(), 2);
        assert!(content[0]["text"].as_str().unwrap().contains('7'));
        assert!(serde_json::to_string(&content[1])
            .unwrap()
            .contains(&base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &fixture.bytes
            )));
    }
}

#[test]
fn prepared_image_text_numbers_original_text_not_media_marker() {
    let fixture = Fixture::new();
    let mut tool = fixture.tool("call");
    let ContentBlock::ToolResult(result) = &mut tool.content[0] else {
        unreachable!()
    };
    let ToolOutput::Parts { parts } = &mut result.output else {
        unreachable!()
    };
    parts.push(ToolOutputPart::Text {
        text: "first exposed text".into(),
    });
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let prompt = vec![tool];
    let session = fixture.session(&[start(&[]), step(1), tool_record(&prompt[0]), step(2)]);
    let id = WireFamilyId::OpenAiChat;
    let mut frozen = frozen_config_for_family(id, "model");
    let profile = select_media_profile(
        id,
        Some(crate::media_profile::MediaQuirk::Openai),
        "model",
        profile().modalities,
        Some(1_000_000),
    );
    frozen.media = Some(profile.clone());
    let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
    assert!(
        bundle.media.is_empty(),
        "unverified Chat tool carriers use markers"
    );
    let bytes = render_prepared(
        &crate::families::OpenAiChat,
        &frozen,
        &CallOptions::new(prompt),
        Some(&mut bundle),
        &|prompt| {
            let mut tagged = prompt.to_vec();
            render_tags_into(&mut tagged, TagRendering::default());
            tagged
        },
    )
    .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let content = body["messages"][0]["content"].as_str().unwrap();
    assert!(content.contains("reason=carrier"), "{content}");
    assert!(content.contains("§7§ first exposed text"), "{content}");
    assert_eq!(content.matches("§7§").count(), 1);
}

#[test]
fn prepared_tool_tag_table_preserves_text_targets_and_image_labels_on_every_family() {
    let fixture = Fixture::new();
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        let family = crate::registry::wire_family(id).unwrap();
        let quirk = match id {
            WireFamilyId::AnthropicMessages => Some(crate::media_profile::MediaQuirk::Anthropic),
            WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses => {
                Some(crate::media_profile::MediaQuirk::Openai)
            }
            WireFamilyId::Gemini => Some(crate::media_profile::MediaQuirk::Google),
            _ => None,
        };
        for shape in ["text_image_text", "image_text", "image_empty", "image_only"] {
            for mode in ["profile", "unavailable", "aged", "request_cap"] {
                let mut tool = fixture.tool("call");
                let ContentBlock::ToolResult(result) = &mut tool.content[0] else {
                    unreachable!()
                };
                let ToolOutput::Parts { parts } = &mut result.output else {
                    unreachable!()
                };
                match shape {
                    "text_image_text" => {
                        parts.insert(
                            0,
                            ToolOutputPart::Text {
                                text: "first exposed text".into(),
                            },
                        );
                        parts.push(ToolOutputPart::Text {
                            text: "later text".into(),
                        });
                    }
                    "image_text" => parts.push(ToolOutputPart::Text {
                        text: "first exposed text".into(),
                    }),
                    "image_empty" => parts.push(ToolOutputPart::Text {
                        text: String::new(),
                    }),
                    _ => {}
                }
                number_message(
                    &mut tool,
                    ComputedRules {
                        render: true,
                        clean: false,
                    },
                    &mut 6,
                )
                .unwrap();
                let prompt = vec![tool];
                let session =
                    fixture.session(&[start(&[]), step(1), tool_record(&prompt[0]), step(2)]);
                let mut frozen = frozen_config_for_family(id, "gemini-3.0-flash");
                if id == WireFamilyId::GoogleCodeAssist {
                    frozen.project_id = Some("tag-fixture".into());
                }
                let mut profile = select_media_profile(
                    id,
                    quirk,
                    "gemini-3.0-flash",
                    profile().modalities,
                    Some(1_000_000),
                );
                if mode == "aged" {
                    profile.aging.byte_trigger = 1;
                    profile.aging.byte_target = 0;
                    profile.aging.token_trigger = u64::MAX;
                    profile.aging.token_target = u64::MAX;
                }
                frozen.media = (mode != "unavailable").then_some(profile.clone());
                let mut aging = MediaAging::default();
                aging.select_view(&profile, &session.bindings(&prompt));
                let mut bundle = prepare(&session, frozen.media.as_ref(), &prompt, &aging).unwrap();
                if mode == "request_cap" && !bundle.media.is_empty() {
                    bundle.mark(0, MediaMarkerReason::RequestCap);
                }
                let shown = bundle.media.len();
                let mut opts = CallOptions::new(prompt.clone());
                opts.step_context = Some(broca_protocol::StepContext {
                    run_id: "tags".into(),
                    step_index: 2,
                    session_address: None,
                    credential_id: None,
                    account_ref: None,
                    max_output_tokens: None,
                });
                let tags = |prompt: &[Message]| {
                    let mut tagged = prompt.to_vec();
                    render_tags_into(&mut tagged, TagRendering::default());
                    tagged
                };
                let bytes =
                    render_prepared(&*family, &frozen, &opts, Some(&mut bundle), &tags).unwrap();
                let rendered = String::from_utf8(bytes.clone()).unwrap();
                let expected = if shape == "text_image_text" || shape == "image_text" {
                    "§7§ first exposed text".to_string()
                } else {
                    "§7§ [image]".to_string()
                };
                assert!(
                    rendered.contains(&expected),
                    "{id:?}/{shape}/{mode}: {rendered}"
                );
                assert_eq!(rendered.matches("§7§").count(), 1, "{id:?}/{shape}/{mode}");
                if shown != 0 {
                    assert!(
                        rendered.contains(&STANDARD.encode(&fixture.bytes)),
                        "{id:?}/{shape}/{mode}: native payload remains attached"
                    );
                } else {
                    assert!(
                        !rendered.contains(&STANDARD.encode(&fixture.bytes)),
                        "{id:?}/{shape}/{mode}"
                    );
                }
                assert_eq!(
                    render_prepared(&*family, &frozen, &opts, Some(&mut bundle), &tags).unwrap(),
                    bytes,
                    "{id:?}/{shape}/{mode}: repeated render"
                );
                assert_eq!(
                    opts.prompt, prompt,
                    "tag and marker projection must not mutate stored input"
                );
            }
        }
    }
}

#[test]
fn media_free_marker_looking_text_keeps_legacy_render_bytes() {
    let fixture = Fixture::new();
    let literal =
        broca_protocol::content::media_not_shown_marker(&fixture.media, MediaMarkerReason::Carrier);
    let mut tool = fixture.tool("call");
    let ContentBlock::ToolResult(result) = &mut tool.content[0] else {
        unreachable!()
    };
    result.output = ToolOutput::Parts {
        parts: vec![
            ToolOutputPart::Text {
                text: literal.clone(),
            },
            ToolOutputPart::Text {
                text: "later text".into(),
            },
        ],
    };
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let prompt = vec![tool];
    let session = fixture.session(&[start(&[]), step(1), tool_record(&prompt[0]), step(2)]);
    let frozen = frozen_config_for_family(WireFamilyId::OpenAiChat, "model");
    let mut opts = CallOptions::new(prompt.clone());
    let tags = |prompt: &[Message]| {
        let mut tagged = prompt.to_vec();
        render_tags_into(&mut tagged, TagRendering::default());
        tagged
    };
    render_tags_into(&mut opts.prompt, TagRendering::default());
    let legacy = crate::families::OpenAiChat.render(&frozen, &opts);
    opts.prompt = prompt.clone();
    let mut prepared = prepare(&session, None, &prompt, &MediaAging::default()).unwrap();
    let actual = render_prepared(
        &crate::families::OpenAiChat,
        &frozen,
        &opts,
        Some(&mut prepared),
        &tags,
    )
    .unwrap();
    assert_eq!(actual, legacy);
    assert!(String::from_utf8(actual)
        .unwrap()
        .contains(&format!("§7§ {literal}")));
}

#[test]
fn native_media_provenance_uses_the_same_bytes_and_durable_source_for_every_family() {
    let fixture = Fixture::new();
    let mut person = fixture.person();
    person.ordinal = Some(42);
    let mut history: Vec<_> = (0..42)
        .map(|ordinal| {
            let mut message = Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: format!("older input {ordinal}"),
                }],
            );
            message.ordinal = Some(ordinal);
            message
        })
        .collect();
    history.push(person.clone());
    let session = fixture.session(&[start(&history), step(1)]);
    let prompt = vec![person];
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        let family = crate::registry::wire_family(id).unwrap();
        let mut frozen = frozen_config_for_family(id, "model");
        frozen.project_id = Some("synthetic-project".into());
        let profile =
            select_media_profile(id, None, "model", profile().modalities, Some(1_000_000));
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let mut opts = CallOptions::new(prompt.clone());
        opts.step_context = Some(broca_protocol::StepContext {
            run_id: "media-provenance".into(),
            step_index: 1,
            max_output_tokens: None,
            session_address: None,
            credential_id: None,
            account_ref: None,
        });
        let plain =
            render_prepared(&*family, &frozen, &opts, Some(&mut bundle), &|p| p.to_vec()).unwrap();
        let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
        let traced = render_prepared_with_provenance(
            &*family,
            &frozen,
            &opts,
            Some(&mut bundle),
            &|p| p.to_vec(),
            Some(&mut provenance),
        )
        .unwrap();
        assert_eq!(plain, traced, "{id:?}");
        assert!(
            (0..provenance.message_count())
                .any(|index| provenance.message_ordinal(index, Some(0)) == Some(42)),
            "{id:?}: source ordinal missing"
        );
    }
}

#[test]
fn frozen_profiles_leave_text_only_request_bytes_unchanged_in_every_family() {
    use crate::media_profile::MediaQuirk;
    use broca_protocol::{
        CallOptions, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
    };
    let fixture = Fixture::new();
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Inspect ordinary text".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("text-call".into()),
                tool_name: "inspect".into(),
                input: json!({}),
                thought_signature: None,
            })],
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("text-call".into()),
                output: ToolOutput::Text {
                    text: "[image/png not shown to the model] is ordinary text".into(),
                },
                is_error: false,
            })],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Continue".into(),
            }],
        ),
    ]);
    opts.step_context = Some(broca_protocol::StepContext {
        run_id: "plain-profile".into(),
        step_index: 2,
        session_address: None,
        credential_id: None,
        account_ref: None,
        max_output_tokens: None,
    });
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        let mut legacy = crate::families::frozen_config_for_family(id, "gemini-3.0-flash");
        legacy.media = None;
        if id == WireFamilyId::GoogleCodeAssist {
            legacy.project_id = Some("synthetic-project".into());
        }
        let family = crate::registry::wire_family(id).unwrap();
        let expected = family.render(&legacy, &opts);
        let mut with = legacy.clone();
        let quirk = match id {
            WireFamilyId::AnthropicMessages => Some(MediaQuirk::Anthropic),
            WireFamilyId::OpenAiChat | WireFamilyId::OpenAiResponses => Some(MediaQuirk::Openai),
            WireFamilyId::Gemini => Some(MediaQuirk::Google),
            _ => None,
        };
        with.media = Some(select_media_profile(
            id,
            quirk,
            &with.wire_model_id,
            InputModalities::from_value(&json!(["text", "image", "pdf", "audio", "video"])),
            Some(128000),
        ));
        let live = crate::media_request::LiveMedia::new(
            fixture.store.clone(),
            Box::new(|_| panic!("text-only rendering must not read WAL or media captures")),
        );
        let actual = live
            .render(&serde_json::to_value(with).unwrap(), &opts, false, true)
            .unwrap();
        assert_eq!(
            actual, expected,
            "{id:?}: profile metadata changed text-only request bytes"
        );
    }
}
