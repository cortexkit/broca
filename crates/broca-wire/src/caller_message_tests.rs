use super::*;
use broca_protocol::content::{ContentBlock, Role, TextProviderFormat, TextProviderState};
use serde_json::json;

fn read_message(content: Vec<ContentBlock>) -> SessionReadMessage {
    SessionReadMessage {
        send_id: None,
        speaks_for: None,
        notice: false,
        sender: None,
        notices: Vec::new(),
        created_at: None,
        completed_at: None,
        reasoning_summaries: Vec::new(),
        client_message_id: None,
        tool_times: Vec::new(),
        rich_ui: None,
        model_fallback: None,
        written_at: None,
        delivered_as: None,
        part_keys: None,
        ordinal: 4,
        mid: "m3".into(),
        message: broca_protocol::Message::new(Role::Assistant, content),
        run: None,
        tool_calls: Vec::new(),
        described: None,
        digest: None,
        part_digests: None,
    }
}

#[test]
fn media_refs_keep_large_payloads_and_flattened_tool_results() {
    use broca_protocol::{
        MediaKind, MediaRef, ToolCall, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
    };
    let media = MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 69,
        sha256: "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640".into(),
        width: Some(1),
        height: Some(1),
    };
    let payload = "x".repeat(300_000);
    let original = read_message(vec![
        ContentBlock::Text {
            text: payload.clone().into(),
        },
        ContentBlock::Media(media.clone()),
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call".into()),
            tool_name: "work".into(),
            input: json!({"large":payload}).into(),
            thought_signature: None,
            provider_state: None,
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call".into()),
            is_error: false,
            output: ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: payload.clone().into(),
                    },
                    ToolOutputPart::Media { media },
                ],
            },
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("text-only".into()),
            is_error: false,
            output: ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: payload.clone().into(),
                    },
                    ToolOutputPart::Text {
                        text: "tail".into(),
                    },
                ],
            },
        }),
        ContentBlock::Reasoning {
            text: payload.clone().into(),
            signature: Some("signature".into()),
        },
    ]);
    let inline = serde_json::to_value(&original).unwrap();
    let mut blocks = original.clone();
    blocks.show_media_blocks_with_identity();
    let blocks = serde_json::to_value(blocks).unwrap();
    let mut refs = original.clone();
    refs.show_media_refs();
    assert_eq!(refs.message, original.message);
    let refs = serde_json::to_value(refs).unwrap();
    let mut expected = inline["message"].clone();
    expected["content"][1] = json!({"type":"media_ref","part":"m3#1","kind":"image","mime":"image/png","byte_size":69,"sha256":"b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640","width":1,"height":1});
    assert_eq!(refs["message"], expected);
    assert_eq!(refs["message"]["content"][0]["text"], payload);
    assert_eq!(refs["digest"], blocks["digest"]);
    assert_eq!(refs["part_digests"], blocks["part_digests"]);
    for index in [0, 2, 3, 4, 5] {
        assert_eq!(
            refs["message"]["content"][index],
            inline["message"]["content"][index]
        );
    }
    let mut text_only = read_message(vec![original.message.content[4].clone()]);
    let default_bytes = serde_json::to_vec(&text_only).unwrap();
    text_only.show_media_refs();
    assert_eq!(serde_json::to_vec(&text_only).unwrap(), default_bytes);
    let mut media_tool_result = read_message(vec![original.message.content[3].clone()]);
    let inline_bytes = serde_json::to_vec(&media_tool_result).unwrap();
    media_tool_result.show_media_refs();
    assert_eq!(
        serde_json::to_vec(&media_tool_result).unwrap(),
        inline_bytes
    );
    // With `tool_media: "refs"` the media-bearing result keeps its parts, its media served
    // as blocks serves it; the text-only result and every other part are as media_refs.
    let mut tool_refs = original.clone();
    tool_refs.show_media_refs_with_tool_media();
    assert_eq!(tool_refs.message, original.message);
    let tool_refs = serde_json::to_value(tool_refs).unwrap();
    let mut expected_tool = refs["message"].clone();
    expected_tool["content"][3] = blocks["message"]["content"][3].clone();
    assert_eq!(tool_refs["message"], expected_tool);
    assert_eq!(
        tool_refs["message"]["content"][3]["output"]["parts"][1]["media"],
        json!({"kind":"image","mime":"image/png","byte_size":69,"sha256":"b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640","width":1,"height":1}),
        "{tool_refs}"
    );
    assert_eq!(tool_refs["digest"], blocks["digest"]);
    assert_eq!(tool_refs["part_digests"], blocks["part_digests"]);
    // A message whose only media is in a tool result serves its output as blocks would,
    // and carries no identity a media_refs read without the opt-in leaves out.
    let mut lone = read_message(vec![original.message.content[3].clone()]);
    lone.show_media_refs_with_tool_media();
    let lone = serde_json::to_value(lone).unwrap();
    let mut lone_blocks = read_message(vec![original.message.content[3].clone()]);
    lone_blocks.show_media_blocks_with_identity();
    let lone_blocks = serde_json::to_value(lone_blocks).unwrap();
    assert_eq!(lone["message"], lone_blocks["message"]);
    let lone_refs = serde_json::to_value(&media_tool_result).unwrap();
    for member in ["digest", "part_digests"] {
        assert_eq!(lone.get(member), lone_refs.get(member), "{member}: {lone}");
    }
    // Without media in any tool result the bytes are media_refs' bytes.
    let mut plain = read_message(vec![original.message.content[4].clone()]);
    plain.show_media_refs_with_tool_media();
    assert_eq!(serde_json::to_vec(&plain).unwrap(), default_bytes);
    let mut descriptors = original;
    descriptors.describe().unwrap();
    let descriptors = serde_json::to_value(descriptors).unwrap();
    assert_eq!(
        refs["message"]["content"][1],
        descriptors["message"]["content"][1]
    );
}

#[test]
fn default_media_read_uses_text_markers_without_changing_the_stored_message() {
    use broca_protocol::{MediaKind, MediaRef, ToolCallId, ToolOutput, ToolOutputPart, ToolResult};
    let media = MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 69,
        sha256: "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640".into(),
        width: Some(1),
        height: Some(1),
    };
    let marker = "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=not_requested]";
    let row = read_message(vec![
        ContentBlock::Text {
            text: "before".into(),
        },
        ContentBlock::Media(media.clone()),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call".into()),
            is_error: true,
            output: ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: "left".into(),
                    },
                    ToolOutputPart::Media { media },
                    ToolOutputPart::Text {
                        text: "right".into(),
                    },
                ],
            },
        }),
    ]);
    let stored = row.message.clone();
    let value = serde_json::to_value(&row).unwrap();
    assert_eq!(
        value["message"]["content"][1],
        json!({"type":"text","text":marker})
    );
    assert_eq!(
        value["message"]["content"][2]["output"],
        json!({"kind":"text","text":format!("left\n{marker}\nright")})
    );
    assert_eq!(value["message"]["content"][2]["is_error"], true);
    assert_eq!(row.message, stored);
}

/// The member names of a JSON object, sorted.
fn members(value: &serde_json::Value) -> Vec<String> {
    let mut names: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
    names.sort();
    names
}

#[test]
fn default_read_envelope_names_every_known_stored_role() {
    for (role, expected) in [
        (broca_protocol::Role::System, "system"),
        (broca_protocol::Role::User, "user"),
        (broca_protocol::Role::Assistant, "assistant"),
        (broca_protocol::Role::Tool, "tool"),
    ] {
        let mut entry = read_message(vec![ContentBlock::Text {
            text: "stored".into(),
        }]);
        entry.message.role = role;
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(body["role"], expected);
        assert_eq!(body["message"]["role"], expected);
        assert!(body.get("digest").is_none());
        assert!(body.get("part_digests").is_none());
    }
}

/// A consumer that re-encodes read messages byte-exactly decodes them strictly, so the
/// default read pins its member set. Beside the members served before, the envelope now
/// names the stored message's role. Computed tag rules add nothing to an entry, its message
/// or any block; only a descriptors read adds `digest` and `part_digests`.
#[test]
fn the_default_read_shape_adds_only_the_known_role_to_legacy_members() {
    use broca_protocol::content::{EncryptedReasoningFormat, ToolCall, ToolOutput, ToolResult};
    let result = |id: &str, output: ToolOutput| {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: broca_protocol::ToolCallId(id.into()),
            output,
            is_error: false,
        })
    };
    let content = vec![
        ContentBlock::Text {
            text: "§3§ plain".into(),
        },
        ContentBlock::TextWithState {
            text: "stateful".into(),
            provider_state: TextProviderState {
                format: TextProviderFormat::OpenAiResponses,
                data: "{}".into(),
            },
        },
        ContentBlock::Reasoning {
            text: "thought".into(),
            signature: Some("sig".into()),
        },
        ContentBlock::RedactedReasoning {
            data: "opaque".into(),
            provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
        },
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: broca_protocol::ToolCallId("c".into()),
            tool_name: "t".into(),
            input: json!({"a": 1}).into(),
            thought_signature: Some("ts".into()),
            provider_state: None,
        }),
        result("c", ToolOutput::Text { text: "out".into() }),
        result(
            "d",
            ToolOutput::Json {
                value: json!({}).into(),
            },
        ),
        result("e", ToolOutput::error("no")),
        result(
            "f",
            ToolOutput::Denied {
                reason: "no".into(),
            },
        ),
    ];
    let mut entry = computed(read_message(content.clone()), true);
    entry.message.origin = Some(broca_protocol::ModelHandle::new("p", "m"));
    let body = serde_json::to_value(&entry).unwrap();
    assert_eq!(
        members(&body),
        ["message", "mid", "ordinal", "part_keys", "role"]
    );
    assert_eq!(members(&body["message"]), ["content", "origin", "role"]);
    let blocks: Vec<Vec<String>> = body["message"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .map(members)
        .collect();
    let expected: Vec<Vec<&str>> = vec![
        vec!["text", "type"],
        vec!["text", "type"],
        vec!["signature", "text", "type"],
        vec!["data", "provider_format", "type"],
        vec![
            "input",
            "thought_signature",
            "tool_call_id",
            "tool_name",
            "type",
        ],
        vec!["is_error", "output", "tool_call_id", "type"],
        vec!["is_error", "output", "tool_call_id", "type"],
        vec!["is_error", "output", "tool_call_id", "type"],
        vec!["is_error", "output", "tool_call_id", "type"],
    ];
    assert_eq!(blocks, expected);
    let outputs: Vec<Vec<String>> = body["message"]["content"].as_array().unwrap()[5..]
        .iter()
        .map(|block| members(&block["output"]))
        .collect();
    assert_eq!(
        outputs,
        [
            vec!["kind", "text"],
            vec!["kind", "value"],
            vec!["kind", "message"],
            vec!["kind", "reason"]
        ]
    );
    // The same message without the computed rules serializes to the same members.
    let plain = serde_json::to_value(read_message(content)).unwrap();
    assert_eq!(
        members(&plain["message"]["content"][0]),
        members(&body["message"]["content"][0])
    );
    assert!(body.get("tags").is_none());
    assert_eq!(body["part_keys"].as_array().unwrap().len(), blocks.len());
}

/// Number `entry`'s message under the computed rules of a tag-rendering run, cleaning the
/// model's text (`clean`) or not.
fn computed(mut entry: SessionReadMessage, clean: bool) -> SessionReadMessage {
    let mut high = 0;
    broca_protocol::tags::number_message(
        &mut entry.message,
        broca_protocol::tags::ComputedRules {
            render: true,
            clean,
        },
        &mut high,
    )
    .unwrap();
    entry.part_keys = Some(part_keys(entry.ordinal - 1, &entry.message));
    entry
}

/// Anthropic's server-side fallback handoff reaches a reader as the stored part, unchanged,
/// at its own position: it keeps its part key and row, and never takes a tag number.
#[test]
fn a_read_passes_a_server_side_fallback_part_through_unchanged() {
    let content = vec![
        ContentBlock::Text {
            text: "Let me".into(),
        },
        ContentBlock::ProviderFallback {
            from_model: "claude-opus-5-5".into(),
            to_model: "claude-opus-4-8".into(),
            raw: None,
        },
        ContentBlock::Text {
            text: " check.".into(),
        },
    ];
    for entry in [
        read_message(content.clone()),
        computed(read_message(content), true),
    ] {
        let body = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            body["message"]["content"][1],
            json!({"type": "provider_fallback", "from_model": "claude-opus-5-5", "to_model": "claude-opus-4-8"})
        );
        assert!(body.get("tags").is_none());
        if let Some(keys) = body["part_keys"].as_array() {
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[1], "m3#1");
        }
    }
}

/// A read shows the model's text cleaned of tag notation when the run that wrote it cleans,
/// as written otherwise (an untagged run, a structured-output run's final text); the stored
/// message keeps the raw text either way.
#[test]
fn a_read_cleans_the_model_text_only_where_its_run_cleans() {
    let raw = "§3§ hi as §1§ says";
    let content = vec![ContentBlock::Text { text: raw.into() }];
    let cleaned = computed(read_message(content.clone()), true);
    let body = serde_json::to_value(&cleaned).unwrap();
    assert_eq!(body["message"]["content"][0]["text"], "hi as  says");
    assert_eq!(body["part_keys"], json!(["m3#0"]));
    assert!(body.get("tags").is_none());
    assert_eq!(cleaned.message.content, content, "stored text stays raw");
    let raw_kept = computed(read_message(content.clone()), false);
    let body = serde_json::to_value(&raw_kept).unwrap();
    assert_eq!(body["message"]["content"][0]["text"], raw);
}

/// A descriptors read serves the full inline message's content identities and replaces a
/// payload over the inline threshold in place by a ref; small payloads and the message's
/// structure stay as an inline read serves them.
#[test]
fn a_descriptors_read_describes_large_payloads_in_place_with_inline_identities() {
    use cortexkit_role_llm_runner::read::descriptors::PART_INLINE_MAX;
    let large = "x".repeat(PART_INLINE_MAX + 1);
    let mut entry = read_message(vec![
        ContentBlock::Text {
            text: "small".into(),
        },
        ContentBlock::Text {
            text: large.clone().into(),
        },
    ]);
    let inline = serde_json::to_value(&entry).unwrap();
    entry.describe().unwrap();
    let body = serde_json::to_value(&entry).unwrap();
    assert_eq!(
        body["digest"],
        json!(cortexkit_role_llm_runner::content_digest::content_digest(
            &inline["message"]
        ))
    );
    assert_eq!(
        body["part_digests"][1]["digest"],
        json!(cortexkit_role_llm_runner::content_digest::content_digest(
            &inline["message"]["content"][1]
        ))
    );
    assert_eq!(body["part_digests"][0]["part"], "m3#0");
    assert_eq!(body["message"]["content"][0]["text"], "small");
    let reference = &body["message"]["content"][1]["text"];
    assert_eq!(reference["type"], "text_ref");
    assert_eq!(reference["byte_size"], json!(large.len()));
    assert!(reference.get("part").is_none(), "a nested ref has no key");
    cortexkit_role_llm_runner::read::descriptors::validate_references(&body["message"]).unwrap();
    // An inline read of the same entry is unchanged.
    let mut again = read_message(entry.message.content.clone());
    again.ordinal = entry.ordinal;
    assert_eq!(serde_json::to_value(&again).unwrap(), inline);
}

/// Opaque keys sit beside the unchanged message; the legacy tag ledger is not a read row.
#[test]
fn tagged_entries_expose_opaque_keys_without_legacy_tag_rows() {
    let content = vec![
        ContentBlock::Text {
            text: "first".into(),
        },
        ContentBlock::Text {
            text: "second".into(),
        },
    ];
    let mut plain = read_message(content.clone());
    plain.part_keys = Some(part_keys(plain.ordinal - 1, &plain.message));
    let mut tagged = read_message(content);
    tagged.message.set_tags(Some(std::sync::Arc::new(
        broca_protocol::tags::PartTags::all([7, 8]),
    )));
    tagged.part_keys = Some(part_keys(tagged.ordinal - 1, &tagged.message));

    let plain_json = serde_json::to_value(&plain).unwrap();
    let tagged_json = serde_json::to_value(&tagged).unwrap();
    assert_eq!(plain_json["part_keys"], json!(["m3#0", "m3#1"]));
    assert_eq!(tagged_json["message"], plain_json["message"]);
    assert_eq!(
        serde_json::to_string(&tagged).unwrap(),
        r#"{"ordinal":4,"mid":"m3","message":{"role":"assistant","content":[{"type":"text","text":"first"},{"type":"text","text":"second"}]},"part_keys":["m3#0","m3#1"],"role":"assistant"}"#
    );
    let decoded: SessionReadMessage = serde_json::from_value(tagged_json).unwrap();
    assert_eq!(decoded.part_keys, tagged.part_keys);
}

#[test]
fn part_keys_do_not_depend_on_tag_ledger_length() {
    let mut entry = read_message(vec![
        ContentBlock::Text {
            text: "first".into(),
        },
        ContentBlock::Text {
            text: "second".into(),
        },
    ]);
    for (parts, expected) in [
        (vec![Some(7)], json!(["m3#0", "m3#1"])),
        (vec![Some(7), None, Some(9)], json!(["m3#0", "m3#1"])),
    ] {
        entry.message.set_tags(Some(std::sync::Arc::new(
            broca_protocol::tags::PartTags::new(parts),
        )));
        assert_eq!(
            serde_json::to_value(part_keys(entry.ordinal - 1, &entry.message)).unwrap(),
            expected
        );
    }
}

/// A caller reads the transcript's text, never the provider bytes kept to send back. A
/// worker's final reply carrying a ChatGPT phase must reach the caller as plain text, or a
/// parser looking for its text sees a serialized envelope instead.
#[test]
fn text_with_provider_state_reaches_callers_as_plain_text() {
    let message = read_message(vec![
        ContentBlock::Text {
            text: "before ".into(),
        },
        ContentBlock::TextWithState {
            text: "<result>done</result>".into(),
            provider_state: TextProviderState {
                format: TextProviderFormat::OpenAiResponsesChatgpt,
                data: r#"{"id":"msg_1","phase":"final_answer"}"#.into(),
            },
        },
    ]);
    let wire = serde_json::to_value(&message).unwrap();
    assert_eq!(
        wire["message"]["content"],
        json!([
            {"type": "text", "text": "before "},
            {"type": "text", "text": "<result>done</result>"},
        ])
    );
    assert!(!wire.to_string().contains("provider_state"));
    assert!(!wire.to_string().contains("final_answer"));
}

#[test]
fn borrowed_stateful_message_keeps_cache_and_origin_metadata() {
    let mut message = broca_protocol::Message::assistant_from(
        vec![ContentBlock::TextWithState {
            text: "final text".into(),
            provider_state: TextProviderState {
                format: TextProviderFormat::OpenAiResponsesChatgpt,
                data: "opaque".into(),
            },
        }],
        broca_protocol::ModelHandle::new("openai", "gpt-test"),
    );
    message.cache_prefix_blocks = Some(1);
    let response = SessionReadMessage {
        send_id: None,
        speaks_for: None,
        notice: false,
        sender: None,
        notices: Vec::new(),
        created_at: None,
        completed_at: None,
        reasoning_summaries: Vec::new(),
        client_message_id: None,
        tool_times: Vec::new(),
        rich_ui: None,
        model_fallback: None,
        written_at: None,
        delivered_as: None,
        part_keys: None,
        ordinal: 0,
        mid: "m0".into(),
        message,
        run: None,
        tool_calls: Vec::new(),
        described: None,
        digest: None,
        part_digests: None,
    };

    let wire = serde_json::to_value(&response).unwrap();
    assert_eq!(wire["message"]["cache_prefix_blocks"], 1);
    assert_eq!(wire["message"]["origin"]["provider_module_id"], "openai");
    assert_eq!(wire["message"]["origin"]["model_id"], "gpt-test");
    assert_eq!(
        wire["message"]["content"],
        json!([{"type": "text", "text": "final text"}])
    );
}

/// Messages without provider state serialize exactly as the canonical message does, so the
/// conversion never moves bytes of an ordinary page.
#[test]
fn messages_without_provider_state_are_unchanged() {
    let message = read_message(vec![ContentBlock::Text {
        text: "plain".into(),
    }]);
    assert_eq!(
        serde_json::to_value(&message).unwrap()["message"],
        serde_json::to_value(&message.message).unwrap()
    );
}

/// A summary-bearing OpenAI Responses reasoning item is written exactly as stored, with no
/// derived readable part. Consumers line part-level data up with the stored block positions,
/// so an inserted block would shift every later part away from its stored index.
#[test]
fn responses_reasoning_summary_is_read_as_the_stored_blocks_only() {
    let item = r#"{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"**Planning** check inputs"}],"encrypted_content":"cipher"}"#;
    let message = read_message(vec![
        ContentBlock::RedactedReasoning {
            data: item.into(),
            provider_format: Some(broca_protocol::EncryptedReasoningFormat::OpenAiResponses),
        },
        ContentBlock::Text {
            text: "All good.".into(),
        },
    ]);
    assert_eq!(
        serde_json::to_value(&message).unwrap()["message"]["content"],
        json!([
            {"type": "redacted_reasoning", "data": item, "provider_format": "open_ai_responses"},
            {"type": "text", "text": "All good."},
        ])
    );
}
