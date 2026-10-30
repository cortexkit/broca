use super::*;
use broca_protocol::{MediaKind, MediaMarkerReason, MediaRef, ToolOutputPart};
use cortexkit_role_llm_runner::read::MediaMode;
use serde_json::{json, Value};

fn image() -> MediaRef {
    MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 69,
        sha256: "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640".into(),
        width: Some(1),
        height: Some(1),
    }
}

fn mixed_parts(text: &str) -> Vec<ToolOutputPart> {
    vec![
        ToolOutputPart::Text { text: text.into() },
        ToolOutputPart::Media { media: image() },
        ToolOutputPart::Text {
            text: "after".into(),
        },
    ]
}

fn seed(key: &BindIdentity, parts: Vec<ToolOutputPart>) -> Vec<WalRecord> {
    let mut records = dispatch_module_episode(key, "media", "call-1");
    if let WalRecord::RunStarted { config, input, .. } = &mut records[0] {
        config.render_tags = true;
        config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
        Arc::make_mut(input)[0]
            .content
            .push(ContentBlock::Media(image()));
    }
    if let WalRecord::ToolResult { result, .. } = &mut records[4] {
        Arc::make_mut(result).output = ToolOutput::Parts { parts };
        Arc::make_mut(result).is_error = true;
    }
    records
}

async fn read(rig: &Rig, key: &BindIdentity, mode: MediaMode) -> broca_wire::SessionReadResponse {
    let mut request = read_request(key, Some(0), Some(10));
    request.media = mode;
    ServeHandler::read(&rig.registry, request)
        .await
        .expect("session.read")
}

async fn notice_matches_descriptors(name: &str, state: Option<broca_protocol::TextProviderState>) {
    use broca_core::compaction::{
        view::{next_notice, NoticeFacts},
        CompactionState,
    };

    let rig = Rig::new(name, Arc::new(NoopExec));
    let key = rig.key(name);
    let mut records = seed(&key, mixed_parts("before"));
    let WalRecord::ModelStepFinished {
        assistant_message, ..
    } = &mut records[7]
    else {
        panic!("final assistant message");
    };
    let text = "§99§ done".to_owned();
    Arc::make_mut(assistant_message).content = vec![match state {
        Some(provider_state) => ContentBlock::TextWithState {
            text,
            provider_state,
        },
        None => ContentBlock::Text { text },
    }];
    write_legacy_seed(&rig.wal_store, &key, &records);

    // Read native rows through the same snapshot as descriptors, not caller JSON decoded back
    // into Message: decoding would already have erased the provider-only text state.
    let native = read(&rig, &key, MediaMode::Blocks).await;
    let caller = serde_json::to_value(&native).unwrap();
    let descriptors = serde_json::to_value(read(&rig, &key, MediaMode::Descriptors).await).unwrap();
    assert_eq!(
        native.messages.len(),
        4,
        "media input, call, result, answer"
    );
    for (entry, described) in native
        .messages
        .iter()
        .zip(descriptors["messages"].as_array().unwrap())
    {
        let notice = next_notice(
            &CompactionState::default(),
            std::slice::from_ref(&entry.message),
            NoticeFacts {
                session_ref: "0123456789abcdef0123456789abcdef".into(),
                lineage_id: "lineage",
                est_tokens: 10,
                model: name,
                window: None,
                usage: None,
                events: vec![],
                seq_floor: 0,
            },
        )
        .unwrap();
        let headers = notice.to_headers();
        assert_eq!(headers["through_ordinal"], entry.ordinal.to_string());
        assert_eq!(described["ordinal"], entry.ordinal);
        assert_eq!(
            json!(headers["through_digest"]),
            described["digest"],
            "{name}: notice and descriptors identity for ordinal {}",
            entry.ordinal
        );
        let served = &caller["messages"][entry.ordinal as usize]["message"];
        assert_eq!(
            described["digest"],
            cortexkit_role_llm_runner::content_digest::content_digest(served),
            "identity must hash the independently serialized caller message, not native state"
        );
        for (index, part) in served["content"].as_array().unwrap().iter().enumerate() {
            assert_eq!(
                described["part_digests"][index]["digest"],
                cortexkit_role_llm_runner::content_digest::content_digest(part)
            );
        }
    }
    assert_eq!(
        descriptors["messages"][3]["message"]["content"],
        json!([{"type":"text","text":"done"}]),
        "the writing run cleans tags and callers never see provider state"
    );
    materialize_all(&rig, &key);
    assert_eq!(
        serde_json::to_value(read(&rig, &key, MediaMode::Descriptors).await).unwrap(),
        descriptors,
        "materialization preserves caller content identity"
    );
}

#[tokio::test]
async fn notice_digest_matches_descriptors_openai_responses() {
    notice_matches_descriptors(
        "responses",
        Some(broca_protocol::TextProviderState {
            format: broca_protocol::TextProviderFormat::OpenAiResponses,
            data: r#"{"id":"msg_test"}"#.into(),
        }),
    )
    .await;
}

#[tokio::test]
async fn notice_digest_matches_descriptors_chatgpt() {
    notice_matches_descriptors(
        "chatgpt",
        Some(broca_protocol::TextProviderState {
            format: broca_protocol::TextProviderFormat::OpenAiResponsesChatgpt,
            data: r#"{"id":"msg_test","phase":"final_answer"}"#.into(),
        }),
    )
    .await;
}

#[tokio::test]
async fn notice_digest_matches_descriptors_gemini() {
    notice_matches_descriptors(
        "gemini",
        Some(broca_protocol::TextProviderState {
            format: broca_protocol::TextProviderFormat::Gemini,
            data: "thought-signature".into(),
        }),
    )
    .await;
}

#[tokio::test]
async fn notice_digest_matches_descriptors_code_assist() {
    notice_matches_descriptors(
        "code-assist",
        Some(broca_protocol::TextProviderState {
            format: broca_protocol::TextProviderFormat::GoogleCodeAssist,
            data: "thought-signature".into(),
        }),
    )
    .await;
}

#[tokio::test]
async fn notice_digest_matches_descriptors_plain_text() {
    notice_matches_descriptors("plain-text", None).await;
}

// Deliberately closed enums model a reader deployed before media blocks and Parts existed.
#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LegacyBlock {
    Text { text: String },
    ToolCall { tool_call_id: String },
    ToolResult { output: LegacyOutput },
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LegacyOutput {
    Text { text: String },
    Json { value: Value },
    Error { message: String },
    Denied { reason: String },
}

fn legacy_text(block: LegacyBlock) -> String {
    match block {
        LegacyBlock::Text { text } => text,
        LegacyBlock::ToolCall { tool_call_id } => tool_call_id,
        LegacyBlock::ToolResult { output } => match output {
            LegacyOutput::Text { text } => text,
            LegacyOutput::Json { value } => value.to_string(),
            LegacyOutput::Error { message } => message,
            LegacyOutput::Denied { reason } => reason,
        },
    }
}

#[tokio::test]
async fn served_media_read_modes_have_pinned_bytes_and_stored_identities() {
    let rig = Rig::new("media-read-modes", Arc::new(NoopExec));
    let key = rig.key("media");
    write_legacy_seed(&rig.wal_store, &key, &seed(&key, mixed_parts("before")));
    let mut pages = Vec::new();
    for (mode, fixture) in [
        (MediaMode::Inline, "session_read.media.inline.json"),
        (MediaMode::Blocks, "session_read.media.blocks.json"),
        (
            MediaMode::Descriptors,
            "session_read.media.descriptors.json",
        ),
    ] {
        let page = read(&rig, &key, mode).await;
        let bytes = serde_json::to_string(&page).unwrap();
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(fixture);
        if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
            std::fs::write(&path, format!("{bytes}\n")).unwrap();
        }
        assert_eq!(
            bytes,
            std::fs::read_to_string(path).unwrap().trim_end(),
            "{mode:?}"
        );
        pages.push(serde_json::to_value(page).unwrap());
    }
    let inline = &pages[0];
    let blocks = &pages[1];
    let descriptors = &pages[2];
    let marker = "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=not_requested]";
    assert_eq!(
        inline["messages"][0]["message"]["content"][1],
        json!({"type":"text","text":marker})
    );
    assert_eq!(
        inline["messages"][2]["message"]["content"][0]["output"],
        json!({"kind":"text","text":format!("before\n{marker}\nafter")})
    );
    for entry in inline["messages"].as_array().unwrap() {
        for block in entry["message"]["content"].as_array().unwrap() {
            let decoded: LegacyBlock =
                serde_json::from_value(block.clone()).expect("legacy reader accepts default read");
            assert!(!legacy_text(decoded).is_empty());
        }
    }
    assert_eq!(
        blocks["messages"][0]["message"]["content"][1]["type"],
        "media"
    );
    assert_eq!(
        blocks["messages"][2]["message"]["content"][0]["output"]["kind"],
        "parts"
    );
    let media = &descriptors["messages"][0]["message"]["content"][1];
    assert_eq!(media["type"], "media_ref");
    assert_eq!(media["part"], "m0#1");
    let nested =
        &descriptors["messages"][2]["message"]["content"][0]["output"]["parts"][1]["media"];
    assert_eq!(nested["type"], "media_ref");
    assert!(
        nested.get("part").is_none(),
        "nested refs have no independent part key"
    );
    for (stored, described) in blocks["messages"]
        .as_array()
        .unwrap()
        .iter()
        .zip(descriptors["messages"].as_array().unwrap())
    {
        assert_eq!(
            described["digest"],
            cortexkit_role_llm_runner::content_digest::content_digest(&stored["message"])
        );
        for (index, part) in stored["message"]["content"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            assert_eq!(
                described["part_digests"][index]["digest"],
                cortexkit_role_llm_runner::content_digest::content_digest(part)
            );
        }
        cortexkit_role_llm_runner::read::descriptors::validate_references(&described["message"])
            .unwrap();
    }
    materialize_all(&rig, &key);
    for (mode, expected) in [MediaMode::Inline, MediaMode::Blocks, MediaMode::Descriptors]
        .into_iter()
        .zip(pages)
    {
        assert_eq!(
            serde_json::to_value(read(&rig, &key, mode).await).unwrap(),
            expected,
            "materialized read {mode:?}"
        );
    }
}

#[tokio::test]
async fn descriptors_bound_large_text_media_text_without_flattening_parts() {
    use cortexkit_role_llm_runner::read::descriptors::DESCRIPTOR_MESSAGE_MAX;
    let rig = Rig::new("large-media-read", Arc::new(NoopExec));
    let key = rig.key("large");
    let large = "x".repeat(DESCRIPTOR_MESSAGE_MAX);
    let mut parts = mixed_parts(&large);
    parts[2] = ToolOutputPart::Text { text: large };
    write_legacy_seed(&rig.wal_store, &key, &seed(&key, parts));
    let page = read(&rig, &key, MediaMode::Descriptors).await;
    let entry = &page.messages[2];
    assert!(serde_json::to_vec(entry).unwrap().len() <= DESCRIPTOR_MESSAGE_MAX);
    let body = serde_json::to_value(entry).unwrap();
    let parts = &body["message"]["content"][0]["output"]["parts"];
    assert_eq!(parts[0]["text"]["type"], "text_ref");
    assert_eq!(parts[0]["text"]["byte_size"], DESCRIPTOR_MESSAGE_MAX);
    assert_eq!(parts[1]["media"]["type"], "media_ref");
    assert_eq!(parts[2]["text"]["type"], "text_ref");
    assert_eq!(parts[2]["text"]["byte_size"], DESCRIPTOR_MESSAGE_MAX);
    assert_eq!(body["message"]["content"][0]["is_error"], true);
}

#[tokio::test]
async fn descriptors_keep_marker_only_and_empty_parts_as_stored() {
    let rig = Rig::new("marker-parts-read", Arc::new(NoopExec));
    for (name, parts) in [
        ("empty", Vec::new()),
        ("marker", vec![ToolOutputPart::Text { text: "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=store_failed]".into() }]),
    ] {
        let key = rig.key(name);
        write_legacy_seed(&rig.wal_store, &key, &seed(&key, parts.clone()));
        let blocks = serde_json::to_value(read(&rig, &key, MediaMode::Blocks).await).unwrap();
        let described = serde_json::to_value(read(&rig, &key, MediaMode::Descriptors).await).unwrap();
        let stored = &blocks["messages"][2]["message"];
        assert_eq!(stored["content"][0]["output"], json!({"kind":"parts","parts":parts}));
        assert_eq!(described["messages"][2]["message"], *stored);
        assert_eq!(described["messages"][2]["digest"], cortexkit_role_llm_runner::content_digest::content_digest(stored));
    }
}

#[tokio::test]
async fn stored_parts_count_once_and_render_without_losing_media() {
    use broca_protocol::tags::{part_kind, render_tags_into, TagRendering};
    use cortexkit_role_llm_runner::tags::PartKind;
    let rig = Rig::new("parts-render-tags", Arc::new(NoopExec));
    let document = MediaRef {
        kind: MediaKind::Document,
        mime: "application/pdf".into(),
        width: None,
        height: None,
        ..image()
    };
    for (name, parts, kind, expected_first, expected_media) in [
        (
            "mixed",
            mixed_parts("before"),
            PartKind::ToolResultText,
            "§2§ before",
            1,
        ),
        (
            "image-only",
            vec![ToolOutputPart::Media { media: image() }; 2],
            PartKind::ToolResultImageOnly,
            "§2§ [image]",
            2,
        ),
        (
            "document-only",
            vec![ToolOutputPart::Media {
                media: document.clone(),
            }],
            PartKind::ToolResultMediaOnly,
            "§2§ [media]",
            1,
        ),
        (
            "image-document",
            vec![
                ToolOutputPart::Media { media: image() },
                ToolOutputPart::Media { media: document },
            ],
            PartKind::ToolResultMediaOnly,
            "§2§ [media]",
            2,
        ),
        (
            "media-first",
            vec![
                ToolOutputPart::Media { media: image() },
                ToolOutputPart::Text {
                    text: "after".into(),
                },
            ],
            PartKind::ToolResultText,
            "§2§ after",
            1,
        ),
    ] {
        let key = rig.key(name);
        write_legacy_seed(&rig.wal_store, &key, &seed(&key, parts.clone()));
        let page = read(&rig, &key, MediaMode::Blocks).await;
        assert_eq!(
            serde_json::to_value(&page.messages[2].tags).unwrap(),
            json!([{"part":"m2#0","tag":"§2§"}])
        );
        assert_eq!(
            serde_json::to_value(&page.messages[3].tags).unwrap(),
            json!([{"part":"m3#0","tag":"§3§"}])
        );
        let mut prompt: Vec<_> = page
            .messages
            .into_iter()
            .map(|entry| entry.message)
            .collect();
        assert_eq!(
            part_kind(Role::Tool, &prompt[2].content[0], false),
            Some(kind)
        );
        render_tags_into(&mut prompt, TagRendering::default());
        let ContentBlock::ToolResult(result) = &prompt[2].content[0] else {
            panic!("tool result")
        };
        let ToolOutput::Parts { parts: rendered } = &result.output else {
            panic!("Parts")
        };
        let texts: Vec<_> = rendered
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts[0], expected_first, "{name}");
        assert_eq!(texts.iter().filter(|text| text.contains("§2§")).count(), 1);
        let media: Vec<_> = rendered
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Media { media } => Some(media),
                _ => None,
            })
            .collect();
        assert_eq!(media.len(), expected_media);
        let stored_media: Vec<_> = parts
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Media { media } => Some(media),
                _ => None,
            })
            .collect();
        assert_eq!(media, stored_media, "all references survive tag rendering");
        for reason in [MediaMarkerReason::Aged, MediaMarkerReason::RequestCap] {
            let mut marked = prompt.clone();
            if let ContentBlock::ToolResult(result) = &mut marked[2].content[0] {
                if let ToolOutput::Parts { parts } = &mut result.output {
                    for part in parts {
                        if let ToolOutputPart::Media { media } = part {
                            *part = ToolOutputPart::Text {
                                text: broca_protocol::content::media_not_shown_marker(
                                    media, reason,
                                ),
                            };
                        }
                    }
                }
            }
            render_tags_into(&mut marked, TagRendering::default());
            assert_eq!(marked[2].part_tags(), prompt[2].part_tags());
            assert_eq!(
                marked[3], prompt[3],
                "later media marking never renumbers the next reply"
            );
        }
    }
}

#[tokio::test]
async fn stored_empty_text_parts_do_not_consume_or_hide_a_number() {
    use broca_protocol::tags::{part_kind, render_tags_into, TagRendering};
    use cortexkit_role_llm_runner::tags::PartKind;
    let rig = Rig::new("empty-parts-tags", Arc::new(NoopExec));
    for (name, parts, kind, label, next) in [
        (
            "image-empty",
            vec![
                ToolOutputPart::Media { media: image() },
                ToolOutputPart::Text {
                    text: String::new(),
                },
            ],
            PartKind::ToolResultImageOnly,
            Some("§2§ [image]"),
            "§3§",
        ),
        (
            "empty-only",
            vec![
                ToolOutputPart::Text {
                    text: String::new()
                };
                2
            ],
            PartKind::EmptyText,
            None,
            "§2§",
        ),
        ("empty", Vec::new(), PartKind::EmptyText, None, "§2§"),
        (
            "empty-image-text",
            vec![
                ToolOutputPart::Text {
                    text: String::new(),
                },
                ToolOutputPart::Media { media: image() },
                ToolOutputPart::Text {
                    text: "visible".into(),
                },
            ],
            PartKind::ToolResultText,
            Some("§2§ visible"),
            "§3§",
        ),
    ] {
        let key = rig.key(name);
        write_legacy_seed(&rig.wal_store, &key, &seed(&key, parts));
        let page = read(&rig, &key, MediaMode::Blocks).await;
        let rows = serde_json::to_value(&page.messages[2].tags).unwrap();
        let expected = if label.is_some() {
            json!([{"part":"m2#0","tag":"§2§"}])
        } else {
            json!([{"part":"m2#0"}])
        };
        assert_eq!(rows, expected, "{name}");
        assert_eq!(
            serde_json::to_value(&page.messages[3].tags).unwrap(),
            json!([{"part":"m3#0","tag":next}])
        );
        let mut prompt: Vec<_> = page
            .messages
            .into_iter()
            .map(|entry| entry.message)
            .collect();
        assert_eq!(
            part_kind(Role::Tool, &prompt[2].content[0], false),
            Some(kind)
        );
        render_tags_into(&mut prompt, TagRendering::default());
        let ContentBlock::ToolResult(result) = &prompt[2].content[0] else {
            panic!("tool result")
        };
        let ToolOutput::Parts { parts } = &result.output else {
            panic!("Parts")
        };
        let visible: Vec<_> = parts
            .iter()
            .filter_map(|part| match part {
                ToolOutputPart::Text { text } if !text.is_empty() => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(visible, label.into_iter().collect::<Vec<_>>(), "{name}");
        assert_eq!(
            parts
                .iter()
                .filter(|part| matches!(part, ToolOutputPart::Media { .. }))
                .count(),
            usize::from(name != "empty-only" && name != "empty")
        );
    }
}

#[tokio::test]
async fn served_tool_result_numbering_matches_runner_vectors() {
    use broca_protocol::tags::{render_tags_into, TagRendering};
    let vectors: Value = serde_json::from_str(
        cortexkit_role_llm_runner::vectors::file("tags.json").expect("runner tags vectors"),
    )
    .unwrap();
    let rig = Rig::new("runner-parts-vectors", Arc::new(NoopExec));
    let cases = vectors["tool_results"].as_array().unwrap();
    assert_eq!(cases.len(), 9, "every tool-result vector is exercised");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let key = rig.key(name);
        let parts = case["result"]["content"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|block| match block["type"].as_str().unwrap() {
                "text" => ToolOutputPart::Text {
                    text: block["text"].as_str().unwrap().into(),
                },
                "image" => ToolOutputPart::Media { media: image() },
                "file" => ToolOutputPart::Media {
                    media: MediaRef {
                        kind: MediaKind::Document,
                        mime: block["mime_type"].as_str().unwrap().into(),
                        width: None,
                        height: None,
                        ..image()
                    },
                },
                other => panic!("unmapped vector block {other}"),
            })
            .collect();
        let mut records = seed(&key, parts);
        if let WalRecord::RunStarted { config, .. } = &mut records[0] {
            config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 39 });
        }
        if let Some(value) = case["result"].get("structuredContent") {
            if let WalRecord::ToolResult { result, .. } = &mut records[4] {
                Arc::make_mut(result).output = ToolOutput::Json {
                    value: value.clone(),
                };
            }
        }
        write_legacy_seed(&rig.wal_store, &key, &records);
        let page = read(&rig, &key, MediaMode::Blocks).await;
        let numbers: Vec<_> = [2, 3]
            .into_iter()
            .map(|index| page.messages[index].message.part_tags().unwrap().parts[0])
            .collect();
        assert_eq!(json!(numbers), case["numbers"], "{name}");
        let mut prompt: Vec<_> = page
            .messages
            .into_iter()
            .map(|entry| entry.message)
            .collect();
        render_tags_into(&mut prompt, TagRendering::default());
        let ContentBlock::ToolResult(result) = &prompt[2].content[0] else {
            panic!("tool result")
        };
        let rendered: Vec<_> = match &result.output {
            ToolOutput::Parts { parts } => parts.iter().map(|part| match part {
                ToolOutputPart::Text { text } => json!({"type":"text","text":text}),
                ToolOutputPart::Media { media } => json!({"type": if media.kind == MediaKind::Image { "image" } else { "file" }}),
            }).collect(),
            ToolOutput::Json { value } => {
                assert_eq!(*value, case["result"]["structuredContent"]);
                Vec::new()
            }
            other => panic!("unexpected rendered output {other:?}"),
        };
        let expected: Vec<_> = case["rendered_blocks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|block| {
                if block["type"] == "text" {
                    block.clone()
                } else {
                    json!({"type":block["type"]})
                }
            })
            .collect();
        assert_eq!(rendered, expected, "{name}");
    }
}
