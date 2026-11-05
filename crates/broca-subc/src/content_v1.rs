//! Explicit mapping for negotiated `tool-result/v1` tool results.
//!
//! Callers must establish both the route's declaration and the tool catalog's opt-in before
//! decoding a reply with the role's types; reply shape alone is not negotiation.

use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::{ToolCallId, ToolResult};
use cortexkit_role_tool_provider::result::{self, ContentPart};

pub(crate) use broca_protocol::content::dropped_media_notice;

pub(crate) fn json_dump_text_bytes<'a>(
    tool_name: &str,
    mut texts: impl Iterator<Item = &'a str> + Clone,
) -> Option<usize> {
    if json_dump_exempt(tool_name) || texts.clone().map(str::len).sum::<usize>() < 200 {
        return None;
    }
    let first = texts.next()?;
    let text = if texts.clone().next().is_none() {
        std::borrow::Cow::Borrowed(first)
    } else {
        std::borrow::Cow::Owned(std::iter::once(first).chain(texts).collect::<String>())
    };
    let text = text.trim();
    if text.len() < 200 {
        return None;
    }
    // The whole text is a JSON object or array, or a JSON string whose content is one (JSON
    // encoded twice). JSON quoted inside other text, such as a code excerpt with an escaped JSON
    // literal, doesn't count: search, grep and outline results quote source files, and source
    // is full of it.
    let object_or_array = |text: &str| {
        matches!(text.as_bytes().first(), Some(b'{' | b'['))
            && serde_json::from_str::<serde_json::Value>(text).is_ok()
    };
    let double_encoded = || {
        text.starts_with('"')
            && serde_json::from_str::<String>(text).is_ok_and(|inner| object_or_array(inner.trim()))
    };
    (object_or_array(text) || double_encoded()).then_some(text.len())
}

fn json_dump_exempt(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "bash" | "read" | "read_json" | "codemode" | "todowrite"
    )
}

pub(crate) fn json_dump_output_text_bytes(
    tool_name: &str,
    output: &broca_protocol::ToolOutput,
) -> Option<usize> {
    use broca_protocol::{ToolOutput, ToolOutputPart};
    if json_dump_exempt(tool_name) {
        return None;
    }
    match output {
        ToolOutput::Text { text } => {
            json_dump_text_bytes(tool_name, std::iter::once(text.as_str()))
        }
        ToolOutput::Error { message, .. } => {
            json_dump_text_bytes(tool_name, std::iter::once(message.as_str()))
        }
        ToolOutput::Parts { parts } => json_dump_text_bytes(
            tool_name,
            parts.iter().filter_map(|part| match part {
                ToolOutputPart::Text { text } => Some(text.as_str()),
                ToolOutputPart::Media { .. } => None,
            }),
        ),
        // A legacy reply's structuredContent field renders as JSON when no content text exists.
        // Its object/array value needs only a byte count, not a second parse. Negotiated
        // tool-result/v1 normalization omits this program-only field.
        ToolOutput::Json { value } if value.is_object() || value.is_array() => {
            let bytes = value.to_string().len();
            (bytes >= 200).then_some(bytes)
        }
        ToolOutput::Json { .. } | ToolOutput::Denied { .. } => None,
    }
}

/// Why a decoded reply cannot become a final model-facing result.
#[derive(Debug)]
pub enum MapError {
    Invalid(result::ResultProblem),
    /// Interim content belongs to the held-call lifecycle, never to the model's final answer.
    ProviderHeld,
    ImageData(base64::DecodeError),
}

impl std::fmt::Display for MapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(problem) => write!(f, "malformed content/v1 tool result: {problem}"),
            Self::ProviderHeld => f.write_str("provider-held reply is not a final tool result"),
            Self::ImageData(error) => write!(f, "invalid content/v1 image data: {error}"),
        }
    }
}

impl std::error::Error for MapError {}

/// Validate a negotiated final reply, then use the legacy storage normalization. A transport
/// change must not change stored text or rendered prompts, including errors and media notices.
pub fn map_content_v1_result(
    id: &ToolCallId,
    reply: &result::ToolResult,
) -> Result<ToolResult, MapError> {
    if reply.held().is_some() {
        return Err(MapError::ProviderHeld);
    }
    let bytes = result::model_visible_bytes(reply);
    if bytes > result::MAX_MODEL_VISIBLE_BYTES {
        return Ok(size_error_result(
            id,
            cortexkit_role_llm_runner::tool_result_limit::SizeError::new(bytes),
        ));
    }
    result::validate(reply).map_err(MapError::Invalid)?;
    for part in &reply.content {
        if let ContentPart::Image(image) = part {
            STANDARD.decode(&image.data).map_err(MapError::ImageData)?;
        }
    }
    let raw = serde_json::to_value(reply).expect("tool results serialize as JSON");
    Ok(crate::subc_plane::map_legacy_call_tool_result(id, &raw))
}

/// Refusal accounting preserves the refusal code and never implies execution.
pub(crate) fn oversized_refusal(code: &str, message: &str) -> Option<broca_protocol::ToolOutput> {
    let error = cortexkit_role_llm_runner::tool_result_limit::refusal_size_error(message)?;
    let mut output = broca_protocol::ToolOutput::error("The provider refused the tool call, but its refusal message exceeded 64 KiB and was discarded. Do not repeat the call just to see that message.");
    if let broca_protocol::ToolOutput::Error {
        code: stored_code,
        detail,
        ..
    } = &mut output
    {
        *stored_code = (!code.is_empty()).then(|| code.to_owned());
        *detail = Some(serde_json::to_string(&error).expect("size metadata serializes"));
    }
    Some(output)
}

/// Discard the entire completed output at intake, before any stored result or media capture.
pub(crate) fn size_error_result(
    id: &ToolCallId,
    error: cortexkit_role_llm_runner::tool_result_limit::SizeError,
) -> ToolResult {
    let mut output = broca_protocol::ToolOutput::error(
        cortexkit_role_llm_runner::tool_result_limit::OUTPUT_DISCARDED,
    );
    if let broca_protocol::ToolOutput::Error { code, detail, .. } = &mut output {
        *code = Some(error.reason.clone());
        *detail = Some(serde_json::to_string(&error).expect("size metadata serializes"));
    }
    ToolResult {
        tool_call_id: id.clone(),
        output,
        is_error: true,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use broca_protocol::{CallOptions, ContentBlock, Message, Role, ToolOutput};
    use broca_provider::{
        families::{anthropic_frozen_config, AnthropicMessages},
        wire::WireFamily,
    };
    use serde_json::json;

    pub(crate) fn render_model_request(result: ToolResult) -> Vec<u8> {
        let request = CallOptions::new(vec![Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(result)],
        )]);
        AnthropicMessages.render(&anthropic_frozen_config("content-v1-model"), &request)
    }

    fn map(raw: serde_json::Value) -> Result<ToolResult, MapError> {
        let reply = serde_json::from_value(raw).unwrap();
        map_content_v1_result(&ToolCallId("call_01".into()), &reply)
    }

    fn json_object(bytes: usize) -> String {
        format!(r#"{{"data":"{}"}}"#, "x".repeat(bytes - 11))
    }

    #[test]
    fn tool_result_json_dump_byte_boundary_199_and_200() {
        for bytes in [199, 200] {
            let text = format!(" \n{}\t ", json_object(bytes));
            assert_eq!(text.trim().len(), bytes);
            assert_eq!(
                json_dump_text_bytes("search", std::iter::once(text.as_str())),
                (bytes == 200).then_some(200)
            );
        }
        let text = format!(r#"{{"data":"{}é"}}"#, "x".repeat(187));
        assert_eq!(text.len(), 200);
        assert_eq!(
            json_dump_text_bytes("search", std::iter::once(text.as_str())),
            Some(200)
        );
        assert_eq!(
            json_dump_text_bytes("search", std::iter::once(" ".repeat(200).as_str())),
            None
        );
    }

    /// A code excerpt quoting an escaped JSON literal is not a dump, however many escaped quotes
    /// it holds: search, grep and outline results quote source files, which hold such literals.
    #[test]
    fn tool_result_json_dump_ignores_json_quoted_inside_text() {
        let excerpt = format!(
            "crates/x/src/lib.rs:12 [lexical match]\n    let body = \"{{\\\"op\\\":\\\"store\\\",{}}}\";\n",
            r#"\"k\":\"v\","#.repeat(20)
        );
        assert!(excerpt.matches(r#"\""#).count() >= 10 && excerpt.len() >= 200);
        assert_eq!(
            json_dump_text_bytes("search", std::iter::once(excerpt.as_str())),
            None
        );
    }

    /// JSON encoded twice, as a JSON string whose content is an object or array, is a dump.
    #[test]
    fn tool_result_json_dump_counts_double_encoded_json() {
        for inner in [json_object(200), format!(r#"["{}"]"#, "x".repeat(196))] {
            let text = serde_json::to_string(&inner).unwrap();
            assert_eq!(
                json_dump_text_bytes("search", std::iter::once(text.as_str())),
                Some(text.len())
            );
        }
    }

    #[test]
    fn tool_result_json_dump_allowlisted_tools_are_exempt() {
        let text = json_object(200);
        for tool in ["bash", "read", "read_json", "codemode", "todowrite"] {
            assert_eq!(
                json_dump_text_bytes(tool, std::iter::once(text.as_str())),
                None
            );
        }
        assert_eq!(
            json_dump_text_bytes("search", std::iter::once(text.as_str())),
            Some(200)
        );
    }

    #[test]
    fn tool_result_json_dump_array_and_whole_text_parse() {
        let text = format!(r#"["{}"]"#, "x".repeat(196));
        assert_eq!(text.len(), 200);
        assert_eq!(
            json_dump_text_bytes("search", std::iter::once(text.as_str())),
            Some(200)
        );
        for not_dump in [
            format!("prefix {text}"),
            format!("{text} suffix"),
            format!("{}broken", json_object(200)),
            format!(r#""{}""#, "x".repeat(200)),
            "1".repeat(200),
        ] {
            assert_eq!(
                json_dump_text_bytes("search", std::iter::once(not_dump.as_str())),
                None
            );
        }
    }

    #[test]
    fn tool_result_json_dump_joins_text_parts_before_trimming() {
        let text = json_object(200);
        let parts = [" \n", &text[..100], &text[100..], "\t "];
        assert_eq!(json_dump_text_bytes("search", parts.into_iter()), Some(200));
        let parts = ["{", &" ".repeat(200), "}"];
        assert_eq!(json_dump_text_bytes("search", parts.into_iter()), Some(202));
    }

    #[test]
    fn tool_result_json_dump_output_excludes_images_and_program_only_content() {
        use broca_protocol::{MediaKind, MediaRef, ToolOutputPart};
        let image = ToolOutputPart::Media {
            media: MediaRef {
                kind: MediaKind::Image,
                mime: "image/png".into(),
                byte_size: 200_000,
                sha256: "0".repeat(64),
                width: Some(1),
                height: Some(1),
            },
        };
        let text = json_object(200);
        let output = ToolOutput::Parts {
            parts: vec![
                ToolOutputPart::Text {
                    text: text[..100].into(),
                },
                image.clone(),
                ToolOutputPart::Text {
                    text: text[100..].into(),
                },
            ],
        };
        assert_eq!(json_dump_output_text_bytes("search", &output), Some(200));
        let output = ToolOutput::Parts {
            parts: vec![
                ToolOutputPart::Text {
                    text: json_object(199),
                },
                image,
            ],
        };
        assert_eq!(json_dump_output_text_bytes("search", &output), None);
        let raw = json!({"content":[],"structuredContent":{"data":"x".repeat(200)}});
        let mut negotiated_raw = raw.clone();
        negotiated_raw["content"] = json!([{"type":"text","text":""}]);
        let negotiated = map(negotiated_raw).unwrap();
        assert_eq!(
            json_dump_output_text_bytes("search", &negotiated.output),
            None
        );
        let legacy =
            crate::subc_plane::map_legacy_call_tool_result(&ToolCallId("legacy".into()), &raw);
        assert_eq!(
            json_dump_output_text_bytes("search", &legacy.output),
            Some(211)
        );
        assert_eq!(
            json_dump_output_text_bytes("read_json", &legacy.output),
            None
        );
        assert_eq!(
            json_dump_output_text_bytes("search", &ToolOutput::error(text)),
            Some(200)
        );
    }

    fn assert_golden(raw: serde_json::Value, golden: &str) {
        let result = map(raw).unwrap();
        let canonical = serde_json::to_string(&result).unwrap();
        let rendered = render_model_request(result);
        assert_eq!(String::from_utf8(rendered).unwrap(), golden.trim_end());
        assert!(!canonical.contains("structuredContent"));
        assert!(!canonical.contains("PROGRAM_ONLY_SECRET"));
    }

    #[test]
    fn content_v1_text_only_model_input_golden() {
        assert_golden(
            json!({
                "content": [{"type": "text", "text": "12:"}, {"type": "text", "text": "00"}],
                "structuredContent": {"private": "PROGRAM_ONLY_SECRET"}
            }),
            include_str!("../tests/fixtures/content_v1/text.json"),
        );
    }

    #[test]
    fn content_v1_mixed_image_model_input_golden() {
        assert_golden(
            json!({
                "content": [
                    {"type": "text", "text": "before"},
                    {"type": "image", "mimeType": "image/png", "data": "aGVsbG8=", "width": 1, "height": 1, "alt": "not base64 text"},
                    {"type": "json", "value": {"answer": 42, "line": "first\nsecond"}},
                    {"type": "text", "text": "after"}
                ],
                "structuredContent": {"private": "PROGRAM_ONLY_SECRET"}
            }),
            include_str!("../tests/fixtures/content_v1/mixed_image.json"),
        );
    }

    #[test]
    fn content_v1_multiple_images_model_input_golden() {
        assert_golden(
            json!({
                "content": [
                    {"type": "image", "mimeType": "image/jpeg", "data": "AA==", "width": 1, "height": 1, "id": "first"},
                    {"type": "text", "text": "between"},
                    {"type": "image", "mimeType": "image/webp", "data": "YWJj", "width": 4294967296_u64, "height": 1, "id": "second"}
                ],
                "structuredContent": {"private": "PROGRAM_ONLY_SECRET"}
            }),
            include_str!("../tests/fixtures/content_v1/multiple_images.json"),
        );
    }

    #[test]
    fn content_v1_error_model_input_golden() {
        let raw = json!({
            "content": [
                {"type": "text", "text": "could not read image"},
                {"type": "image", "mimeType": "image/gif", "data": "YWJj", "width": 1, "height": 1},
                {"type": "text", "text": "try another file"}
            ],
            "isError": true,
            "errorCode": "provider_error",
            "structuredContent": {"private": "PROGRAM_ONLY_SECRET"}
        });
        assert!(map(raw.clone()).unwrap().is_error);
        assert_golden(raw, include_str!("../tests/fixtures/content_v1/error.json"));
    }

    const PDF_BLOB: &str = "JVBERi0xLjcKc3ludGhldGljIGRvY3VtZW50Cg==";
    const PDF_NOTICE: &str =
        "[application/pdf not shown to the model: this runner does not pass tool images to models yet]";

    fn pdf_reply() -> serde_json::Value {
        json!({"content": [{"type": "resource", "resource": {
            "uri": "file:///report.pdf", "name": "report.pdf",
            "mimeType": "application/pdf", "blob": PDF_BLOB
        }}]})
    }

    #[test]
    fn content_v1_shared_normalization_omits_resource_blobs_and_preserves_history() {
        let result = map(pdf_reply()).unwrap();
        let stored = serde_json::to_string(&result).unwrap();
        let rendered = String::from_utf8(render_model_request(result.clone())).unwrap();
        assert!(!stored.contains(&PDF_BLOB[..16]), "{stored}");
        assert!(!rendered.contains(&PDF_BLOB[..16]), "{rendered}");
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: PDF_NOTICE.into()
            }
        );

        let old_text = format!("[unsupported content: {}]", pdf_reply()["content"][0]);
        let old_stored = json!({
            "tool_call_id": "call_01", "is_error": false,
            "output": {"kind": "parts", "parts": [{"kind": "text", "text": old_text}]}
        });
        let old_result: ToolResult = serde_json::from_value(old_stored.clone()).unwrap();
        assert_eq!(serde_json::to_value(&old_result).unwrap(), old_stored);
        let old_rendered = String::from_utf8(render_model_request(old_result)).unwrap();
        assert!(old_rendered.contains(&PDF_BLOB[..16]));
    }

    #[test]
    fn content_v1_shared_normalization_pdf_notice_has_a_text_tag() {
        use broca_protocol::tags::{number_message, render_tags_into, ComputedRules, TagRendering};

        let mut message = Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(map(pdf_reply()).unwrap())],
        );
        number_message(
            &mut message,
            ComputedRules {
                render: true,
                clean: true,
            },
            &mut 0,
        )
        .unwrap();
        render_tags_into(std::slice::from_mut(&mut message), TagRendering::default());
        let ContentBlock::ToolResult(result) = &message.content[0] else {
            panic!("expected tool result")
        };
        assert_eq!(
            result.output,
            ToolOutput::Text {
                text: format!("§1§ {PDF_NOTICE}")
            }
        );
        assert!(!broca_protocol::content::is_image_only_notice(PDF_NOTICE));
        let rendered = String::from_utf8(render_model_request(result.clone())).unwrap();
        assert!(rendered.contains(&format!("§1§ {PDF_NOTICE}")));
        assert!(!rendered.contains("[image]"));
    }

    #[test]
    fn content_v1_shared_normalization_stores_legacy_bytes_without_parts() {
        let cases = [
            (
                "text only",
                json!({"content": [{"type": "text", "text": "12:"}, {"type": "text", "text": "00"}]}),
                "12:00",
            ),
            (
                "text and image",
                json!({"content": [
                    {"type": "text", "text": "before"},
                    {"type": "image", "mimeType": "image/png", "data": "aGVsbG8=", "width": 1, "height": 1},
                    {"type": "text", "text": "after"}
                ]}),
                "beforeafter\n[image/png not shown to the model: this runner does not pass tool images to models yet]",
            ),
            (
                "text and binary resource",
                json!({"content": [
                    {"type": "text", "text": "read document"},
                    pdf_reply()["content"][0].clone()
                ]}),
                "read document\n[application/pdf not shown to the model: this runner does not pass tool images to models yet]",
            ),
            (
                "binary resource only",
                pdf_reply(),
                PDF_NOTICE,
            ),
            (
                "text resource",
                json!({"content": [{"type": "resource", "resource": {
                    "uri": "file:///notes.txt", "mimeType": "text/plain", "text": "notes"
                }}]}),
                "[non-text tool content omitted: type=resource]",
            ),
            (
                "unknown part",
                json!({"content": [{"type": "future", "mimeType": "application/pdf", "data": PDF_BLOB,
                    "nested": [{"blob": PDF_BLOB}]}]}),
                "[non-text tool content omitted: type=future mimeType=application/pdf data≈40 bytes]",
            ),
            (
                "mixed unknown and image",
                json!({"content": [
                    {"type": "text", "text": "before"},
                    {"type": "image", "mimeType": "image/png", "data": "aGVsbG8=", "width": 1, "height": 1},
                    {"type": "json", "value": {"answer": 42}},
                    {"type": "text", "text": "after"}
                ]}),
                "before[non-text tool content omitted: type=json]after\n[image/png not shown to the model: this runner does not pass tool images to models yet]",
            ),
            (
                "error with unknown part",
                json!({"content": [
                    {"type": "text", "text": "cannot read"},
                    {"type": "future", "blob": PDF_BLOB}
                ], "isError": true, "errorCode": "provider_error"}),
                "cannot read[non-text tool content omitted: type=future]",
            ),
        ];
        for (name, mut raw, expected_text) in cases {
            raw["structuredContent"] = json!({"private": "PROGRAM_ONLY_SECRET"});
            let legacy =
                crate::subc_plane::map_legacy_call_tool_result(&ToolCallId("call_01".into()), &raw);
            let v1 = map(raw).unwrap();
            let stored = serde_json::to_vec(&v1).unwrap();
            assert_eq!(stored, serde_json::to_vec(&legacy).unwrap(), "{name}");
            match &v1.output {
                ToolOutput::Text { text } => assert_eq!(text, expected_text, "{name}"),
                ToolOutput::Error { message, .. } => assert_eq!(message, expected_text, "{name}"),
                output => panic!("{name}: expected legacy text or error, never parts: {output:?}"),
            }
            let rendered = render_model_request(v1);
            assert_eq!(rendered, render_model_request(legacy), "{name}");
            for bytes in [&stored, &rendered] {
                let text = std::str::from_utf8(bytes).unwrap();
                assert!(!text.contains(&PDF_BLOB[..16]), "{name}: {text}");
                assert!(!text.contains("aGVsbG8="), "{name}: {text}");
                assert!(!text.contains("PROGRAM_ONLY_SECRET"), "{name}: {text}");
            }
        }

        for width in [4_294_967_296_u64, u64::MAX] {
            let raw = json!({"content": [{"type": "image", "mimeType": "image/png",
                "data": PDF_BLOB, "width": width, "height": 1}]});
            let reply: result::ToolResult = serde_json::from_value(raw.clone()).unwrap();
            assert!(matches!(&reply.content[0], ContentPart::Image(image) if image.width == width));
            let v1 = map(raw.clone()).unwrap();
            let legacy =
                crate::subc_plane::map_legacy_call_tool_result(&ToolCallId("call_01".into()), &raw);
            assert_eq!(
                serde_json::to_vec(&v1).unwrap(),
                serde_json::to_vec(&legacy).unwrap()
            );
            assert_eq!(v1.output, ToolOutput::Text {
                text: "[image/png not shown to the model: this runner does not pass tool images to models yet]".into()
            });
            assert!(!String::from_utf8(render_model_request(v1))
                .unwrap()
                .contains(&PDF_BLOB[..16]));
        }
    }

    #[test]
    fn content_v1_structured_content_never_substitutes_for_content() {
        for content in [json!([]), json!([{"type": "text", "text": ""}])] {
            let reply = json!({"content": content, "structuredContent": {"private": "PROGRAM_ONLY_SECRET"}});
            if content.as_array().unwrap().is_empty() {
                assert!(matches!(map(reply), Err(MapError::Invalid(_))));
            } else {
                let result = map(reply).unwrap();
                assert_eq!(
                    result.output,
                    ToolOutput::Text {
                        text: String::new()
                    }
                );
            }
        }
    }

    #[test]
    fn content_v1_rejects_malformed_errors_and_images() {
        let text = json!([{"type": "text", "text": "boom"}]);
        for raw in [
            json!({"content": text, "isError": true}),
            json!({"content": text, "errorCode": "provider_error"}),
            json!({"content": text, "isError": false, "errorCode": "provider_error"}),
            json!({"content": text, "isError": true, "errorCode": "Not_Snake"}),
            json!({"content": [{"type": "image", "mimeType": "image/png", "data": "data:image/png;base64,AA==", "width": 1, "height": 1}]}),
            json!({"content": [{"type": "image", "mimeType": "image/png", "data": "AB==", "width": 1, "height": 1}]}),
            json!({"content": [{"type": "image", "mimeType": "text/plain", "data": "AA==", "width": 1, "height": 1}]}),
            json!({"content": [{"type": "image", "mimeType": "image/png", "data": "AA==", "width": 0, "height": 1}]}),
        ] {
            assert!(
                matches!(map(raw.clone()), Err(MapError::Invalid(_))),
                "{raw}"
            );
        }
    }

    #[test]
    fn content_v1_provider_held_interim_content_is_not_a_final_result() {
        let raw = json!({
            "content": [{"type": "text", "text": "interim content must not enter the prompt"}],
            "held": {"reason": "running"}
        });
        assert!(matches!(map(raw), Err(MapError::ProviderHeld)));
    }
}

#[cfg(test)]
mod size_limit_tests {
    #[test]
    fn oversized_refusal_retains_its_code_without_claiming_execution() {
        use broca_protocol::ToolOutput;
        assert!(super::oversized_refusal("permission_denied", &"x".repeat(65_536)).is_none());
        let ToolOutput::Error {
            code,
            message,
            detail,
            ..
        } = super::oversized_refusal("permission_denied", &"x".repeat(65_537)).unwrap()
        else {
            panic!("expected refusal")
        };
        assert_eq!(code.as_deref(), Some("permission_denied"));
        assert!(!message.contains("tool ran"));
        assert!(message.contains("refused") && message.contains("discarded"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(detail.as_deref().unwrap()).unwrap()["bytes"],
            65_537
        );
    }

    use super::*;
    use broca_protocol::ToolOutput;
    use serde_json::json;

    #[test]
    fn negotiated_limit_is_inclusive_and_discards_the_entire_completed_result() {
        let id = ToolCallId("call".into());
        for (bytes, oversized) in [(65_536, false), (65_537, true)] {
            let reply = result::ToolResult::new(vec![ContentPart::text("x".repeat(bytes))]);
            let stored = map_content_v1_result(&id, &reply).unwrap();
            assert_eq!(stored.is_error, oversized);
            if oversized {
                let ToolOutput::Error {
                    message,
                    code,
                    detail,
                    ..
                } = stored.output
                else {
                    panic!("oversized success must become an error")
                };
                assert_eq!(code.as_deref(), Some("tool_result_too_large"));
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(detail.as_deref().unwrap()).unwrap(),
                    json!({"reason":"tool_result_too_large","bytes":65_537,"limit":65_536})
                );
                assert!(
                    message.contains("tool ran")
                        && message.contains("discarded")
                        && message.contains("not be repeated")
                );
                assert!(!message.contains(&"x".repeat(100)));
            } else {
                assert!(matches!(stored.output, ToolOutput::Text { text } if text.len() == bytes));
            }
        }
    }

    #[test]
    fn negotiated_structured_content_does_not_count_toward_the_text_limit() {
        let reply: result::ToolResult = serde_json::from_value(json!({
            "content":[{"type":"text","text":"short"}],
            "structuredContent":{"large":"x".repeat(65_537)}
        }))
        .unwrap();
        let stored = map_content_v1_result(&ToolCallId("call".into()), &reply).unwrap();
        assert!(!stored.is_error);
        assert!(matches!(stored.output, ToolOutput::Text { text } if text == "short"));
    }
}
