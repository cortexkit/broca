//! Explicit mapping for negotiated `tool-result/v1` tool results.
//!
//! Callers must establish both the route's declaration and the tool catalog's opt-in before
//! decoding a reply with the role's types; reply shape alone is not negotiation.

use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::{ToolCallId, ToolResult};
use cortexkit_role_tool_provider::result::{self, ContentPart};

pub(crate) use broca_protocol::content::dropped_media_notice;

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
    result::validate(reply).map_err(MapError::Invalid)?;
    if reply.held().is_some() {
        return Err(MapError::ProviderHeld);
    }
    for part in &reply.content {
        if let ContentPart::Image(image) = part {
            STANDARD.decode(&image.data).map_err(MapError::ImageData)?;
        }
    }
    let raw = serde_json::to_value(reply).expect("tool results serialize as JSON");
    Ok(crate::subc_plane::map_legacy_call_tool_result(id, &raw))
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
