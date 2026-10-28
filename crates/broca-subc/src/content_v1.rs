//! Explicit mapping for negotiated `content/v1` tool results.
//!
//! Callers must establish both the route's `content/v1` declaration and the tool catalog's
//! opt-in before decoding a reply with the role's types. Dispatch supplies that context only
//! when the held-call integration seams are installed; reply shape alone is not negotiation.

use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::{ToolCallId, ToolOutput, ToolOutputPart, ToolResult};
use cortexkit_role_tool_provider::result::{self, ContentPart};

pub(crate) fn dropped_media_notice(mime: &str) -> String {
    format!("[{mime} not shown to the model: this runner does not pass tool images to models yet]")
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

/// Validate and map a negotiated, final reply, preserving content order and provider error codes.
/// `structuredContent` is program-facing and is never included in the model output.
pub fn map_content_v1_result(
    id: &ToolCallId,
    reply: &result::ToolResult,
) -> Result<ToolResult, MapError> {
    result::validate(reply).map_err(MapError::Invalid)?;
    if reply.held().is_some() {
        return Err(MapError::ProviderHeld);
    }

    let mut parts = Vec::new();
    // The canonical result has no error-code field. Keep the code in a separate text part so
    // provider text stays verbatim and an error remains distinguishable even when it has images.
    if let Some(code) = &reply.error_code {
        parts.push(ToolOutputPart::Text {
            text: format!("[tool error: {code}]"),
        });
    }
    for part in &reply.content {
        let text = match part {
            ContentPart::Text { text } => text.clone(),
            ContentPart::Image(image) => {
                STANDARD.decode(&image.data).map_err(MapError::ImageData)?;
                // Dispatch replaces this notice with captured media before recording the result
                // when a capture handle is available. Otherwise the omission is durable text.
                dropped_media_notice(&image.mime_type)
            }
            // Unknown parts retain their JSON, escaped onto one line, instead of disappearing.
            other => format!(
                "[unsupported content: {}]",
                serde_json::to_string(other).expect("content parts serialize as JSON")
            ),
        };
        parts.push(ToolOutputPart::Text { text });
    }
    Ok(ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::Parts { parts },
        is_error: reply.is_error == Some(true),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use broca_protocol::{CallOptions, ContentBlock, Message, Role};
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
                    ToolOutput::Parts {
                        parts: vec![ToolOutputPart::Text {
                            text: String::new()
                        }]
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
