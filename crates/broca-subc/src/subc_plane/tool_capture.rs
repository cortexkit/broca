//! Pure reply classification followed by session-owned byte capture.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::{
    content::media_not_shown_marker, MediaKind, MediaMarkerReason, MediaRef, ToolOutputPart,
};
use sha2::{Digest, Sha256};

pub(super) fn supplied_reference(block: &serde_json::Value) -> Option<MediaRef> {
    if block.get("type").and_then(serde_json::Value::as_str) == Some("media")
        || block.get("kind").and_then(serde_json::Value::as_str) == Some("media")
    {
        if let Some(media) = block.get("media") {
            serde_json::from_value(media.clone()).ok()
        } else {
            match serde_json::from_value::<broca_protocol::ContentBlock>(block.clone()).ok()? {
                broca_protocol::ContentBlock::Media(media) => Some(media),
                _ => None,
            }
        }
    } else {
        None
    }
}

pub(super) struct PendingCapture {
    index: usize,
    bytes: Vec<u8>,
    metadata: MediaRef,
}

fn blob(block: &serde_json::Value) -> Option<(&serde_json::Value, &str)> {
    match block.get("type")?.as_str()? {
        "image" | "audio" => Some((block, "data")),
        "resource" => {
            let resource = block.get("resource")?;
            resource.get("blob").map(|_| (resource, "blob"))
        }
        _ => None,
    }
}

pub(super) fn blob_mime(block: &serde_json::Value) -> Option<&str> {
    let (source, _) = blob(block)?;
    Some(
        source
            .get("mimeType")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("application/octet-stream"),
    )
}

fn pending_part(
    block: &serde_json::Value,
    index: usize,
) -> (ToolOutputPart, Option<PendingCapture>) {
    let (source, field) = blob(block).expect("classified as capturable");
    let mime = source
        .get("mimeType")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("application/octet-stream");
    let kind = match block.get("type").and_then(serde_json::Value::as_str) {
        Some("image") => MediaKind::Image,
        Some("audio") => MediaKind::Audio,
        _ if mime.starts_with("image/") => MediaKind::Image,
        _ if mime.starts_with("audio/") => MediaKind::Audio,
        _ if mime.starts_with("video/") => MediaKind::Video,
        _ => MediaKind::Document,
    };
    let bytes = source
        .get(field)
        .and_then(serde_json::Value::as_str)
        .and_then(|data| STANDARD.decode(data).ok());
    let metadata = MediaRef {
        kind,
        mime: mime.into(),
        byte_size: bytes.as_ref().map_or(0, |bytes| bytes.len() as u64),
        sha256: format!("{:x}", Sha256::digest(bytes.as_deref().unwrap_or_default())),
        width: None,
        height: None,
    };
    let part = ToolOutputPart::Text {
        text: media_not_shown_marker(&metadata, MediaMarkerReason::Undecodable),
    };
    (
        part,
        bytes.map(|bytes| PendingCapture {
            index,
            bytes,
            metadata,
        }),
    )
}

pub(super) fn classify(
    id: &ToolCallId,
    raw: &serde_json::Value,
) -> (ToolResult, Vec<PendingCapture>) {
    let content = raw.get("content").and_then(serde_json::Value::as_array);
    let Some(content) = content.filter(|blocks| blocks.iter().any(|block| blob(block).is_some()))
    else {
        return (
            super::map_text_call_tool_result(id, raw, non_text_tool_content_placeholder),
            Vec::new(),
        );
    };
    let mut captures = Vec::new();
    let parts = content
        .iter()
        .enumerate()
        .map(|(index, block)| {
            if blob(block).is_some() {
                let (part, pending) = pending_part(block, index);
                captures.extend(pending);
                part
            } else {
                let text = match block.get("type").and_then(serde_json::Value::as_str) {
                    Some("text") => block.get("text").and_then(serde_json::Value::as_str),
                    Some("resource") => block
                        .get("resource")
                        .and_then(|resource| resource.get("text"))
                        .and_then(serde_json::Value::as_str),
                    _ => None,
                };
                ToolOutputPart::Text {
                    text: text
                        .map(str::to_owned)
                        .unwrap_or_else(|| non_text_tool_content_placeholder(block)),
                }
            }
        })
        .collect();
    (
        ToolResult {
            tool_call_id: id.clone(),
            output: ToolOutput::Parts { parts },
            is_error: raw
                .get("isError")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        },
        captures,
    )
}

/// Negotiated content/v1 keeps its own text and error-code mapping. Only replace
/// the image slots its mapper deliberately left as markers pending capture.
pub(super) fn content_v1_captures(
    result: &mut ToolResult,
    raw: &serde_json::Value,
) -> Vec<PendingCapture> {
    let ToolOutput::Parts { parts } = &mut result.output else {
        return Vec::new();
    };
    let Some(content) = raw.get("content").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    let offset = usize::from(
        raw.get("errorCode")
            .and_then(serde_json::Value::as_str)
            .is_some(),
    );
    let mut captures = Vec::new();
    for (index, block) in content.iter().enumerate() {
        if block.get("type").and_then(serde_json::Value::as_str) == Some("image") {
            let (part, pending) = pending_part(block, index + offset);
            parts[index + offset] = part;
            captures.extend(pending);
        }
    }
    captures
}

pub(super) async fn capture(
    ctx: &BatchCtx,
    mut result: ToolResult,
    captures: Vec<PendingCapture>,
) -> ToolResult {
    let ToolOutput::Parts { parts } = &mut result.output else {
        return result;
    };
    for pending in captures {
        let captured = match &ctx.media_capture {
            Some(handle) => {
                handle
                    .capture(
                        &ctx.batch_id,
                        &result.tool_call_id,
                        pending.bytes,
                        pending.metadata.mime.clone(),
                    )
                    .await
            }
            None => Err(
                match broca_core::media_validation::validate_media(
                    &pending.bytes,
                    &pending.metadata.mime,
                ) {
                    Ok(media) => broca_core::tool_media::CaptureFailure {
                        reason: MediaMarkerReason::StoreFailed,
                        media: Some(media),
                    },
                    Err(reason) => broca_core::tool_media::CaptureFailure {
                        reason,
                        media: None,
                    },
                },
            ),
        };
        parts[pending.index] = match captured {
            Ok(media) => ToolOutputPart::Media { media },
            Err(failure) => ToolOutputPart::Text {
                text: media_not_shown_marker(
                    failure.media.as_ref().unwrap_or(&pending.metadata),
                    failure.reason,
                ),
            },
        };
    }
    result
}

#[cfg(all(test, feature = "test-attachments"))]
mod tests {
    use super::*;

    #[test]
    fn image_classifier_returns_decoded_bytes_awaiting_capture() {
        assert!(broca_protocol::attachments_enabled());
        let hex = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
        let png: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let (result, pending) = super::super::map_call_tool_result(
            &ToolCallId("capture-call".into()),
            &serde_json::json!({"content": [{"type": "image", "mimeType": "image/png", "data": STANDARD.encode(&png)}]}),
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].index, 0);
        assert_eq!(pending[0].bytes, png);
        assert_eq!(pending[0].metadata.kind, MediaKind::Image);
        assert_eq!(pending[0].metadata.mime, "image/png");
        assert_eq!(pending[0].metadata.byte_size, 69);
        assert_eq!(
            pending[0].metadata.sha256,
            "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640"
        );
        let ToolOutput::Parts { parts } = result.output else {
            panic!("image must produce output parts")
        };
        assert_eq!(parts, [ToolOutputPart::Text {
            text: "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=undecodable]".into(),
        }]);
    }
}
