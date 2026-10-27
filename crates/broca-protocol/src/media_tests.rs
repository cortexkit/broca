use crate::content::*;
use crate::{ToolCallId, ToolOutputPart};

fn image() -> MediaRef {
    MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 42,
        sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
        width: Some(3),
        height: Some(2),
    }
}

#[test]
fn media_serialization_goldens_round_trip() {
    let mut blocks = Vec::new();
    for (kind, mime) in [
        (MediaKind::Image, "image/png"),
        (MediaKind::Document, "application/pdf"),
        (MediaKind::Audio, "audio/wav"),
        (MediaKind::Video, "video/mp4"),
    ] {
        let mut media = image();
        media.kind = kind;
        media.mime = mime.into();
        if kind != MediaKind::Image {
            media.width = None;
            media.height = None;
        }
        blocks.push(ContentBlock::Media(media));
    }
    let mixed = vec![
        ToolOutputPart::Text {
            text: "before".into(),
        },
        ToolOutputPart::Media { media: image() },
        ToolOutputPart::Text {
            text: "after".into(),
        },
    ];
    for (parts, is_error) in [
        (mixed.clone(), false),
        (
            vec![ToolOutputPart::Text {
                text: media_not_shown_marker(&image(), MediaMarkerReason::StoreFailed),
            }],
            false,
        ),
        (mixed, true),
    ] {
        blocks.push(ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call".into()),
            output: ToolOutput::Parts { parts },
            is_error,
        }));
    }
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("../tests/goldens/media-content.json")).unwrap();
    assert_eq!(serde_json::to_value(&blocks).unwrap(), golden);
    let decoded: Vec<ContentBlock> = serde_json::from_value(golden).unwrap();
    assert_eq!(decoded, blocks);
}

#[test]
fn media_markers_pin_vocabulary_and_never_match_image_placeholders() {
    let reasons = [
        (MediaMarkerReason::Modality, "modality"),
        (MediaMarkerReason::Carrier, "carrier"),
        (MediaMarkerReason::Mime, "mime"),
        (MediaMarkerReason::Undecodable, "undecodable"),
        (MediaMarkerReason::TooLarge, "too_large"),
        (MediaMarkerReason::RequestCap, "request_cap"),
        (MediaMarkerReason::Aged, "aged"),
        (MediaMarkerReason::Provenance, "provenance"),
        (MediaMarkerReason::StoreFailed, "store_failed"),
        (MediaMarkerReason::NotRequested, "not_requested"),
    ];
    for (reason, name) in reasons {
        let marker = media_not_shown_marker(&image(), reason);
        assert_eq!(marker, format!("[media not shown: kind=image mime=image/png bytes=42 sha256=0123456789ab reason={name}]"));
        assert_eq!(serde_json::to_value(reason).unwrap(), name);
        assert!(!is_image_only_placeholder(&marker));
        assert_ne!(
            marker,
            non_text_tool_content_placeholder("image", Some("image/png"), Some(42))
        );
    }
}

#[test]
fn media_reference_dimensions_and_reference_only_decode() {
    let valid = serde_json::to_value(image()).unwrap();
    for field in ["width", "height"] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<MediaRef>(missing).is_err());
    }
    for kind in ["document", "audio", "video"] {
        let mut invalid = valid.clone();
        invalid["kind"] = kind.into();
        assert!(serde_json::from_value::<MediaRef>(invalid).is_err());
    }
    for field in ["bytes", "base64"] {
        let mut invalid = valid.clone();
        invalid[field] = "payload".into();
        assert!(serde_json::from_value::<MediaRef>(invalid).is_err());
    }
}

#[test]
fn read_only_parts_joining_is_pinned() {
    let parts = vec![
        ToolOutputPart::Text {
            text: "before".into(),
        },
        ToolOutputPart::Media { media: image() },
        ToolOutputPart::Text {
            text: "after".into(),
        },
    ];
    assert_eq!(media_parts_text(&parts, MediaMarkerReason::NotRequested), "before\n[media not shown: kind=image mime=image/png bytes=42 sha256=0123456789ab reason=not_requested]\nafter");
}

#[test]
fn media_wire_codes_are_fixed() {
    assert_eq!(
        [
            crate::error::MEDIA_UNSUPPORTED,
            crate::error::MEDIA_OBJECT_MISSING,
            crate::error::MEDIA_OBJECT_CORRUPT,
            crate::error::REQUEST_TOO_LARGE
        ],
        [
            "media_unsupported",
            "media_object_missing",
            "media_object_corrupt",
            "request_too_large"
        ]
    );
}

#[test]
fn multipart_tagging_preserves_media_and_numbers_only_once() {
    use crate::tags::{number_message, render_tags_into, ComputedRules, TagRendering};
    use crate::{Message, Role};
    for parts in [
        vec![
            ToolOutputPart::Text {
                text: "before".into(),
            },
            ToolOutputPart::Media { media: image() },
            ToolOutputPart::Text {
                text: "after".into(),
            },
        ],
        vec![ToolOutputPart::Media { media: image() }],
    ] {
        let mut message = Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call".into()),
                output: ToolOutput::Parts { parts },
                is_error: false,
            })],
        );
        let mut high = 0;
        number_message(
            &mut message,
            ComputedRules {
                render: true,
                clean: false,
            },
            &mut high,
        )
        .unwrap();
        assert_eq!(high, 1);
        render_tags_into(std::slice::from_mut(&mut message), TagRendering::default());
        let ContentBlock::ToolResult(result) = &message.content[0] else {
            panic!()
        };
        let ToolOutput::Parts { parts } = &result.output else {
            panic!()
        };
        assert!(matches!(&parts[0], ToolOutputPart::Text { text } if text.contains("§1§")));
        assert_eq!(
            parts
                .iter()
                .filter(|part| matches!(part, ToolOutputPart::Media { media } if media == &image()))
                .count(),
            1
        );
        let once = message.clone();
        render_tags_into(std::slice::from_mut(&mut message), TagRendering::default());
        assert_eq!(once, message);
    }
}
