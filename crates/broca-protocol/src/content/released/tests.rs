use super::*;
use crate::content::{MediaKind, MediaRef, ToolCall, ToolCallId, ToolResult};

fn media() -> MediaRef {
    MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 10,
        sha256: "a".repeat(64),
        width: Some(1),
        height: Some(1),
    }
}

fn result(output: ToolOutput) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId("call-1".into()),
        output,
        is_error: true,
    })
}

#[test]
fn a_released_value_is_never_taken_for_the_same_bytes() {
    let same = Text::from(Text::released().as_str());
    assert_eq!(same, Text::released());
    assert!(!same.is_released());
    assert!(Text::released().is_released());
    assert!(!Json::from_text("{}").unwrap().is_released());
    assert!(Json::released().is_released());
}

#[test]
fn released_blocks_keep_their_kind_ids_and_emptiness() {
    let text = ContentBlock::Text {
        text: "hello".into(),
    };
    let released = text.released().unwrap();
    assert!(released.holds_released());
    assert!(matches!(&released, ContentBlock::Text { text } if !text.is_empty()));
    assert!(!text.holds_released());

    // Empty text has nothing to release, and stays empty.
    assert_eq!(
        ContentBlock::Text {
            text: Text::default()
        }
        .released(),
        None
    );
    let reasoning = ContentBlock::Reasoning {
        text: Text::default(),
        signature: Some("sig".into()),
    };
    let ContentBlock::Reasoning { text, signature } = reasoning.released().unwrap() else {
        panic!("a reasoning block stays a reasoning block");
    };
    assert!(text.is_empty() && !text.is_released());
    assert!(signature.unwrap().is_released());

    let call = ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId("call-1".into()),
        tool_name: "read".into(),
        input: Json::from_text(r#"{"path":"a"}"#).unwrap(),
        thought_signature: None,
    });
    let ContentBlock::ToolCall(released_call) = call.released().unwrap() else {
        panic!("a call stays a call");
    };
    assert_eq!(released_call.tool_call_id.0, "call-1");
    assert_eq!(released_call.tool_name, "read");
    assert!(released_call.input.is_released());
}

#[test]
fn a_released_result_keeps_its_media_in_place_and_its_error_flag() {
    let parts = result(ToolOutput::Parts {
        parts: vec![
            ToolOutputPart::Text {
                text: "first".into(),
            },
            ToolOutputPart::Media { media: media() },
            ToolOutputPart::Text {
                text: Text::default(),
            },
        ],
    });
    let ContentBlock::ToolResult(released) = parts.released().unwrap() else {
        panic!("a result stays a result");
    };
    assert!(released.is_error);
    assert_eq!(released.tool_call_id.0, "call-1");
    let ToolOutput::Parts { parts } = &released.output else {
        panic!("parts stay parts");
    };
    assert!(matches!(&parts[0], ToolOutputPart::Text { text } if text.is_released()));
    assert_eq!(parts[1], ToolOutputPart::Media { media: media() });
    assert!(matches!(&parts[2], ToolOutputPart::Text { text } if text.is_empty()));

    // Only media: nothing to release.
    let only_media = result(ToolOutput::Parts {
        parts: vec![ToolOutputPart::Media { media: media() }],
    });
    assert_eq!(only_media.released(), None);
    // Denials and errors are kept.
    let denied = result(ToolOutput::Denied {
        reason: "no".into(),
    });
    assert_eq!(denied.released(), None);
    let json = result(ToolOutput::Json {
        value: Json::from_text(r#"{"a":1}"#).unwrap(),
    });
    assert!(json.released().unwrap().holds_released());
    assert_eq!(ContentBlock::Media(media()).released(), None);
}
