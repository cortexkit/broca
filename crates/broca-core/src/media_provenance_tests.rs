use super::*;
use broca_protocol::content::{MediaKind, MediaRef, Role, ToolResult};
use broca_protocol::ToolCallId;

fn image() -> MediaRef {
    MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 67,
        sha256: "ab".repeat(32),
        width: Some(1),
        height: Some(1),
    }
}

fn message(role: Role, content: Vec<ContentBlock>) -> Message {
    Message::new(role, content)
}

fn text(value: &str) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}

fn parts_result(parts: Vec<ToolOutputPart>) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId("call".into()),
        output: ToolOutput::Parts { parts },
        is_error: false,
    })
}

#[test]
fn a_media_block_is_found_with_its_path() {
    let messages = vec![
        message(Role::User, vec![text("hello")]),
        message(Role::User, vec![text("see"), ContentBlock::Media(image())]),
    ];
    let found = find_supplied_media(&messages).expect("media block is supplied media");
    assert_eq!(
        found,
        SuppliedMedia {
            message: 1,
            block: 1,
            part: None
        }
    );
    assert_eq!(found.path(), "messages[1].content[1]");
    assert!(found.to_string().contains("a media block"));
    assert!(found.to_string().contains("upload references"));
}

#[test]
fn a_tool_result_media_part_is_found_with_its_part_index() {
    let messages = vec![message(
        Role::Tool,
        vec![parts_result(vec![
            ToolOutputPart::Text {
                text: "before".into(),
            },
            ToolOutputPart::Media { media: image() },
        ])],
    )];
    let found = find_supplied_media(&messages).expect("media part is supplied media");
    assert_eq!(found.path(), "messages[0].content[0].output.parts[1]");
    assert!(found.to_string().contains("a tool result media part"));
}

#[test]
fn text_only_content_and_text_only_parts_are_not_media() {
    let messages = vec![
        message(Role::User, vec![text("hello")]),
        message(
            Role::Tool,
            vec![parts_result(vec![ToolOutputPart::Text {
                text: "[media not shown: reason=store_failed]".into(),
            }])],
        ),
    ];
    assert_eq!(find_supplied_media(&messages), None);
}
