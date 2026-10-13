use broca_ck::{CkMessage, OpaqueSource};

/// Adjacent text blocks must stay separate across the round trip.
///
/// Block boundaries are not cosmetic: a markdown construct opened in one block and left
/// unterminated must not be able to close on a delimiter in the next. Merging two blocks
/// into one lets exactly that happen, and the result renders as well-formed content that
/// nobody wrote rather than as anything visibly broken.
///
/// The codec has no reason to merge them today. This pins that, because the failure is
/// invisible in the data — the merged text is still valid text — and would only surface as
/// a rendering artifact somewhere downstream.
#[test]
fn adjacent_text_blocks_are_not_merged_by_the_round_trip() {
    use broca_protocol::content::ContentBlock;
    use broca_protocol::{Message, Role};

    let original = vec![Message::new(
        Role::Assistant,
        vec![
            ContentBlock::Text {
                text: "a paragraph ending in an unterminated fence\n```rust".into(),
            },
            ContentBlock::Text {
                text: "a later paragraph carrying its own fence\n```".into(),
            },
        ],
    )];

    let lifted = broca_ck::codec::lift(&original, &|_| "anthropic".to_string());
    let projected = broca_ck::codec::project(&lifted).expect("project back to canonical");

    // Compare the whole sequence, not its length. A count alone passes for a codec that
    // reorders the blocks or rewrites their text, and both destroy the same structure a
    // merge does.
    assert_eq!(
        projected[0].content, original[0].content,
        "the content sequence must round-trip identically; merging lets the first block's open \
         fence close on the second's delimiter, and reordering moves text across a construct \
         boundary that the model placed deliberately"
    );
}

/// Blocks of different kinds keep their relative order across the round trip.
///
/// Order carries meaning independently of separation: text written before a tool call is a
/// preamble, and the same text after it is a result commentary. A codec that groups blocks by
/// kind rather than walking them in sequence produces well-formed output that says something
/// the model did not.
#[test]
fn mixed_block_kinds_keep_their_relative_order() {
    use broca_protocol::content::{ContentBlock, ToolCall};
    use broca_protocol::{Message, Role, ToolCallId};

    let original = vec![Message::new(
        Role::Assistant,
        vec![
            ContentBlock::Text {
                text: "before the call".into(),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call-1".into()),
                tool_name: "read".into(),
                input: serde_json::json!({}),
                thought_signature: None,
            }),
            ContentBlock::Text {
                text: "after the call".into(),
            },
        ],
    )];

    let lifted = broca_ck::codec::lift(&original, &|_| "anthropic".to_string());
    let projected = broca_ck::codec::project(&lifted).expect("project back to canonical");

    assert_eq!(
        projected[0].content, original[0].content,
        "text before a tool call must stay before it; bucketing blocks by kind would render a \
         preamble as commentary on a result it was written ahead of"
    );
}

#[test]
fn opaque_source_round_trips_the_pinned_wire_shape() {
    let source = OpaqueSource::Wire {
        wire: "openai_responses".into(),
    };
    let rendered = serde_json::to_value(&source).expect("serialize OpaqueSource::Wire");
    assert_eq!(
        rendered,
        serde_json::json!({"source": "wire", "wire": "openai_responses"})
    );

    let golden: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/ck_wire_golden.json")).unwrap();
    let last = golden.as_array().unwrap().last().unwrap().clone();
    let parsed: CkMessage =
        serde_json::from_value(last).expect("deserialize golden reasoning vector");
    let rerendered = serde_json::to_value(parsed).expect("re-serialize golden reasoning vector");
    assert_eq!(
        rerendered,
        golden.as_array().unwrap().last().unwrap().clone()
    );
}
