use broca_ck::{
    CkMessage, ContentBlock as CkContentBlock, ContentKind, OpaqueArcKind, OpaqueArcRole,
    ResultBlockKind, Role, ToolResultOutputKind,
};
use broca_protocol::{
    content::{ContentBlock as CanonContentBlock, Message, ToolOutput, ToolResult},
    request::CallOptions,
    ToolCallId,
};
use broca_provider::{
    families::{anthropic_decode, anthropic_frozen_config, AnthropicMessages},
    wire::WireFamily,
};
use serde_json::{json, Value};

#[test]
fn assistant_and_system_tool_results_remain_dropped() {
    let stray_result = CanonContentBlock::ToolResult(ToolResult {
        tool_call_id: ToolCallId("stray".into()),
        output: ToolOutput::Text {
            text: "must not render".into(),
        },
        is_error: false,
    });
    let opts = CallOptions::new(vec![
        Message::new(Role::System, vec![stray_result.clone()]),
        Message::new(Role::Assistant, vec![stray_result]),
    ]);
    let rendered: Value = serde_json::from_slice(
        &AnthropicMessages.render(&anthropic_frozen_config("claude"), &opts),
    )
    .unwrap();

    assert!(rendered["system"]
        .as_array()
        .unwrap()
        .iter()
        .all(|block| block["type"] != "tool_result"));
    assert!(rendered["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .all(|block| block["type"] != "tool_result"));
}

#[test]
fn real_claude_code_shaped_fixture_preserves_extras_arcs_and_opaque_blocks() {
    let future_block = json!({
        "type": "totally_future_block",
        "payload": { "nested": [true, { "x": 1 }] }
    });
    let server_tool_use = json!({
        "type": "server_tool_use",
        "id": "srv_1",
        "name": "web_search",
        "input": { "query": "cache control" }
    });
    let server_tool_result = json!({
        "type": "web_search_tool_result",
        "tool_use_id": "srv_1",
        "content": [{ "title": "Result", "url": "https://example.com" }]
    });
    let nested_image = json!({
        "type": "image",
        "source": { "type": "base64", "media_type": "image/png", "data": "AA==" }
    });
    let body = json!({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1024,
        "stream": true,
        "system": [
            { "type": "text", "text": "You are Claude Code." },
            {
                "type": "text",
                "text": "Cache this tail.",
                "cache_control": { "type": "ephemeral", "ttl": "1h" }
            }
        ],
        "tools": [{
            "name": "grep",
            "description": "Search files.",
            "input_schema": { "type": "object", "properties": { "query": { "type": "string" } } }
        }],
        "messages": [
            { "role": "user", "content": [{ "type": "text", "text": "Find the cache rule." }] },
            {
                "role": "assistant",
                "content": [
                    { "type": "thinking", "thinking": "I should search first.", "signature": "sig-byte-verbatim==" },
                    { "type": "tool_use", "id": "toolu_grep", "name": "grep", "input": { "query": "cache_control" } },
                    server_tool_use.clone(),
                    server_tool_result.clone(),
                    future_block.clone()
                ]
            },
            {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_grep",
                    "is_error": false,
                    "content": [
                        { "type": "text", "text": "Found cache_control." },
                        nested_image.clone()
                    ]
                }]
            }
        ]
    });

    let decoded = anthropic_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(decoded.tools[0].name, "grep");
    assert_eq!(decoded.residual["max_tokens"], json!(1024));

    let system_tail = &decoded.messages[1].content[0];
    assert_eq!(
        system_tail.provider_extras["anthropic"]["cache_control"],
        json!({ "type": "ephemeral", "ttl": "1h" })
    );

    let assistant = decoded
        .messages
        .iter()
        .find(|message| message.role == Role::Assistant)
        .unwrap();
    match &assistant.content[0].kind {
        ContentKind::Reasoning { text, signature } => {
            assert_eq!(text, "I should search first.");
            assert_eq!(signature.as_deref(), Some("sig-byte-verbatim=="));
        }
        other => panic!("expected signed thinking block, got {other:?}"),
    }

    let server_use = opaque_block(&assistant.content[2]);
    assert_eq!(server_use.kind, "server_tool_use");
    assert_eq!(
        serde_json::from_slice::<Value>(&server_use.raw).unwrap(),
        server_tool_use
    );
    assert_eq!(server_use.arc.as_ref().unwrap().kind, OpaqueArcKind::Tool);
    assert_eq!(server_use.arc.as_ref().unwrap().id, "srv_1");
    assert_eq!(
        server_use.arc.as_ref().unwrap().role,
        OpaqueArcRole::Request
    );

    let server_result = opaque_block(&assistant.content[3]);
    assert_eq!(server_result.kind, "web_search_tool_result");
    assert_eq!(
        serde_json::from_slice::<Value>(&server_result.raw).unwrap(),
        server_tool_result
    );
    assert_eq!(
        server_result.arc.as_ref().unwrap().kind,
        OpaqueArcKind::Tool
    );
    assert_eq!(server_result.arc.as_ref().unwrap().id, "srv_1");
    assert_eq!(
        server_result.arc.as_ref().unwrap().role,
        OpaqueArcRole::Response
    );

    let future = opaque_block(&assistant.content[4]);
    assert_eq!(future.kind, "totally_future_block");
    assert_eq!(
        serde_json::from_slice::<Value>(&future.raw).unwrap(),
        future_block
    );

    let tool_message = decoded
        .messages
        .iter()
        .find(|message| message.role == Role::Tool)
        .unwrap();
    match &tool_message.content[0].kind {
        ContentKind::ToolResult {
            id,
            tool_name,
            output,
            provider_executed,
        } => {
            assert_eq!(id, "toolu_grep");
            assert_eq!(tool_name, "grep");
            assert!(!provider_executed);
            match &output.kind {
                ToolResultOutputKind::Content { blocks } => {
                    assert!(matches!(blocks[0].kind, ResultBlockKind::Text { .. }));
                    let ResultBlockKind::Opaque { opaque } = &blocks[1].kind else {
                        panic!("expected nested non-text result block to be opaque");
                    };
                    assert_eq!(opaque.kind, "image");
                    assert_eq!(
                        serde_json::from_slice::<Value>(&opaque.raw).unwrap(),
                        nested_image
                    );
                }
                other => panic!("expected nested content output, got {other:?}"),
            }
        }
        other => panic!("expected tool result block, got {other:?}"),
    }
}

/// Decode a synthetic Claude Code-shaped request (8-message turn).
/// The wire skeleton, not any original prompt or signature, is the regression surface.
/// CC-shaped turns insert a role="system" reminder into the message array to apply
/// instructions at that point in the conversation, alongside the top-level system field.
/// The decoder must accept that reminder and preserve its wire position, rather than
/// rejecting the turn or moving the reminder to the start.
#[test]
fn real_cc_turn_with_mid_array_system_decodes() {
    let bytes = include_bytes!("../fixtures/cc_system_reminder_request.json");
    let body: Value = serde_json::from_slice(bytes).unwrap();
    let decoded = anthropic_decode::decode(bytes).expect("a real CC turn decodes");

    // The top-level system field (3 blocks) becomes leading System messages; the 8 wire
    // messages follow. The mid-array system decodes typed AND KEEPS ITS WIRE POSITION —
    // no hoisting — so a splice consumer's 1:1 wire alignment holds by construction.
    let system_lead = decoded
        .messages
        .iter()
        .take_while(|message| message.role == Role::System)
        .count();
    assert!(
        system_lead >= 1,
        "top-level system decoded as leading System"
    );
    let wire = &decoded.messages[system_lead..];
    let wire_roles: Vec<Role> = wire.iter().map(|m| m.role).collect();
    assert_eq!(wire_roles[0], Role::User);
    assert_eq!(
        wire_roles[1],
        Role::System,
        "the mid-array <system-reminder> decodes typed, in place"
    );
    assert_eq!(wire_roles[2], Role::Assistant);
    assert_eq!(
        wire.len(),
        body["messages"].as_array().unwrap().len(),
        "1:1 wire alignment: every wire message decodes to exactly one CK message"
    );

    // The same synthetic turn exercises thinking + tool_use/tool_result decoding alongside
    // the fix: assistant turns carry typed Reasoning, and the tool arc pairs.
    assert!(wire.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block.kind, ContentKind::Reasoning { .. }))
    }));
    let call_id = wire
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match &block.kind {
            ContentKind::ToolCall { id, .. } => Some(id.clone()),
            _ => None,
        })
        .expect("the turn carries a tool call");
    assert!(
        wire.iter().flat_map(|message| &message.content).any(
            |block| matches!(&block.kind, ContentKind::ToolResult { id, .. } if *id == call_id)
        ),
        "the tool_result pairs with the call by id"
    );
}

#[test]
fn future_and_malformed_blocks_decode_to_opaque_not_error() {
    let body = json!({
        "model": "claude-sonnet-4-20250514",
        "messages": [{
            "role": "user",
            "content": [
                { "type": "totally_made_up_2027", "weird": { "nested": [1, 2, 3] } },
                { "type": "text" }
            ]
        }]
    });
    let decoded = anthropic_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    let content = &decoded.messages[0].content;
    assert_eq!(opaque_block(&content[0]).kind, "totally_made_up_2027");
    assert_eq!(opaque_block(&content[1]).kind, "text");
}

#[test]
fn decode_is_deterministic() {
    let bytes = serde_json::to_vec(&minimal_tool_request()).unwrap();
    let first = anthropic_decode::decode(&bytes).unwrap();
    let second = anthropic_decode::decode(&bytes).unwrap();
    assert_eq!(first, second);
}

#[test]
fn repeated_tool_use_ids_resolve_within_their_own_turns() {
    let body = json!({
        "model": "claude-sonnet-4-20250514",
        "messages": [
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_0", "name": "search", "input": {}}]
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "call_0", "content": "search-result"}]
            },
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "call_0", "name": "read", "input": {}}]
            },
            {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "call_0", "content": "read-result"}]
            }
        ]
    });

    let decoded = anthropic_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    let names = decoded
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match &block.kind {
            ContentKind::ToolResult { tool_name, .. } => Some(tool_name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["search", "read"]);

    let mut legacy_global_names = std::collections::BTreeMap::new();
    for message in body["messages"].as_array().unwrap() {
        for block in message["content"].as_array().unwrap() {
            if block["type"] == "tool_use" {
                legacy_global_names.insert(
                    block["id"].as_str().unwrap().to_string(),
                    block["name"].as_str().unwrap().to_string(),
                );
            }
        }
    }
    let legacy_result_names = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .filter(|block| block["type"] == "tool_result")
        .map(|block| legacy_global_names[block["tool_use_id"].as_str().unwrap()].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        legacy_result_names,
        ["read", "read"],
        "a request-global last-write-wins map renames the first result"
    );
}

#[test]
fn array_error_tool_result_decodes_only_as_error_content() {
    let body = br#"{"model":"claude","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","is_error":true,"content":[{"type":"text","text":"boom"}]}]}]}"#;
    let decoded = anthropic_decode::decode(body).expect("array error tool result decodes");
    let ContentKind::ToolResult { output, .. } = &decoded.messages[0].content[0].kind else {
        panic!("expected a tool result");
    };

    assert!(matches!(
        output.kind,
        ToolResultOutputKind::ErrorContent { .. }
    ));
    assert!(!matches!(output.kind, ToolResultOutputKind::Content { .. }));
}

#[test]
fn decode_error_is_reserved_for_structural_garbage() {
    assert!(matches!(
        anthropic_decode::decode(b"not json"),
        Err(anthropic_decode::DecodeError::InvalidJson(_))
    ));
    assert!(matches!(
        anthropic_decode::decode(br#"{"messages":42}"#),
        Err(anthropic_decode::DecodeError::MessagesNotArray)
    ));
    assert!(matches!(
        anthropic_decode::decode(br#"{"messages":[{"content":[]}] }"#),
        Err(anthropic_decode::DecodeError::MessageWithoutRole { index: 0 })
    ));
}

#[test]
fn system_string_and_array_decode_to_leading_system_messages() {
    let string_body = br#"{
        "model":"claude-sonnet-4-20250514",
        "system":"alpha\n\u03b2",
        "messages":[{"role":"user","content":"hello"}]
    }"#;
    let decoded = anthropic_decode::decode(string_body).unwrap();
    assert_eq!(decoded.messages[0].role, Role::System);
    assert_eq!(text_of(&decoded.messages[0]), "alpha\nβ");
    assert_eq!(decoded.messages[1].role, Role::User);
    assert_eq!(text_of(&decoded.messages[1]), "hello");

    let array_body = json!({
        "model": "claude-sonnet-4-20250514",
        "system": [
            { "type": "text", "text": "first" },
            { "type": "text", "text": "second" }
        ],
        "messages": [{ "role": "user", "content": [{ "type": "text", "text": "third" }] }]
    });
    let decoded = anthropic_decode::decode(&serde_json::to_vec(&array_body).unwrap()).unwrap();
    assert_eq!(
        decoded.messages.iter().map(text_of).collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
    assert_eq!(decoded.messages[0].role, Role::System);
    assert_eq!(decoded.messages[1].role, Role::System);
    assert_eq!(decoded.messages[2].role, Role::User);
}

fn minimal_tool_request() -> Value {
    json!({
        "model": "claude-sonnet-4-20250514",
        "tools": [{
            "name": "lookup",
            "description": "Look up data.",
            "input_schema": { "type": "object" }
        }],
        "messages": [
            { "role": "assistant", "content": [{ "type": "tool_use", "id": "toolu_1", "name": "lookup", "input": { "id": 1 } }] },
            { "role": "user", "content": [{ "type": "tool_result", "tool_use_id": "toolu_1", "content": "ok", "is_error": false }] }
        ]
    })
}

fn opaque_block(block: &CkContentBlock) -> &broca_ck::OpaqueBlock {
    match &block.kind {
        ContentKind::Opaque(opaque) => opaque,
        other => panic!("expected opaque block, got {other:?}"),
    }
}

fn text_of(message: &CkMessage) -> &str {
    match &message.content[0].kind {
        ContentKind::Text { text } => text,
        other => panic!("expected text block, got {other:?}"),
    }
}

#[test]
fn test_duplicate_keys_anthropic() {
    // 1. Top-level duplicate: {"model":"a","model":"b","messages":[...valid...]} decodes successfully, wire_model_id == "b".
    let body_top_dup = br#"{
        "model": "claude-sonnet-4-20250514-a",
        "model": "claude-sonnet-4-20250514-b",
        "messages": [
            { "role": "user", "content": "hello" }
        ]
    }"#;
    let decoded = anthropic_decode::decode(body_top_dup).unwrap();
    assert_eq!(decoded.wire_model_id, "claude-sonnet-4-20250514-b");

    // 2. Duplicate inside a message: a message with two `role` keys decodes with the LAST role.
    let body_msg_dup = br#"{
        "model": "claude-sonnet-4-20250514",
        "messages": [
            { "role": "assistant", "role": "user", "content": "hello" }
        ]
    }"#;
    let decoded = anthropic_decode::decode(body_msg_dup).unwrap();
    assert_eq!(decoded.messages[0].role, Role::User);

    // 3. Duplicate inside a content block: two `type` keys — decodes as the last type (typed, not Opaque-demoted).
    let body_block_dup = br#"{
        "model": "claude-sonnet-4-20250514",
        "messages": [
            {
                "role": "user",
                "content": [
                    { "type": "totally_made_up_2027", "type": "text", "text": "hello" }
                ]
            }
        ]
    }"#;
    let decoded = anthropic_decode::decode(body_block_dup).unwrap();
    assert_eq!(decoded.messages[0].content.len(), 1);
    match &decoded.messages[0].content[0].kind {
        ContentKind::Text { text } => assert_eq!(text, "hello"),
        other => panic!("expected text block, got {:?}", other),
    }
}

#[test]
fn scalar_content_decode_stamps_message_level_provenance() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [
            {"role": "user", "content": "plain scalar prompt"},
            {"role": "system", "content": "trailing scalar system"},
        ]
    });
    let decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    for message in &decoded.messages {
        let form = message
            .provider_extras
            .get("anthropic")
            .and_then(|a| a.get(anthropic_decode::CONTENT_FORM_EXTRA))
            .and_then(Value::as_str);
        assert_eq!(form, Some(anthropic_decode::CONTENT_FORM_SCALAR));
    }
}

#[test]
fn array_content_decode_never_mints_content_form() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "array form"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "reply"}]},
        ]
    });
    let decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    for message in &decoded.messages {
        assert!(
            message
                .provider_extras
                .get("anthropic")
                .and_then(|a| a.get(anthropic_decode::CONTENT_FORM_EXTRA))
                .is_none(),
            "array-form content must not carry a content_form marker"
        );
    }
}

#[test]
fn recognized_extension_fields_stay_on_typed_blocks() {
    let body = br#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"x","frobnicate":{"z":1,"a":2}}]}]}"#;
    let decoded = anthropic_decode::decode(body).expect("decodes");
    let block = &decoded.messages[0].content[0];

    assert!(matches!(block.kind, ContentKind::Text { ref text } if text == "x"));
    assert_eq!(
        block.provider_extras["anthropic"]["frobnicate"],
        json!({"z": 1, "a": 2})
    );
}
