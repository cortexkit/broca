use broca_ck::{
    CkMessage, ContentBlock as CkContentBlock, ContentKind, HarnessMeta, MediaBlock, MediaKind,
    MediaSource, OpaqueBlock, OpaqueSource, Role, ToolResultOutputKind,
};
use broca_protocol::EncryptedReasoningFormat;
use broca_provider::families::{
    anthropic_decode,
    anthropic_encode::{self, EncodeError},
};
use serde::Serialize;
use serde_json::{json, value::RawValue, Value};

#[derive(Serialize)]
struct WrappedRequest<'a> {
    model: &'a str,
    messages: Vec<Box<RawValue>>,
}

#[test]
fn real_cc_fixture_is_decode_encode_decode_fixpoint() {
    let decoded = anthropic_decode::decode(include_bytes!(
        "../fixtures/cc_system_reminder_request.json"
    ))
    .expect("the synthetic Claude Code-shaped fixture decodes");

    let round_tripped = decode_encode_decode(&decoded.wire_model_id, &decoded.messages);
    assert_eq!(round_tripped, decoded.messages);
}

#[test]
fn real_shaped_unknown_blocks_and_nested_tool_result_opaques_are_fixpoint_stable() {
    let future_block = json!({
        "type": "totally_future_block",
        "payload": { "nested": [true, { "x": 1 }] }
    });
    let nested_image = json!({
        "type": "image",
        "source": { "type": "base64", "media_type": "image/png", "data": "AA==" }
    });
    let body = json!({
        "model": "claude-sonnet-4-20250514",
        "messages": [
            {
                "role": "user",
                "content": [
                    future_block.clone(),
                    { "type": "text" },
                    { "type": "text", "text": "hello" }
                ]
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "I should look this up.",
                        "signature": "sig-byte-verbatim=="
                    },
                    {
                        "type": "tool_use",
                        "id": "toolu_lookup_1",
                        "name": "lookup",
                        "input": { "id": 1 }
                    }
                ]
            },
            {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_lookup_1",
                    "is_error": false,
                    "content": [
                        {
                            "type": "text",
                            "text": "done",
                            "cache_control": { "type": "ephemeral", "ttl": "1h" }
                        },
                        nested_image.clone(),
                        42
                    ]
                }]
            }
        ]
    });

    let decoded = anthropic_decode::decode(&serde_json::to_vec(&body).expect("body bytes"))
        .expect("the real-shaped body decodes");

    let round_tripped = decode_encode_decode(&decoded.wire_model_id, &decoded.messages);
    assert_eq!(round_tripped, decoded.messages);
}

#[test]
fn array_error_tool_result_round_trip_preserves_is_error_true() {
    let body = br#"{"model":"claude","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","is_error":true,"content":[{"type":"text","text":"boom"}]}]}]}"#;
    let decoded = anthropic_decode::decode(body).expect("array error tool result decodes");
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("messages encode");

    assert_eq!(
        encoded[0].get(),
        r#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":[{"type":"text","text":"boom"}],"is_error":true}]}"#
    );
}

#[test]
fn successful_array_tool_results_keep_content_kind_and_omit_is_error() {
    let with_false = br#"{"model":"claude","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","is_error":false,"content":[{"type":"text","text":"ok"}]}]}]}"#;
    let without_flag = br#"{"model":"claude","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":[{"type":"text","text":"ok"}]}]}]}"#;

    for body in [with_false.as_slice(), without_flag.as_slice()] {
        let decoded = anthropic_decode::decode(body).expect("successful array result decodes");
        let ContentKind::ToolResult { output, .. } = &decoded.messages[0].content[0].kind else {
            panic!("expected a tool result");
        };
        assert!(matches!(output.kind, ToolResultOutputKind::Content { .. }));
        let encoded =
            anthropic_encode::encode_messages(&decoded.messages).expect("messages encode");
        assert_eq!(
            encoded[0].get(),
            r#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":[{"type":"text","text":"ok"}]}]}"#
        );
    }
}

#[test]
fn encode_is_deterministic() {
    let first = serialize_messages_array(&fixture_messages());
    let second = serialize_messages_array(&fixture_messages());
    assert_eq!(first, second);
}

#[test]
fn appending_a_new_turn_preserves_the_existing_serialized_prefix() {
    let prefix = vec![
        message(Role::User, vec![text_block("question")]),
        message(Role::System, vec![text_block("system reminder")]),
        message(
            Role::Assistant,
            vec![
                CkContentBlock::bare(ContentKind::Reasoning {
                    text: "thinking".into(),
                    signature: Some("sig".into()),
                }),
                CkContentBlock::bare(ContentKind::ToolCall {
                    thought_signature: None,
                    id: "toolu_1".into(),
                    name: "lookup".into(),
                    input: json!({ "id": 1 }),
                    provider_executed: false,
                }),
            ],
        ),
    ];
    let extended = prefix
        .clone()
        .into_iter()
        .chain([message(Role::User, vec![text_block("follow-up")])])
        .collect::<Vec<_>>();

    let prefix_bytes = serialize_messages_array(&prefix);
    let extended_bytes = serialize_messages_array(&extended);

    assert_eq!(
        &prefix_bytes[..prefix_bytes.len() - 1],
        &extended_bytes[..prefix_bytes.len() - 1]
    );
}

#[test]
fn system_messages_stay_in_place() {
    let messages = vec![
        message(Role::User, vec![text_block("hello")]),
        message(Role::System, vec![text_block("mid-array reminder")]),
        message(Role::Assistant, vec![text_block("ack")]),
    ];

    let encoded = anthropic_encode::encode_messages(&messages).expect("system encodes in place");
    let wire: Value = serde_json::from_slice(&serde_json::to_vec(&encoded).expect("array bytes"))
        .expect("wire array parses");
    let wire = wire.as_array().expect("messages array");

    assert_eq!(wire[0]["role"], json!("user"));
    assert_eq!(wire[1]["role"], json!("system"));
    assert_eq!(wire[2]["role"], json!("assistant"));
    assert_eq!(
        wire[1]["content"],
        json!([{ "type": "text", "text": "mid-array reminder" }])
    );
}

#[test]
fn typed_media_block_fails_loud() {
    let messages = vec![message(
        Role::User,
        vec![CkContentBlock::bare(ContentKind::Media(MediaBlock {
            kind: MediaKind::Image,
            media_type: "image/png".into(),
            filename: None,
            source: MediaSource::Url {
                url: "https://example.invalid/image.png".into(),
            },
        }))],
    )];

    assert!(matches!(
        anthropic_encode::encode_messages(&messages),
        Err(EncodeError::MediaBlock {
            message_index: 0,
            block_index: 0,
            result_block_index: None,
        })
    ));
}

#[test]
fn foreign_encrypted_reasoning_fails_closed() {
    let messages = vec![message(
        Role::Assistant,
        vec![CkContentBlock::bare(ContentKind::RedactedReasoning {
            data: r#"{"type":"reasoning","encrypted_content":"cipher"}"#.into(),
            provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
        })],
    )];

    assert!(matches!(
        anthropic_encode::encode_messages(&messages),
        Err(EncodeError::ForeignEncryptedReasoning {
            message_index: 0,
            block_index: 0,
        })
    ));
}

#[test]
fn opaque_without_raw_fails_loud() {
    let messages = vec![message(
        Role::Assistant,
        vec![CkContentBlock::bare(ContentKind::Opaque(OpaqueBlock {
            source: OpaqueSource::Harness {
                harness: "test-harness".into(),
            },
            kind: "future_block".into(),
            raw: Vec::new(),
            arc: None,
        }))],
    )];

    assert!(matches!(
        anthropic_encode::encode_messages(&messages),
        Err(EncodeError::OpaqueWithoutRaw {
            message_index: 0,
            block_index: 0,
            result_block_index: None,
            ..
        })
    ));
}

fn fixture_messages() -> Vec<CkMessage> {
    anthropic_decode::decode(include_bytes!(
        "../fixtures/cc_system_reminder_request.json"
    ))
    .expect("the synthetic Claude Code-shaped fixture decodes")
    .messages
}

fn decode_encode_decode(model: &str, messages: &[CkMessage]) -> Vec<CkMessage> {
    let encoded = anthropic_encode::encode_messages(messages).expect("messages encode");
    let body = WrappedRequest {
        model,
        messages: encoded,
    };
    anthropic_decode::decode(&serde_json::to_vec(&body).expect("request bytes"))
        .expect("encoded body decodes")
        .messages
}

fn serialize_messages_array(messages: &[CkMessage]) -> Vec<u8> {
    let encoded = anthropic_encode::encode_messages(messages).expect("messages encode");
    serde_json::to_vec(&encoded).expect("message array bytes")
}

fn message(role: Role, content: Vec<CkContentBlock>) -> CkMessage {
    CkMessage {
        role,
        content,
        origin: None,
        provider_extras: Default::default(),
        meta: HarnessMeta::default(),
    }
}

fn text_block(text: &str) -> CkContentBlock {
    CkContentBlock::bare(ContentKind::Text { text: text.into() })
}

#[test]
fn scalar_marked_message_encodes_back_to_the_bare_string_spelling() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [
            {"role": "user", "content": "scalar user prompt"},
            {"role": "system", "content": "trailing scalar system"},
            {"role": "assistant", "content": [{"type": "text", "text": "array reply"}]},
        ]
    });
    let decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("encodes");
    let round_tripped: Vec<Value> = encoded
        .iter()
        .map(|raw| serde_json::from_str(raw.get()).unwrap())
        .collect();
    assert_eq!(
        round_tripped[0],
        json!({"role": "user", "content": "scalar user prompt"})
    );
    assert_eq!(
        round_tripped[1],
        json!({"role": "system", "content": "trailing scalar system"})
    );
    assert_eq!(
        round_tripped[2],
        json!({"role": "assistant", "content": [{"type": "text", "text": "array reply"}]})
    );
}

#[test]
fn scalar_marked_message_that_outgrew_the_form_falls_back_to_the_complete_array() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [{"role": "user", "content": "scalar"}]
    });
    let mut decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    // Mutation after decode grows the marked message past what a bare string can spell.
    decoded.messages[0]
        .content
        .push(CkContentBlock::bare(ContentKind::Text {
            text: "second block".into(),
        }));
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("array fallback");
    let round_tripped: Value = serde_json::from_str(encoded[0].get()).unwrap();
    // The spelling hint loses; the content wins, complete and in order.
    assert_eq!(
        round_tripped,
        json!({"role": "user", "content": [
            {"type": "text", "text": "scalar"},
            {"type": "text", "text": "second block"},
        ]})
    );
}

#[test]
fn scalar_marked_text_that_gained_cache_control_keeps_it_via_array_fallback() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [{"role": "user", "content": "scalar"}]
    });
    let mut decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    // A cache marker added after decode cannot ride a bare string; the array spelling must
    // win so the marker survives.
    let mut anthropic = std::collections::BTreeMap::new();
    anthropic.insert("cache_control".to_string(), json!({"type": "ephemeral"}));
    decoded.messages[0].content[0]
        .provider_extras
        .insert("anthropic".to_string(), anthropic);
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("array fallback");
    let round_tripped: Value = serde_json::from_str(encoded[0].get()).unwrap();
    assert_eq!(
        round_tripped,
        json!({"role": "user", "content": [
            {"type": "text", "text": "scalar", "cache_control": {"type": "ephemeral"}},
        ]})
    );
}

#[test]
fn unrecognized_content_form_marker_fails_loud() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [{"role": "user", "content": "scalar"}]
    });
    let mut decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    decoded.messages[0]
        .provider_extras
        .get_mut("anthropic")
        .unwrap()
        .insert(
            anthropic_decode::CONTENT_FORM_EXTRA.to_string(),
            json!("mystery"),
        );
    assert!(matches!(
        anthropic_encode::encode_messages(&decoded.messages),
        Err(EncodeError::ScalarFormIncompatible {
            message_index: 0,
            ..
        })
    ));
}

#[test]
fn cache_control_emits_type_first_ttl_second_exactly() {
    let body = json!({
        "model": "claude-sonnet-4-5",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "with ttl", "cache_control": {"ttl": "1h", "type": "ephemeral"}},
                {"type": "text", "text": "no ttl", "cache_control": {"type": "ephemeral"}},
            ]},
        ]
    });
    let decoded = anthropic_decode::decode(body.to_string().as_bytes()).expect("decodes");
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("encodes");
    let raw = encoded[0].get();
    // Exact rendered bytes: the authored Anthropic key order (type before ttl), not the
    // alphabetical order a sorted JSON map would produce.
    assert!(
        raw.contains(r#""cache_control":{"type":"ephemeral","ttl":"1h"}"#),
        "ttl case must render type-first: {raw}"
    );
    assert!(
        raw.contains(r#""cache_control":{"type":"ephemeral"}"#),
        "no-ttl case must render bare type: {raw}"
    );
}

#[test]
fn typed_citations_round_trip_with_source_value_bytes() {
    let body = br#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"search result","citations":[{"type":"web_search_result_location","url":"https://example.test","title":"Example"}]}]}]}"#;
    let decoded = anthropic_decode::decode(body).expect("decodes");
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("encodes");

    assert_eq!(
        encoded[0].get(),
        r#"{"role":"user","content":[{"type":"text","text":"search result","citations":[{"type":"web_search_result_location","url":"https://example.test","title":"Example"}]}]}"#
    );
}

#[test]
fn typed_future_field_round_trip_preserves_unsorted_inner_object_keys() {
    let body = br#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"x","frobnicate":{"z":1,"a":2}}]}]}"#;
    let decoded = anthropic_decode::decode(body).expect("decodes");
    let encoded = anthropic_encode::encode_messages(&decoded.messages).expect("encodes");

    assert_eq!(
        encoded[0].get(),
        r#"{"role":"user","content":[{"type":"text","text":"x","frobnicate":{"z":1,"a":2}}]}"#
    );
}

#[test]
fn residual_extras_serialize_deterministically_and_coexist_with_cache_control() {
    let body = br#"{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"ttl":"1h","type":"ephemeral"},"citations":{"z":1,"a":2}}]}]}"#;
    let first = anthropic_decode::decode(body).expect("first decode");
    let second = anthropic_decode::decode(body).expect("second decode");
    let first_extras = serde_json::to_vec(&first.messages[0].content[0].provider_extras)
        .expect("serialize first extras");
    let second_extras = serde_json::to_vec(&second.messages[0].content[0].provider_extras)
        .expect("serialize second extras");

    assert_eq!(first_extras, second_extras);
    assert_eq!(
        first_extras,
        br#"{"anthropic":{"cache_control":{"ttl":"1h","type":"ephemeral"},"citations":{"z":1,"a":2}}}"#
    );
    let ck_wire = serde_json::to_vec(&first.messages).expect("serialize CK messages");
    let round_tripped: Vec<CkMessage> =
        serde_json::from_slice(&ck_wire).expect("parse CK messages");
    let encoded = anthropic_encode::encode_messages(&round_tripped).expect("encodes");
    assert_eq!(
        encoded[0].get(),
        r#"{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"1h"},"citations":{"z":1,"a":2}}]}"#
    );
}
