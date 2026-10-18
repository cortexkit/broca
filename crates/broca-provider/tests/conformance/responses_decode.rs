use broca_ck::{
    CkMessage, ContentBlock as CkContentBlock, ContentKind, OpaqueArcKind, OpaqueArcRole,
    OpaqueBlock, ResultBlock, ResultBlockKind, Role, ToolResultOutputKind,
};
use broca_protocol::{
    content::{ContentBlock, EncryptedReasoningFormat, FinishReason, Message},
    request::{CallOptions, ModelHandle},
    NormalizedEvent, ProviderControl,
};
use broca_provider::{
    conformance::check_render_determinism,
    families::{
        anthropic_frozen_config, gemini_frozen_config, openai_responses_chatgpt_frozen_config,
        openai_responses_frozen_config, responses_decode, AnthropicMessages, Gemini,
        OpenAiResponses, OpenAiResponsesChatgpt,
    },
    policy::ReasoningPolicy,
    wire::WireFamily,
};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};

#[test]
fn decode_splice_identity_for_realistic_codex_fixture() {
    let body = realistic_codex_fixture();
    let bytes = serde_json::to_vec(&body).unwrap();
    let decoded = responses_decode::decode(&bytes).unwrap();

    assert_eq!(decoded.wire_model_id, "gpt-5-codex");
    assert_eq!(decoded.tools.len(), 1);
    assert_eq!(decoded.tools[0].name, "read_file");
    assert_eq!(reconstruct_input(&decoded.messages), body["input"]);
    assert_eq!(decoded.residual, expected_residual(&body));

    let system = &decoded.messages[0];
    assert_eq!(system.role, Role::System);
    assert_eq!(
        text_of(system.content.first().unwrap()),
        "Arrange imaginary blue tiles."
    );
}

/// Decode a synthetic Codex resume-turn request with the production wire shape:
///  - a reasoning item echoed back with synthetic `encrypted_content` bytes and NO `id`
///    field (so the Opaque arc is absent and identity is content-derived downstream);
///  - a `developer`-role message and `input_text`/`output_text` content-part arrays;
///  - `instructions`, `prompt_cache_key`, `client_metadata`, `include`, `text` riding in
///    the residual; `max_output_tokens` absent (the codex backend strips it).
///
/// NOTE the deliberate scope: reconstruction identity (as `reconstruct_input` asserts for
/// the minimal fixtures) does NOT hold for these richer shapes and is not the contract — typed
/// mapping is render-lossless, not spelling-lossless (part-arrays become Text blocks, and
/// extra item fields like `phase` are dropped). The MITM consumer splices from its own
/// original bytes and never re-emits typed items from CK, so what decode must guarantee is:
/// never-Err, correct role/pairing structure, byte-verbatim Opaque raw, and a lossless CK
/// wire round-trip.
#[test]
fn captured_codex_resume_turn_decodes_with_verbatim_reasoning() {
    let bytes = include_bytes!("../fixtures/codex_resume_turn_with_echoed_reasoning.json");
    let body: Value = serde_json::from_slice(bytes).unwrap();
    let decoded = responses_decode::decode(bytes).unwrap();

    assert_eq!(decoded.wire_model_id, body["model"].as_str().unwrap());
    assert_eq!(decoded.residual, expected_residual(&body));

    // Structure: [instructions→System, developer→System, user(env), user(prompt),
    // reasoning⊕assistant-message coalesced into ONE assistant message (the MC-atomicity
    // pairing), user(follow-up)].
    let roles: Vec<Role> = decoded.messages.iter().map(|m| m.role).collect();
    assert_eq!(
        roles,
        vec![
            Role::System,
            Role::System,
            Role::User,
            Role::User,
            Role::Assistant,
            Role::User
        ],
        "real capture decodes to the expected role sequence with the reasoning item \
         coalesced onto the assistant answer"
    );
    let assistant = &decoded.messages[4];
    assert_eq!(assistant.content.len(), 2, "opaque reasoning + answer text");
    assert!(
        matches!(&assistant.content[1].kind, ContentKind::Text { text } if text.contains("REASON=391")),
        "the assistant answer decoded as typed text"
    );

    // The echoed reasoning item decodes to an assistant-side Opaque: byte-verbatim raw
    // (the encrypted_content ciphertext must survive untouched), arc ABSENT because the
    // live item carries no id.
    let reasoning_input = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "reasoning")
        .expect("the capture contains an echoed reasoning item");
    assert!(
        reasoning_input.get("id").is_none(),
        "fixture premise: the live echoed reasoning item has no id field"
    );
    let enc = reasoning_input["encrypted_content"].as_str().unwrap();
    assert!(!enc.is_empty());

    let reasoning_opaque = decoded
        .messages
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .flat_map(|message| &message.content)
        .filter_map(|block| match &block.kind {
            ContentKind::Opaque(opaque) if opaque.kind == "reasoning" => Some(opaque),
            _ => None,
        })
        .next()
        .expect("the reasoning item decoded to an Opaque block");
    assert!(reasoning_opaque.arc.is_none(), "no id => no arc");
    assert_eq!(
        &serde_json::from_slice::<Value>(&reasoning_opaque.raw).unwrap(),
        reasoning_input,
        "the Opaque raw is the item byte-verbatim, ciphertext included"
    );

    // CK wire round-trip: the decoded messages must survive serialization across a
    // process boundary (the subc wire to a transform module) and come back with the
    // ciphertext intact — the exact path the OpaqueSource serialization bug would have
    // broken on the first reasoning-bearing Codex turn.
    let lifted = decoded.messages.clone();
    let wire = serde_json::to_string(&lifted).expect("CK messages serialize");
    let back: Vec<CkMessage> = serde_json::from_str(&wire).expect("CK messages parse back");
    assert_eq!(back, lifted, "CK wire round-trip is lossless");
    let back_reasoning = back
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match &block.kind {
            ContentKind::Opaque(opaque) if opaque.kind == "reasoning" => Some(opaque),
            _ => None,
        })
        .next()
        .expect("the reasoning Opaque survived the wire");
    assert_eq!(
        serde_json::from_slice::<Value>(&back_reasoning.raw).unwrap()["encrypted_content"]
            .as_str()
            .unwrap(),
        enc,
        "the encrypted_content ciphertext survives the CK wire byte-for-byte"
    );
}

/// Decode a synthetic Codex tool-call turn carrying a `function_call` ↔
/// `function_call_output` pair. Pins the pairing behavior against production shapes:
///  - correlation is by `call_id` ONLY — the function_call item has NO native `id` field
///    (same id-less pattern as the reasoning item), so pairing must never rely on one;
///  - `arguments` is a JSON-ENCODED STRING (double-encoded), which decode parses into the
///    typed ToolCall input; the `output` is a PLAIN string riding as-is;
///  - the tool name for the ToolResult is recovered from the paired function_call via the
///    call_id map even though the output item itself carries no name.
#[test]
fn captured_codex_tool_call_turn_pairs_by_call_id() {
    let bytes = include_bytes!("../fixtures/codex_toolcall_turn_function_call_pairing.json");
    let body: Value = serde_json::from_slice(bytes).unwrap();
    let decoded = responses_decode::decode(bytes).unwrap();

    let fc = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "function_call")
        .unwrap();
    assert!(
        fc.get("id").is_none(),
        "fixture premise: the live function_call has no native id"
    );
    let call_id = fc["call_id"].as_str().unwrap();
    assert!(
        fc["arguments"].is_string(),
        "fixture premise: arguments is a JSON-encoded string"
    );

    // The function_call decodes to a typed ToolCall with the double-encoded arguments
    // parsed into a structured input.
    let tool_call = decoded
        .messages
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .flat_map(|message| &message.content)
        .find_map(|block| match &block.kind {
            ContentKind::ToolCall {
                id, name, input, ..
            } => Some((id, name, input)),
            _ => None,
        })
        .expect("the function_call decoded to a typed ToolCall");
    assert_eq!(tool_call.0, call_id);
    assert_eq!(tool_call.1, "sample_probe");
    assert_eq!(
        tool_call.2["cmd"], "count blue.tiles",
        "double-encoded arguments parsed to a structured input"
    );

    // The function_call_output decodes to a ToolResult correlated by the SAME call_id,
    // with the tool name recovered from the paired call.
    let tool_result = decoded
        .messages
        .iter()
        .filter(|message| message.role == Role::Tool)
        .flat_map(|message| &message.content)
        .find_map(|block| match &block.kind {
            ContentKind::ToolResult {
                id,
                tool_name,
                output,
                ..
            } => Some((id, tool_name, output)),
            _ => None,
        })
        .expect("the function_call_output decoded to a typed ToolResult");
    assert_eq!(tool_result.0, call_id, "pairing keys on call_id");
    assert_eq!(
        tool_result.1, "sample_probe",
        "tool name recovered from the paired function_call"
    );
    match &tool_result.2.kind {
        ToolResultOutputKind::Text { text } => {
            assert!(
                text.contains("3 blue.tiles"),
                "the plain-string output rode through as text"
            );
        }
        other => panic!("expected the plain-string output as Text, got {other:?}"),
    }
}

#[test]
fn repeated_call_ids_resolve_within_their_own_function_call_groups() {
    let body = json!({
        "model": "gpt-5-codex",
        "input": [
            {"type": "function_call", "call_id": "call_0", "name": "search", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call_0", "output": "search-result"},
            {"type": "function_call", "call_id": "call_0", "name": "read", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call_0", "output": "read-result"}
        ]
    });

    let decoded = responses_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
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
    for item in body["input"].as_array().unwrap() {
        if item["type"] == "function_call" {
            legacy_global_names.insert(
                item["call_id"].as_str().unwrap().to_string(),
                item["name"].as_str().unwrap().to_string(),
            );
        }
    }
    let legacy_result_names = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| legacy_global_names[item["call_id"].as_str().unwrap()].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        legacy_result_names,
        ["read", "read"],
        "an input-global last-write-wins map renames the first result"
    );
}

#[derive(Deserialize)]
struct CapturedOutputItemDone {
    item: Box<RawValue>,
}

fn captured_response_reasoning_raw() -> String {
    let sse = std::str::from_utf8(include_bytes!(
        "../fixtures/codex_reasoning_turn_response.sse"
    ))
    .unwrap();
    sse.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|payload| {
            serde_json::from_str::<Value>(payload)
                .ok()
                .map(|v| (payload, v))
        })
        .find_map(|(payload, event)| {
            (event.get("type").and_then(Value::as_str) == Some("response.output_item.done")
                && event
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    == Some("reasoning"))
            .then(|| {
                serde_json::from_str::<CapturedOutputItemDone>(payload)
                    .unwrap()
                    .item
                    .get()
                    .to_owned()
            })
        })
        .expect("capture contains a completed reasoning item")
}

fn normalized_captured_reasoning_events() -> Vec<NormalizedEvent> {
    let sse = include_bytes!("../fixtures/codex_reasoning_turn_response.sse");
    let family = OpenAiResponsesChatgpt;
    let config = openai_responses_chatgpt_frozen_config("gpt-5.5");
    let mut normalizer = family.normalizer(&config, broca_protocol::StepId(1));
    let mut events = Vec::new();
    for chunk in sse.chunks(97) {
        events.extend(
            normalizer
                .push(chunk)
                .expect("real codex SSE normalizes without a protocol error"),
        );
    }
    events.extend(normalizer.finish().expect("EOF finalization is clean"));
    events
}

fn normalized_captured_reasoning_message() -> broca_protocol::AssistantMessage {
    normalized_captured_reasoning_events()
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("capture assembles an assistant message")
}

#[test]
fn captured_codex_terminal_envelope_preserves_output_contract() {
    let events = normalized_captured_reasoning_events();
    let terminal_controls: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
                    | NormalizedEvent::Control(ProviderControl::StepFinished { .. })
                    | NormalizedEvent::Control(ProviderControl::Finished { .. })
            )
        })
        .collect();
    assert_eq!(terminal_controls.len(), 3);

    let (step_usage, step_reason) = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished {
                usage,
                finish_reason,
                ..
            }) => Some((*usage, *finish_reason)),
            _ => None,
        })
        .expect("captured response emits StepFinished");
    assert_eq!(step_usage.input_tokens, Some(4_267));
    assert_eq!(step_usage.cached_input_tokens, Some(9_600));
    assert_eq!(step_usage.fill_tokens(), Some(13_867));
    assert_eq!(step_usage.output_tokens, Some(178));
    assert_eq!(step_usage.reasoning_tokens, Some(98));
    assert_eq!(step_reason, FinishReason::Stop);
    assert!(events.iter().any(|event| matches!(
        event,
        NormalizedEvent::Control(ProviderControl::Finished {
            usage,
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }) if *usage == step_usage
    )));

    let message = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("captured response emits AssistantMessage");
    let ContentBlock::RedactedReasoning { data, .. } = &message.content[0] else {
        panic!("captured reasoning item must remain first");
    };
    let normalized: Value = serde_json::from_str(data).unwrap();
    let expected: Value = serde_json::from_str(&captured_response_reasoning_raw()).unwrap();
    assert_eq!(
        normalized["encrypted_content"].as_str().unwrap().as_bytes(),
        expected["encrypted_content"].as_str().unwrap().as_bytes(),
        "captured encrypted_content bytes must remain unchanged"
    );
}

#[test]
fn captured_codex_reasoning_sse_preserves_the_complete_item_in_output_order() {
    let message = normalized_captured_reasoning_message();
    let raw_expected = captured_response_reasoning_raw();
    let ContentBlock::RedactedReasoning {
        data,
        provider_format,
    } = &message.content[0]
    else {
        panic!(
            "reasoning output item must remain first: {:?}",
            message.content
        );
    };
    assert_eq!(
        *provider_format,
        Some(EncryptedReasoningFormat::OpenAiResponsesChatgpt)
    );
    assert_eq!(data.as_bytes(), raw_expected.as_bytes());
    let captured: Value = serde_json::from_str(data).unwrap();
    assert!(
        captured.get("id").is_some(),
        "the lossless capture keeps id"
    );
    assert!(
        captured.get("metadata").is_some(),
        "the lossless capture keeps metadata"
    );
    let ciphertext = captured["encrypted_content"].as_str().unwrap();
    assert_eq!(ciphertext.len(), 1_444);
    let Some(ContentBlock::TextWithState { provider_state, .. }) = message.content.get(1) else {
        panic!("captured phase-bearing message must follow reasoning");
    };
    let state: Value = serde_json::from_str(&provider_state.data).unwrap();
    assert_eq!(state["phase"], "final_answer");
}

#[test]
fn captured_codex_reasoning_renders_the_native_continuation_item() {
    let captured = normalized_captured_reasoning_message();
    let model = ModelHandle::new("openai", "gpt-5.5");
    let mut opts = CallOptions::new(vec![
        Message::assistant_from(captured.content, model.clone()),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Now continue.".into(),
            }],
        ),
    ]);
    opts.target_model = Some(model);
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.5");
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "medium".into(),
        summary: None,
    };

    let conformance = check_render_determinism(&OpenAiResponsesChatgpt, &frozen, &opts);
    assert!(
        conformance.passed(),
        "captured reasoning render is not deterministic: {:?}",
        conformance.failures()
    );
    let rendered: Value =
        serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, &opts)).unwrap();
    let actual = rendered["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        .expect("same-model continuation echoes reasoning");
    let native: Value = serde_json::from_slice(include_bytes!(
        "../fixtures/codex_resume_turn_with_echoed_reasoning.json"
    ))
    .unwrap();
    let expected = native["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        .unwrap();
    assert_eq!(
        broca_protocol::canonical_json_bytes(actual),
        broca_protocol::canonical_json_bytes(expected),
        "render edge must match the proven native request item exactly"
    );
    assert_eq!(actual["encrypted_content"].as_str().unwrap().len(), 1_444);
    assert_eq!(rendered["include"], json!(["reasoning.encrypted_content"]));
}

#[test]
fn captured_codex_reasoning_is_stripped_for_a_different_target_model() {
    let captured = normalized_captured_reasoning_message();
    let mut opts = CallOptions::new(vec![Message::assistant_from(
        captured.content,
        ModelHandle::new("openai", "gpt-5.5"),
    )]);
    opts.target_model = Some(ModelHandle::new("openai", "gpt-5.6"));
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.6");
    frozen.reasoning = ReasoningPolicy::OpenAiResponsesReasoning {
        effort: "medium".into(),
        summary: None,
    };

    let rendered: Value =
        serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, &opts)).unwrap();
    assert!(rendered["input"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| { item.get("type").and_then(Value::as_str) != Some("reasoning") }));
}

#[test]
fn unknown_and_malformed_items_decode_to_opaque_not_error() {
    let body = json!({
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": [
            {"type": "totally_future_item", "payload": {"nested": [1, 2, 3]}},
            {"type": "reasoning", "id": "rs_secret", "summary": [], "encrypted_content": "enc_blob"},
            {"type": "function_call", "id": "fc_bad", "call_id": "call_bad", "name": "read_file", "arguments": "{not json"},
            {"type": "message", "role": "reviewer", "content": "mystery role"}
        ]
    });

    let decoded = responses_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(reconstruct_input(&decoded.messages), body["input"]);

    let assistant = decoded
        .messages
        .iter()
        .find(|message| message.role == Role::Assistant)
        .unwrap();
    assert_eq!(
        opaque_block(&assistant.content[0]).kind,
        "totally_future_item"
    );
    let reasoning = opaque_block(&assistant.content[1]);
    assert_eq!(reasoning.kind, "reasoning");
    assert_eq!(
        reasoning.arc.as_ref().unwrap().kind,
        OpaqueArcKind::Reasoning
    );
    assert_eq!(reasoning.arc.as_ref().unwrap().id, "rs_secret");
    assert_eq!(reasoning.arc.as_ref().unwrap().role, OpaqueArcRole::Request);
    assert_eq!(opaque_block(&assistant.content[2]).kind, "function_call");

    let user_opaque = decoded
        .messages
        .iter()
        .find(|message| message.role == Role::User)
        .unwrap();
    assert_eq!(opaque_block(&user_opaque.content[0]).kind, "message");
}

#[test]
fn reasoning_pairs_with_following_function_call_inside_one_assistant_message() {
    let reasoning = json!({
        "type": "reasoning",
        "id": "rs_1",
        "summary": [{"type": "summary_text", "text": "thinking"}],
        "encrypted_content": "enc_1"
    });
    let function_call = json!({
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_1",
        "name": "read_file",
        "arguments": "{\"path\":\"src/main.rs\"}"
    });
    let function_call_output = json!({
        "type": "function_call_output",
        "call_id": "call_1",
        "output": [{"type": "text", "text": "fn main() {}"}]
    });
    let body = json!({
        "model": "gpt-5-codex",
        "store": false,
        "include": ["reasoning.encrypted_content"],
        "input": [reasoning.clone(), function_call.clone(), function_call_output.clone()]
    });

    let decoded = responses_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(decoded.messages.len(), 2);
    let assistant = &decoded.messages[0];
    assert_eq!(assistant.role, Role::Assistant);
    assert_eq!(assistant.content.len(), 2);

    let opaque = opaque_block(&assistant.content[0]);
    assert_eq!(opaque.kind, "reasoning");
    assert_eq!(
        serde_json::from_slice::<Value>(&opaque.raw).unwrap(),
        reasoning
    );
    assert_eq!(opaque.arc.as_ref().unwrap().kind, OpaqueArcKind::Reasoning);
    assert_eq!(opaque.arc.as_ref().unwrap().id, "rs_1");
    assert_eq!(opaque.arc.as_ref().unwrap().role, OpaqueArcRole::Request);

    match &assistant.content[1].kind {
        ContentKind::ToolCall {
            id, name, input, ..
        } => {
            assert_eq!(id, "call_1");
            assert_eq!(name, "read_file");
            assert_eq!(input, &json!({"path": "src/main.rs"}));
            assert_eq!(
                assistant.content[1].provider_extras["openai_responses"]["id"],
                json!("fc_1")
            );
        }
        other => panic!("expected tool call, got {other:?}"),
    }

    let tool_message = &decoded.messages[1];
    assert_eq!(tool_message.role, Role::Tool);
    match &tool_message.content[0].kind {
        ContentKind::ToolResult { id, output, .. } => {
            assert_eq!(id, "call_1");
            match &output.kind {
                ToolResultOutputKind::Content { blocks } => {
                    assert!(matches!(blocks[0].kind, ResultBlockKind::Text { .. }));
                }
                other => panic!("expected nested content output, got {other:?}"),
            }
        }
        other => panic!("expected tool result, got {other:?}"),
    }
}

#[test]
fn decode_error_is_reserved_for_structural_garbage() {
    assert!(matches!(
        responses_decode::decode(b"not json"),
        Err(responses_decode::DecodeError::InvalidJson(_))
    ));
    assert!(matches!(
        responses_decode::decode(br#"{"input":42}"#),
        Err(responses_decode::DecodeError::InputNotArray)
    ));
    assert!(matches!(
        responses_decode::decode(br#"{"input":[{"role":"user"}]}"#),
        Err(responses_decode::DecodeError::ItemWithoutType { index: 0 })
    ));
}

#[test]
fn message_part_arrays_decode_one_text_block_per_part_in_order() {
    let body = json!({
        "model": "gpt-5-codex",
        "input": [{
            "type": "message",
            "role": "assistant",
            "content": [
                {"type": "output_text", "text": "alpha"},
                {"type": "text", "text": "beta"},
                {"type": "refusal", "text": "nope"}
            ]
        }]
    });

    let decoded = responses_decode::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
    let assistant = &decoded.messages[0];
    assert_eq!(assistant.role, Role::Assistant);
    assert_eq!(assistant.content.len(), 3);
    assert_eq!(text_of(&assistant.content[0]), "alpha");
    assert_eq!(text_of(&assistant.content[1]), "beta");
    assert_eq!(opaque_block(&assistant.content[2]).kind, "refusal");
}

fn realistic_codex_fixture() -> Value {
    json!({
        "model": "gpt-5-codex",
        "store": false,
        "stream": true,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": "sess_123",
        "instructions": "Arrange imaginary blue tiles.",
        "tools": [
            {
                "type": "function",
                "name": "read_file",
                "description": "Read a file from disk.",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                },
                "strict": false
            },
            {
                "type": "web_search_preview",
                "search_context_size": "medium"
            }
        ],
        "reasoning": {"effort": "medium", "summary": "auto"},
        "input": [
            {"type": "message", "role": "user", "content": "Open src/main.rs"},
            {"type": "message", "role": "assistant", "content": "I will inspect the file."},
            {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "read_file", "arguments": "{\"path\":\"src/main.rs\"}"},
            {"type": "function_call_output", "call_id": "call_1", "output": "fn main() {}"}
        ]
    })
}

fn expected_residual(body: &Value) -> Value {
    let mut residual = body.as_object().unwrap().clone();
    residual.remove("model");
    residual.remove("input");
    residual.remove("instructions");
    Value::Object(residual)
}

fn reconstruct_input(messages: &[CkMessage]) -> Value {
    let mut items = Vec::new();
    for message in messages {
        if message.role == Role::System {
            continue;
        }
        match message.role {
            Role::Assistant => reconstruct_assistant_message(message, &mut items),
            Role::User | Role::System => reconstruct_plain_message(message, &mut items),
            Role::Tool => reconstruct_tool_message(message, &mut items),
        }
    }
    Value::Array(items)
}

fn reconstruct_plain_message(message: &CkMessage, items: &mut Vec<Value>) {
    let role = match message.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => unreachable!(),
    };
    let mut parts = Vec::new();
    let mut text = String::new();
    let mut text_only = true;
    for block in &message.content {
        match &block.kind {
            ContentKind::Text { text: part } => text.push_str(part),
            ContentKind::Opaque(opaque) if raw_type_is_message_part(opaque) => {
                text_only = false;
                parts.push(serde_json::from_slice(&opaque.raw).unwrap())
            }
            ContentKind::Opaque(opaque) => {
                text_only = false;
                items.push(serde_json::from_slice(&opaque.raw).unwrap())
            }
            other => panic!("cannot reconstruct plain message block {other:?}"),
        }
    }
    if text_only {
        if !text.is_empty() {
            items.push(json!({"type": "message", "role": role, "content": text}));
        }
        return;
    }
    if !text.is_empty() {
        parts.insert(0, json!({"type": "text", "text": text}));
    }
    if parts.is_empty() {
        return;
    }
    items.push(json!({"type": "message", "role": role, "content": parts}));
}

fn reconstruct_assistant_message(message: &CkMessage, items: &mut Vec<Value>) {
    let mut parts = Vec::new();
    let mut text = String::new();
    for block in &message.content {
        match &block.kind {
            ContentKind::Text { text: part } => text.push_str(part),
            ContentKind::ToolCall {
                id, name, input, ..
            } => {
                flush_assistant_parts(&mut text, &mut parts, items);
                let mut item = serde_json::Map::new();
                item.insert("type".into(), json!("function_call"));
                if let Some(item_id) = block
                    .provider_extras
                    .get("openai_responses")
                    .and_then(|family| family.get("id"))
                {
                    item.insert("id".into(), item_id.clone());
                }
                item.insert("call_id".into(), json!(id));
                item.insert("name".into(), json!(name));
                item.insert(
                    "arguments".into(),
                    json!(serde_json::to_string(input).unwrap()),
                );
                items.push(Value::Object(item));
            }
            ContentKind::Opaque(opaque) if raw_type_is_message_part(opaque) => {
                if !text.is_empty() {
                    parts.push(json!({"type": "output_text", "text": std::mem::take(&mut text)}));
                }
                parts.push(serde_json::from_slice(&opaque.raw).unwrap())
            }
            ContentKind::Opaque(opaque) => {
                flush_assistant_parts(&mut text, &mut parts, items);
                items.push(serde_json::from_slice(&opaque.raw).unwrap());
            }
            other => panic!("cannot reconstruct assistant block {other:?}"),
        }
    }
    flush_assistant_parts(&mut text, &mut parts, items);
}

fn reconstruct_tool_message(message: &CkMessage, items: &mut Vec<Value>) {
    for block in &message.content {
        match &block.kind {
            ContentKind::ToolResult { id, output, .. } => {
                items.push(json!({
                    "type": "function_call_output",
                    "call_id": id,
                    "output": reconstruct_tool_output(output)
                }));
            }
            ContentKind::Opaque(opaque) => items.push(serde_json::from_slice(&opaque.raw).unwrap()),
            other => panic!("cannot reconstruct tool block {other:?}"),
        }
    }
}

fn reconstruct_tool_output(output: &broca_ck::ToolResultOutput) -> Value {
    match &output.kind {
        ToolResultOutputKind::Text { text } => json!(text),
        ToolResultOutputKind::Json { value } => value.clone(),
        ToolResultOutputKind::ErrorText { text } => json!(text),
        ToolResultOutputKind::ErrorJson { value } => value.clone(),
        ToolResultOutputKind::ExecutionDenied { reason } => {
            json!(reason.clone().unwrap_or_default())
        }
        ToolResultOutputKind::Content { blocks }
        | ToolResultOutputKind::ErrorContent { blocks } => {
            // Responses has no error bit on function_call_output, so both CK content variants
            // use the same block rendering on this wire.
            Value::Array(blocks.iter().map(reconstruct_result_block).collect())
        }
    }
}

fn reconstruct_result_block(block: &ResultBlock) -> Value {
    match &block.kind {
        ResultBlockKind::Text { text } => json!({"type": "text", "text": text}),
        ResultBlockKind::Opaque { opaque } => serde_json::from_slice(&opaque.raw).unwrap(),
        other => panic!("cannot reconstruct result block {other:?}"),
    }
}

fn flush_assistant_parts(text: &mut String, parts: &mut Vec<Value>, items: &mut Vec<Value>) {
    if parts.is_empty() {
        if !text.is_empty() {
            items.push(json!({
                "type": "message",
                "role": "assistant",
                "content": std::mem::take(text)
            }));
        }
        return;
    }
    if !text.is_empty() {
        parts.insert(
            0,
            json!({"type": "output_text", "text": std::mem::take(text)}),
        );
    }
    items.push(json!({
        "type": "message",
        "role": "assistant",
        "content": std::mem::take(parts)
    }));
}

fn raw_type_is_message_part(opaque: &OpaqueBlock) -> bool {
    matches!(
        opaque.kind.as_str(),
        "input_text" | "output_text" | "text" | "refusal"
    )
}

fn opaque_block(block: &CkContentBlock) -> &OpaqueBlock {
    match &block.kind {
        ContentKind::Opaque(opaque) => opaque,
        other => panic!("expected opaque block, got {other:?}"),
    }
}

fn text_of(block: &CkContentBlock) -> &str {
    match &block.kind {
        ContentKind::Text { text } => text,
        other => panic!("expected text block, got {other:?}"),
    }
}

#[test]
fn test_duplicate_keys_responses() {
    // 4. Same trio for responses_decode with its shapes (duplicate type on an input item, duplicate role on a message item, top-level duplicate model).

    // Top-level duplicate model:
    let body_top_dup = br#"{
        "model": "gpt-5-codex-a",
        "model": "gpt-5-codex-b",
        "input": [
            { "type": "message", "role": "user", "content": "hello" }
        ]
    }"#;
    let decoded = responses_decode::decode(body_top_dup).unwrap();
    assert_eq!(decoded.wire_model_id, "gpt-5-codex-b");

    // Duplicate role on a message item:
    let body_msg_dup = br#"{
        "model": "gpt-5-codex",
        "input": [
            { "type": "message", "role": "assistant", "role": "user", "content": "hello" }
        ]
    }"#;
    let decoded = responses_decode::decode(body_msg_dup).unwrap();
    assert_eq!(decoded.messages[0].role, Role::User);

    // Duplicate type on an input item:
    let body_item_dup = br#"{
        "model": "gpt-5-codex",
        "input": [
            { "type": "totally_future_item", "type": "message", "role": "user", "content": "hello" }
        ]
    }"#;
    let decoded = responses_decode::decode(body_item_dup).unwrap();
    assert_eq!(decoded.messages[0].role, Role::User);
    match &decoded.messages[0].content[0].kind {
        ContentKind::Text { text } => assert_eq!(text, "hello"),
        other => panic!("expected text block, got {:?}", other),
    }
}

#[test]
fn openai_encrypted_reasoning_never_renders_into_other_wire_families() {
    let block = normalized_captured_reasoning_message().content.remove(0);
    let model = ModelHandle::new("shared-provider-id", "shared-model-id");
    let mut opts = CallOptions::new(vec![Message::assistant_from(vec![block], model.clone())]);
    // Deliberately make the producing-model check pass. The provider-format field must still
    // prevent a different request grammar from emitting the ciphertext.
    opts.target_model = Some(model);

    let anthropic = String::from_utf8(
        AnthropicMessages.render(&anthropic_frozen_config("shared-model-id"), &opts),
    )
    .unwrap();
    let gemini =
        String::from_utf8(Gemini.render(&gemini_frozen_config("shared-model-id"), &opts)).unwrap();
    let platform = String::from_utf8(
        OpenAiResponses.render(&openai_responses_frozen_config("shared-model-id"), &opts),
    )
    .unwrap();
    assert!(!anthropic.contains("encrypted_content"));
    assert!(!anthropic.contains("gAAAAABqTV8"));
    assert!(!gemini.contains("encrypted_content"));
    assert!(!gemini.contains("gAAAAABqTV8"));
    assert!(!platform.contains("encrypted_content"));
    assert!(!platform.contains("gAAAAABqTV8"));
}

#[test]
fn recognized_item_extensions_stay_on_typed_tool_calls() {
    let body = br#"{"model":"gpt-5-codex","input":[{"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":"{}","metadata":{"z":1,"a":2}}]}"#;
    let decoded = responses_decode::decode(body).expect("decodes");
    let block = &decoded.messages[0].content[0];

    assert!(matches!(block.kind, ContentKind::ToolCall { ref id, .. } if id == "call_1"));
    assert_eq!(
        block.provider_extras["openai"]["metadata"],
        json!({"z": 1, "a": 2})
    );
    let wire = serde_json::to_vec(&decoded.messages).expect("serialize CK messages");
    let wire = String::from_utf8(wire).expect("CK JSON is UTF-8");
    assert!(
        wire.contains(r#""metadata":{"z":1,"a":2}"#),
        "retained extension value keeps source key order: {wire}"
    );
    let round_tripped: Vec<CkMessage> = serde_json::from_str(&wire).expect("parse CK messages");
    let round_tripped = serde_json::to_string(&round_tripped).expect("serialize CK messages again");
    assert!(round_tripped.contains(r#""metadata":{"z":1,"a":2}"#));
}
