//! B4 — cross-family tool-call-id aliasing through the Anthropic render (CK#1 §5.6.1 r3).
//!
//! When a canonical tool-call id is INVALID for Anthropic's id grammar
//! (`^[a-zA-Z0-9_-]+$`, ≤ 64 chars) — e.g. an OpenAI-Responses 450-char `|` id carried in
//! from a provider switch — the render substitutes a deterministic Anthropic-valid alias,
//! applied to BOTH the tool_use and its tool_result so the arc still pairs.
//!
//! These tests are the C7 proof set: the alias is byte-affecting, so it MUST be a pure
//! deterministic function of durable state (the canonical id + the frozen target family)
//! and reproduce identically across a resume. The merge-invariant gate is deliberately NOT
//! used here — the alias is render-edge state (computed at render from the message's
//! canonical id + the frozen run-config family), not replay-walk state, so the fast-path-
//! vs-full-replay divergence class it guards against structurally cannot arise. The precise
//! C7 proof is render-twice determinism; a serde round-trip of the source Message then
//! re-render is byte-identical (the actual resume path, since the canonical id survives
//! storage and the alias recomputes); injectivity on the CK#1 collision corpus; and the
//! render-edge-only guard that an aliasing render does NOT mutate the source Message.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle},
    ToolCallId,
};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::WireFamily;
use serde_json::{json, Value};

const ANTHROPIC_MODEL: &str = "claude-opus-4-8";
const DERIVED_INVALID_ID: &str = "bad:id";
const DERIVED_LEGACY_ALIAS: &str = "t_Vpezy1fil0QVLlVzRotpU2";

/// An OpenAI-Responses-shaped id: long, with a `|` — invalid for Anthropic.
fn foreign_id(tag: &str) -> String {
    format!("call_{}|{}", "a".repeat(440), tag)
}

/// A prompt where a foreign-id tool call (from a prior provider) is answered by its result —
/// the provider-switch-continuation shape. Carrying a foreign origin so this is a real
/// cross-family continuation, not an owned Anthropic turn.
fn cross_family_prompt(id: &str) -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(id.into()),
                tool_name: "get_time".into(),
                input: json!({"tz": "UTC"}),
            })],
            ModelHandle::new("openai", "gpt-5"),
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text {
                    text: "12:00".into(),
                },
                is_error: false,
            })],
        ),
    ]
}

fn render(prompt: Vec<Message>) -> Value {
    serde_json::from_slice(&render_bytes(prompt)).unwrap()
}

fn render_bytes(prompt: Vec<Message>) -> Vec<u8> {
    let frozen = anthropic_frozen_config(ANTHROPIC_MODEL);
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(ModelHandle::new("anthropic", ANTHROPIC_MODEL));
    AnthropicMessages.render(&frozen, &opts)
}

/// The wire id on the single tool_use block, and the single tool_result's tool_use_id.
fn wire_ids(body: &Value) -> (String, String) {
    let messages = body["messages"].as_array().unwrap();
    let mut tool_use_id = None;
    let mut tool_result_id = None;
    for m in messages {
        for b in m["content"].as_array().unwrap() {
            match b["type"].as_str() {
                Some("tool_use") => tool_use_id = Some(b["id"].as_str().unwrap().to_string()),
                Some("tool_result") => {
                    tool_result_id = Some(b["tool_use_id"].as_str().unwrap().to_string())
                }
                _ => {}
            }
        }
    }
    (
        tool_use_id.expect("a tool_use"),
        tool_result_id.expect("a tool_result"),
    )
}

fn is_anthropic_valid(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn two_arc_prompt(first: &str, second: &str) -> Vec<Message> {
    vec![
        Message::assistant_from(
            vec![
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(first.into()),
                    tool_name: "first".into(),
                    input: json!({}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(second.into()),
                    tool_name: "second".into(),
                    input: json!({}),
                }),
            ],
            ModelHandle::new("openai", "gpt-5"),
        ),
        Message::new(
            Role::Tool,
            vec![
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(first.into()),
                    output: ToolOutput::Text { text: "a".into() },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(second.into()),
                    output: ToolOutput::Text { text: "b".into() },
                    is_error: false,
                }),
            ],
        ),
    ]
}

fn rendered_tool_ids(body: &Value) -> (Vec<String>, Vec<String>) {
    let mut calls = Vec::new();
    let mut results = Vec::new();
    for message in body["messages"].as_array().unwrap() {
        for block in message["content"].as_array().unwrap() {
            match block["type"].as_str() {
                Some("tool_use") => calls.push(block["id"].as_str().unwrap().to_owned()),
                Some("tool_result") => {
                    results.push(block["tool_use_id"].as_str().unwrap().to_owned())
                }
                _ => {}
            }
        }
    }
    (calls, results)
}

#[test]
fn foreign_id_is_aliased_to_a_valid_id_and_call_result_pair_match() {
    let id = foreign_id("x");
    let body = render(cross_family_prompt(&id));
    let (tu, tr) = wire_ids(&body);
    assert!(
        !is_anthropic_valid(&id),
        "the test id must be invalid for Anthropic"
    );
    assert!(
        is_anthropic_valid(&tu),
        "the rendered tool_use id must be a valid Anthropic id, got {tu:?}"
    );
    assert_ne!(tu, id, "the foreign id must have been aliased");
    assert_eq!(
        tu, tr,
        "the tool_use id and its tool_result tool_use_id must alias to the SAME wire id so \
         the arc still pairs"
    );
}

#[test]
fn alias_render_is_deterministic_across_passes() {
    // Render-twice with fresh inputs each time → byte-identical (no process-seeded hash, no
    // map iteration leak).
    let id = foreign_id("det");
    let a = AnthropicMessages.render(&anthropic_frozen_config(ANTHROPIC_MODEL), &{
        let mut o = CallOptions::new(cross_family_prompt(&id));
        o.target_model = Some(ModelHandle::new("anthropic", ANTHROPIC_MODEL));
        o
    });
    let b = AnthropicMessages.render(&anthropic_frozen_config(ANTHROPIC_MODEL), &{
        let mut o = CallOptions::new(cross_family_prompt(&id));
        o.target_model = Some(ModelHandle::new("anthropic", ANTHROPIC_MODEL));
        o
    });
    assert_eq!(
        a, b,
        "aliasing render must be byte-deterministic across passes"
    );
}

#[test]
fn alias_survives_message_serde_round_trip() {
    // THE RESUME PATH: the source messages are persisted as JSON (WAL/store) and rebuilt on
    // resume. The canonical id round-trips through serde unchanged, so the re-render
    // recomputes the IDENTICAL alias → byte-identical request. This is the C7 proof that
    // fits the alias (it is render-edge, recomputed from the durable canonical id), in place
    // of the merge-invariant gate (which exercises the stub family, not this alias).
    let id = foreign_id("rt");
    let prompt = cross_family_prompt(&id);

    let direct = render(prompt.clone());

    let json = serde_json::to_string(&prompt).unwrap();
    let restored: Vec<Message> = serde_json::from_str(&json).unwrap();
    let after_round_trip = render(restored);

    assert_eq!(
        direct, after_round_trip,
        "the alias must reproduce byte-identically after the source messages round-trip \
         through serialization (the resume path)"
    );
}

#[test]
fn aliasing_render_does_not_mutate_the_source_message() {
    // THE RENDER-EDGE-ONLY GUARD: the canonical id is the SOLE persisted form. Aliasing is a
    // render-time projection — it must NEVER write the alias back into a Message (a stored
    // tool_result carrying the aliased id would no longer be canonical, and a later render
    // would alias the already-aliased id → C7 resume breaks). Assert the source messages
    // still carry the canonical id after an aliasing render.
    let id = foreign_id("guard");
    let prompt = cross_family_prompt(&id);
    let _ = render(prompt.clone());
    for m in &prompt {
        for b in &m.content {
            match b {
                ContentBlock::ToolCall(tc) => assert_eq!(
                    tc.tool_call_id.0, id,
                    "the source tool_call must still hold the CANONICAL id after render"
                ),
                ContentBlock::ToolResult(tr) => assert_eq!(
                    tr.tool_call_id.0, id,
                    "the source tool_result must still hold the CANONICAL id after render"
                ),
                _ => {}
            }
        }
    }
}

#[test]
fn distinct_long_ids_sharing_a_64_char_prefix_get_distinct_aliases() {
    // CK#1 injectivity: the naive sanitize+truncate would collapse two distinct 450-char ids
    // that share a 64-char prefix into the same Anthropic id, merging two arcs. The
    // hash-carried alias keeps them DISTINCT (the entropy is in the hash, not a raw prefix).
    let shared_prefix = "b".repeat(70); // longer than Anthropic's 64-char budget
    let id1 = format!("call_{shared_prefix}|alpha|{}", "1".repeat(300));
    let id2 = format!("call_{shared_prefix}|beta|{}", "2".repeat(300));
    assert_eq!(
        &id1[..64],
        &id2[..64],
        "the two ids share a 64-char prefix by construction"
    );

    // Render a prompt with BOTH arcs; both must alias to valid, DISTINCT wire ids.
    let prompt = vec![
        Message::assistant_from(
            vec![
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id1.clone()),
                    tool_name: "a".into(),
                    input: json!({}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id2.clone()),
                    tool_name: "b".into(),
                    input: json!({}),
                }),
            ],
            ModelHandle::new("openai", "gpt-5"),
        ),
        Message::new(
            Role::Tool,
            vec![
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(id1.clone()),
                    output: ToolOutput::Text { text: "x".into() },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(id2.clone()),
                    output: ToolOutput::Text { text: "y".into() },
                    is_error: false,
                }),
            ],
        ),
    ];
    let body = render(prompt);
    let messages = body["messages"].as_array().unwrap();
    let wire_ids: Vec<String> = messages
        .iter()
        .flat_map(|m| m["content"].as_array().unwrap())
        .filter_map(|b| match b["type"].as_str() {
            Some("tool_use") => Some(b["id"].as_str().unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(wire_ids.len(), 2);
    assert!(wire_ids.iter().all(|w| is_anthropic_valid(w)));
    assert_ne!(
        wire_ids[0], wire_ids[1],
        "two distinct ids sharing a 64-char prefix must alias to DISTINCT wire ids \
         (hash-carried, not prefix-truncated)"
    );
}

#[test]
fn generated_alias_collision_with_valid_id_resolves_in_both_orders() {
    for (first, second) in [
        (DERIVED_INVALID_ID, DERIVED_LEGACY_ALIAS),
        (DERIVED_LEGACY_ALIAS, DERIVED_INVALID_ID),
    ] {
        let body = render(two_arc_prompt(first, second));
        let (calls, results) = rendered_tool_ids(&body);
        assert_eq!(
            calls, results,
            "each tool_use must still pair with its result"
        );
        assert_eq!(calls.len(), 2);
        assert_ne!(calls[0], calls[1], "the two arcs need distinct wire ids");
        assert!(calls.iter().all(|id| is_anthropic_valid(id)));
        let valid_position = usize::from(first == DERIVED_INVALID_ID);
        assert_eq!(
            calls[valid_position], DERIVED_LEGACY_ALIAS,
            "an already-valid canonical id keeps its own bytes even when it appears later"
        );
    }
}

#[test]
fn noncolliding_alias_render_keeps_legacy_bytes() {
    let aliased = render_bytes(cross_family_prompt(DERIVED_INVALID_ID));
    let identity = render_bytes(cross_family_prompt(DERIVED_LEGACY_ALIAS));
    assert_eq!(
        aliased, identity,
        "attempt zero must retain the exact pre-probe alias bytes, avoiding a renderer bump"
    );
}
