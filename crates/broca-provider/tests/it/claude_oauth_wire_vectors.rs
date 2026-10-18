//! The shared Claude OAuth wire vectors (Claude Code 2.1.302 persona) against broca.
//!
//! Each vector is a request built and signed from fixed inputs by the anthropic-auth client
//! integration for OpenCode. These tests compare broca's Claude Code headers, body fields,
//! signature, URL and key order with those reference requests.
//!
//! broca matches the vectors exactly on the persona: the billing block (apart from
//! `cc_prev_req`, which broca does not send), the `cch` rule, the URL, every Claude Code header
//! and its value and order, `metadata.user_id`, and the top-level key order. Request-construction
//! differences are pinned in `DIFFERENCES`. The raw vector betas are exact; the render comparison
//! omits only OpenCode's host streaming beta and its server-side fallback feature.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, Tool, ToolChoice},
    ToolCallId,
};
use broca_provider::claude_code_persona as persona;
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::variant::{FrozenVariant, VariantRequest};
use broca_provider::wire::{AuthHandle, ClaudeCodePersona, FrozenRenderConfig, WireFamily};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn vectors() -> Value {
    serde_json::from_str(cortexkit_anthropic_policy::vectors::VECTORS_JSON)
        .expect("the shared vectors are JSON")
}

fn cases() -> Vec<Value> {
    let vectors = vectors();
    assert_eq!(vectors["schema"], "claude-oauth-wire-vectors/v1");
    let cases = vectors["cases"].as_array().expect("cases").clone();
    assert_eq!(cases.len(), 7, "the vector set has seven cases");
    cases
}

fn output_body(case: &Value) -> &str {
    case["output"]["body"].as_str().expect("output body")
}

fn header<'a>(case: &'a Value, name: &str) -> Option<&'a str> {
    case["output"]["headers_wire_order"]
        .as_array()?
        .iter()
        .find(|pair| {
            pair[0]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
        .and_then(|pair| pair[1].as_str())
}

#[test]
fn the_shared_vectors_match_their_published_checksums() {
    use cortexkit_anthropic_policy::vectors::{FILES, SHA256SUMS};
    let mut checked = 0;
    for line in SHA256SUMS.lines().filter(|line| !line.trim().is_empty()) {
        let (digest, name) = line.split_once("  ").expect("`<sha256>  <file>` lines");
        let (_, contents) = FILES
            .iter()
            .find(|(file, _)| *file == name)
            .unwrap_or_else(|| panic!("{name} is listed but not shipped"));
        let actual: String = Sha256::digest(contents.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(actual, digest, "{name} differs from its published checksum");
        checked += 1;
    }
    assert_eq!(checked, 2, "vectors.json and README.md are both checked");
}

#[test]
fn primitives_reproduce_the_vector_unit_checks() {
    let primitives = &vectors()["primitives"];
    assert_eq!(
        persona::compute_cch(b"hello world test message"),
        primitives["computeCCH_utf8_hello_world_test_message"]
    );
    assert_eq!(
        persona::cc_version_suffix("short", "2.1.258"),
        primitives["computeCcVersionSuffix_short_2_1_258"]
    );
}

#[test]
fn each_vector_suffix_follows_from_its_first_user_text() {
    for case in cases() {
        let id = case["id"].as_str().unwrap();
        let body: Value = serde_json::from_str(output_body(&case)).unwrap();
        let expected = &case["output"]["cc_version_suffix"];
        let text = persona::first_user_message_text(body["messages"].as_array().unwrap());
        assert_eq!(text, expected["first_user_text"], "{id}: first user text");
        assert_eq!(
            persona::cc_version_suffix(&text, persona::CLAUDE_CODE_VERSION),
            expected["suffix"],
            "{id}: cc_version suffix"
        );
    }
}

#[test]
fn each_vector_cch_follows_from_its_canonical_body_and_signs_in_place() {
    for case in cases() {
        let id = case["id"].as_str().unwrap();
        let body = output_body(&case);
        let cch = case["output"]["cch"].as_str().unwrap();
        let offset = persona::cch_placeholder_offset(body).expect("billing block located");
        assert_eq!(&body[offset..offset + 5], cch, "{id}: placeholder location");

        let mut unsigned = body.to_string();
        unsigned.replace_range(offset..offset + 5, persona::CCH_PLACEHOLDER);
        let canonical = persona::cch_canonical_body(&unsigned).expect("canonical body");
        assert_eq!(
            canonical, case["output"]["cch_canonical_body"],
            "{id}: canonical body"
        );
        assert_eq!(persona::compute_cch(canonical.as_bytes()), cch, "{id}: cch");
        assert_eq!(persona::sign_body(&unsigned), body, "{id}: signing");
        assert_eq!(persona::sign_body(body), body, "{id}: re-signing is stable");
        assert_eq!(
            body.find("x-anthropic-billing-header").map(|at| {
                let end = body[at..].find('"').unwrap();
                &body[at..at + end]
            }),
            case["output"]["billing_header"].as_str(),
            "{id}: billing header field"
        );
    }
}

#[test]
fn each_vector_beta_list_follows_from_its_body_model_and_host_betas() {
    for case in cases() {
        let id = case["id"].as_str().unwrap();
        let body: Value = serde_json::from_str(output_body(&case)).unwrap();
        let host: Vec<&str> = case["input"]["host_headers"]["anthropic-beta"]
            .as_str()
            .into_iter()
            .collect();
        assert_eq!(
            persona::select_betas(&body, case["input"]["model"].as_str().unwrap(), &host),
            header(&case, "anthropic-beta").unwrap(),
            "{id}: exact beta list"
        );
    }
}

#[test]
fn each_vector_body_is_the_persona_serialisation_of_itself() {
    for case in cases() {
        let id = case["id"].as_str().unwrap();
        let body = output_body(&case);
        let parsed: Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            persona::to_persona_json(&parsed),
            body,
            "{id}: key order and number spelling"
        );
    }
}

/// The Claude Code persona the vectors were generated with (`fixed` in vectors.json).
fn fixed_persona(vectors: &Value) -> ClaudeCodePersona {
    let fixed = &vectors["fixed"];
    ClaudeCodePersona {
        version: persona::CLAUDE_CODE_VERSION.to_string(),
        device_id: fixed["deviceId"].as_str().unwrap().to_string(),
        account_uuid: Some(fixed["accountUuid"].as_str().unwrap().to_string()),
        session_id: fixed["claudeCodeSessionId"].as_str().unwrap().to_string(),
        prompt_id: Some(fixed["promptId"].as_str().unwrap().to_string()),
        cc_version_suffix: None,
        prompt_ordinal: None,
    }
}

/// anthropic-auth renames tools `mcp_<Name>`; broca sends the caller's name, so a vector's tool
/// names are mapped back to the host's (`mcp_Read` → `read`).
fn host_tool_name(wire: &str) -> String {
    let name = wire.strip_prefix("mcp_").unwrap_or(wire);
    let mut chars = name.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_lowercase().collect::<String>() + chars.as_str()
    })
}

/// The vector's conversation as broca's caller would send it: the host system blocks as one
/// system message, then the messages the vector finally sent (its internal effort messages,
/// which broca has no form for, are left out).
fn broca_request(case: &Value) -> CallOptions {
    let host: Value = serde_json::from_str(
        case["input"]
            .get("lowered_host_body")
            .unwrap_or(&case["input"]["host_body"])
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let sent: Value = serde_json::from_str(output_body(case)).unwrap();
    let mut prompt = vec![Message::new(
        Role::System,
        host["system"]
            .as_array()
            .unwrap()
            .iter()
            .map(|block| ContentBlock::Text {
                text: block["text"].as_str().unwrap().to_string(),
            })
            .collect(),
    )];
    for message in sent["messages"].as_array().unwrap() {
        let role = message["role"].as_str().unwrap();
        if role == "system" {
            continue;
        }
        let mut blocks = Vec::new();
        let mut results = Vec::new();
        for block in message["content"].as_array().unwrap() {
            match block["type"].as_str().unwrap() {
                "text" => blocks.push(ContentBlock::Text {
                    text: block["text"].as_str().unwrap().to_string(),
                }),
                "thinking" => blocks.push(ContentBlock::Reasoning {
                    text: block["thinking"].as_str().unwrap().to_string(),
                    signature: block["signature"].as_str().map(str::to_string),
                }),
                "tool_use" => blocks.push(ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(block["id"].as_str().unwrap().to_string()),
                    tool_name: host_tool_name(block["name"].as_str().unwrap()),
                    input: block["input"].clone(),
                })),
                "tool_result" => results.push(ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(block["tool_use_id"].as_str().unwrap().to_string()),
                    output: ToolOutput::Text {
                        text: block["content"].as_str().unwrap().to_string(),
                    },
                    is_error: false,
                })),
                other => panic!("no broca form for a {other} block"),
            }
        }
        if !results.is_empty() {
            prompt.push(Message::new(Role::Tool, results));
        }
        if !blocks.is_empty() {
            let role = if role == "user" {
                Role::User
            } else {
                Role::Assistant
            };
            prompt.push(Message::new(role, blocks));
        }
    }
    let mut opts = CallOptions::new(prompt);
    if let Some(tools) = sent["tools"].as_array() {
        opts.tools = tools
            .iter()
            .map(|tool| Tool {
                name: host_tool_name(tool["name"].as_str().unwrap()),
                description: tool["description"].as_str().unwrap().to_string(),
                input_schema: tool["input_schema"].clone(),
                module: None,
                execution_mode: Default::default(),
            })
            .collect::<Vec<_>>()
            .into();
        opts.tool_choice = ToolChoice::Auto.into();
    }
    opts
}

/// broca's render config for the vector: the persona fixed as the vector's, the model and
/// output cap the vector sent, the vector's thinking and effort as a reasoning variant, and
/// fast mode when the vector asked for it.
fn broca_frozen(case: &Value, vectors: &Value) -> FrozenRenderConfig {
    let sent: Value = serde_json::from_str(output_body(case)).unwrap();
    let mut frozen = anthropic_frozen_config(case["input"]["model"].as_str().unwrap());
    frozen.max_output_tokens = sent["max_tokens"].as_u64().unwrap() as u32;
    let mut persona = fixed_persona(vectors);
    persona.prompt_ordinal = Some(
        sent["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| {
                message["role"] == "user"
                    && message["content"].as_array().is_some_and(|blocks| {
                        blocks.iter().any(|block| block["type"] != "tool_result")
                    })
            })
            .count() as u64,
    );
    // anthropic-auth sends no lineage on a subagent's request. broca's request carries no
    // subagent marker, so a broca episode always sends its prompt id; the vector's subagent
    // case is rendered as an episode admitted without one.
    if case["input"]["subagent"] == true {
        persona.prompt_id = None;
    }
    // The suffix pinned at admission, from the conversation's first user message, as broca
    // freezes it for a new session; the vector's billing block must agree with it.
    persona.cc_version_suffix =
        broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript(
            &broca_request(case).prompt,
            &persona.version,
        );
    assert_eq!(
        persona.cc_version_suffix.as_deref(),
        case["output"]["cc_version_suffix"]["suffix"].as_str(),
        "the admission-time suffix matches the vector"
    );
    frozen.claude_code_persona = Some(persona);
    frozen.variant = Some(FrozenVariant {
        name: "vector".into(),
        request: VariantRequest::Anthropic {
            // anthropic-auth's `block_binding` comes from one of its account settings, which
            // broca has no counterpart for.
            thinking: sent.get("thinking").cloned().map(|mut thinking| {
                if let Some(object) = thinking.as_object_mut() {
                    object.remove("block_binding");
                }
                thinking
            }),
            effort: sent
                .pointer("/output_config/effort")
                .and_then(Value::as_str)
                .map(str::to_string),
        },
    });
    frozen.speed = sent["speed"].as_str().map(str::to_string);
    // A `json_schema` output is broca's structured output in its strict final-text form.
    if let Some(schema) = sent.pointer("/output_config/format/schema") {
        use broca_protocol::structured_output::{
            enforcement, FrozenStructuredOutput, Mode, SchemaDelivery, DEFAULT_NAME,
        };
        frozen.structured_output = Some(FrozenStructuredOutput {
            mode: Mode::FinalText,
            name: DEFAULT_NAME.into(),
            schema_jcs: serde_json::to_string(schema).unwrap(),
            enforcement: enforcement::ENFORCED.into(),
            reason: None,
            profile: None,
            delivery: SchemaDelivery::Strict,
        });
    }
    frozen
}

/// Per vector: the top-level fields whose values differ between broca's request and the
/// vector's (each a documented broca policy), and the top-level keys only the vector sends.
struct Difference {
    id: &'static str,
    differing_fields: &'static [&'static str],
    vector_only_keys: &'static [&'static str],
}

/// Reasons, shared by every vector:
/// - `system`: anthropic-auth rewrites OpenCode's own system paragraphs (drops `You are
///   OpenCode` and its links, rewrites two phrases) and sets its cache breakpoints; broca sends
///   its caller's system text as given and places breakpoints by its frozen cache policy (none
///   in these renders).
/// - `messages` / `tools` / top-level `cache_control`: anthropic-auth sets `cache_control` by its
///   own cache mode (its automatic mode is one top-level marker) and prefixes tool names `mcp_`;
///   broca keeps caller tool names and its own breakpoint policy.
/// - `diagnostics`, `fallbacks`, the `server-side-fallback` beta: OpenCode cache diagnostics and
///   server-side fallback, which broca does not send.
/// - `fine-grained-tool-streaming-2025-05-14`: a beta the OpenCode host sends, passed through;
///   broca has no host betas.
/// - `cc_prev_req` (checked in the billing test): broca does not send it.
/// - `thinking` differing by `block_binding`: an anthropic-auth account setting broca has no
///   counterpart for.
/// - `context_management`, the removal of a forced
///   `tool_choice`, and the inserted effort `role: system` message: anthropic-auth request
///   transforms broca's request model does not carry.
/// - Sending no `cc_prompt_id` on a subagent's request: broca's request has no subagent marker.
const DIFFERENCES: [Difference; 7] = [
    Difference {
        id: "fable51-first-turn",
        differing_fields: &["messages", "system"],
        vector_only_keys: &["diagnostics", "fallbacks"],
    },
    Difference {
        id: "opus55-continuation-replayed-thinking",
        differing_fields: &["messages", "system", "thinking"],
        vector_only_keys: &["diagnostics", "fallbacks"],
    },
    Difference {
        id: "opus55-effort-change",
        differing_fields: &["messages", "system"],
        vector_only_keys: &["diagnostics", "fallbacks"],
    },
    Difference {
        id: "sonnet55-between-tools",
        differing_fields: &["system"],
        vector_only_keys: &["cache_control", "diagnostics"],
    },
    Difference {
        id: "haiku55-tools-continuation",
        differing_fields: &["messages", "system", "tools"],
        vector_only_keys: &["diagnostics"],
    },
    Difference {
        id: "sonnet55-full-agent-subagent",
        differing_fields: &["messages", "system", "tools"],
        vector_only_keys: &["context_management", "diagnostics"],
    },
    Difference {
        id: "opus48-structured-output-fast",
        differing_fields: &["messages", "system"],
        vector_only_keys: &["diagnostics"],
    },
];

#[test]
fn broca_renders_each_vectors_persona_exactly() {
    let vectors = vectors();
    // Every documented-difference row is checked before failing, so one run shows them all.
    let mut table = Vec::new();
    let mut mismatched = false;
    for case in cases() {
        let id = case["id"].as_str().unwrap();
        let difference = DIFFERENCES
            .iter()
            .find(|difference| difference.id == id)
            .unwrap_or_else(|| panic!("{id}: no documented difference row"));
        let frozen = broca_frozen(&case, &vectors);
        let bytes = AnthropicMessages.render(&frozen, &broca_request(&case));
        let text = String::from_utf8(bytes.clone()).unwrap();
        let ours: Value = serde_json::from_str(&text).unwrap();
        let theirs: Value = serde_json::from_str(output_body(&case)).unwrap();

        // The bytes are the persona serialisation, signed over themselves.
        assert_eq!(persona::to_persona_json(&ours), text, "{id}: key order");
        assert_eq!(persona::sign_body(&text), text, "{id}: cch is current");
        let offset = persona::cch_placeholder_offset(&text).expect("billing block");
        assert_ne!(
            &text[offset..offset + 5],
            persona::CCH_PLACEHOLDER,
            "{id}: signed"
        );

        // The billing block: the vector's text without `cc_prev_req`, with broca's own cch.
        let theirs_billing = theirs["system"][0]["text"].as_str().unwrap();
        let without_prev = theirs_billing
            .split(' ')
            .filter(|field| !field.starts_with("cc_prev_req="))
            .collect::<Vec<_>>()
            .join(" ");
        let their_cch = format!("cch={};", case["output"]["cch"].as_str().unwrap());
        let our_cch = format!("cch={};", &text[offset..offset + 5]);
        assert_eq!(
            ours["system"][0],
            serde_json::json!({"type": "text", "text": without_prev.replace(&their_cch, &our_cch)}),
            "{id}: billing block"
        );
        assert_eq!(
            ours["system"][1], theirs["system"][1],
            "{id}: identity block"
        );
        assert_eq!(
            ours["system"].as_array().unwrap().len(),
            theirs["system"].as_array().unwrap().len(),
            "{id}: one system block per host block after billing and identity"
        );

        // Top-level keys: broca's are in the persona order, and the keys both send appear in
        // the same relative order.
        let our_keys: Vec<&str> = ours
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let their_order: Vec<&str> = {
            let mut order = Vec::new();
            for key in persona::TOP_LEVEL_KEY_ORDER {
                if output_body(&case).contains(&format!("\"{key}\":")) && theirs.get(key).is_some()
                {
                    order.push(key);
                }
            }
            order
        };
        let our_order: Vec<&str> = persona::TOP_LEVEL_KEY_ORDER
            .into_iter()
            .filter(|key| ours.get(*key).is_some())
            .collect();
        let shared: Vec<&str> = their_order
            .iter()
            .copied()
            .filter(|key| our_keys.contains(key))
            .collect();
        assert_eq!(
            our_order
                .iter()
                .copied()
                .filter(|key| shared.contains(key))
                .collect::<Vec<_>>(),
            shared,
            "{id}: relative key order"
        );

        // Field by field: equal except the documented differences.
        let mut differing = Vec::new();
        let mut vector_only = Vec::new();
        for (key, value) in theirs.as_object().unwrap() {
            match ours.get(key) {
                Some(our_value) if our_value == value => {}
                Some(_) => differing.push(key.as_str()),
                None => vector_only.push(key.as_str()),
            }
        }
        let broca_only: Vec<&str> = our_keys
            .iter()
            .copied()
            .filter(|key| theirs.get(*key).is_none())
            .collect();
        differing.sort_unstable();
        vector_only.sort_unstable();
        table.push(format!(
            "{id}: differing {differing:?} expected {:?}",
            difference.differing_fields
        ));
        if differing != difference.differing_fields {
            mismatched = true;
        }
        table.push(format!(
            "{id}: vector-only keys {vector_only:?} expected {:?}",
            difference.vector_only_keys
        ));
        if vector_only != difference.vector_only_keys {
            mismatched = true;
        }
        assert!(
            broca_only.is_empty() || broca_only == ["tool_choice"],
            "{id}: broca-only keys {broca_only:?}"
        );
        assert_eq!(
            ours["metadata"], theirs["metadata"],
            "{id}: metadata.user_id"
        );

        // The request: URL and every Claude Code header, in the vector's wire order.
        let request = AnthropicMessages.endpoint(
            &frozen,
            &AuthHandle::Bearer(vectors["fixed"]["accessToken"].as_str().unwrap().into()),
            bytes,
        );
        assert_eq!(request.url, case["output"]["url"], "{id}: url");
        let ours_beta = request
            .headers
            .iter()
            .find(|(name, _)| name == "anthropic-beta")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(
            ours_beta,
            persona::select_betas(&ours, &frozen.wire_model_id, &[]),
            "{id}: betas follow from the sent body"
        );
        // OpenCode supplies a host streaming beta and enables fallbacks=default; broca has
        // neither input. Every other beta, including JSON-schema output, must match in order.
        let reference_betas = header(&case, "anthropic-beta")
            .unwrap()
            .split(',')
            .filter(|beta| {
                !matches!(
                    *beta,
                    "fine-grained-tool-streaming-2025-05-14" | "server-side-fallback-2026-07-01"
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        assert_eq!(ours_beta, reference_betas, "{id}: production beta list");

        // Transport-owned headers (Connection, Host, Accept-Encoding, Content-Length) are the
        // HTTP client's; the machine-dependent and per-request ones are compared by form.
        let transport_owned = ["connection", "host", "accept-encoding", "content-length"];
        let theirs_headers: Vec<(String, String)> = case["output"]["headers_wire_order"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().unwrap().to_ascii_lowercase(),
                    pair[1].as_str().unwrap().to_string(),
                )
            })
            .filter(|(name, _)| !transport_owned.contains(&name.as_str()))
            .collect();
        let names = |headers: &[(String, String)]| -> Vec<String> {
            headers.iter().map(|(name, _)| name.clone()).collect()
        };
        assert_eq!(
            names(&request.headers),
            names(&theirs_headers),
            "{id}: header order"
        );
        for ((name, ours_value), (_, theirs_value)) in request.headers.iter().zip(&theirs_headers) {
            match name.as_str() {
                "anthropic-beta" => {}
                "x-client-request-id" => assert!(
                    ours_value.len() == 36 && ours_value.as_bytes()[14] == b'4',
                    "{id}: x-client-request-id is a v4 UUID"
                ),
                "x-stainless-os" | "x-stainless-arch" => assert!(!ours_value.is_empty()),
                _ => assert_eq!(ours_value, theirs_value, "{id}: header {name}"),
            }
        }
    }
    assert!(
        !mismatched,
        "documented differences do not match:\n{}",
        table.join("\n")
    );
}

#[test]
fn a_persona_runs_billing_block_is_the_same_on_every_step() {
    let vectors = vectors();
    let case = cases()
        .into_iter()
        .find(|case| case["id"] == "haiku55-tools-continuation")
        .unwrap();
    let frozen = broca_frozen(&case, &vectors);
    let full = broca_request(&case);
    // The same run one step earlier: the conversation before the tool result.
    let mut earlier = full.clone();
    earlier.prompt = full.prompt[..full.prompt.len() - 1].to_vec();
    let billing = |opts: &CallOptions| {
        let shaped = AnthropicMessages.shape(&frozen, opts);
        shaped["system"][0]["text"].as_str().unwrap().to_string()
    };
    assert_eq!(billing(&earlier), billing(&full));
    // Signed bodies differ only in their cch: the step's bytes change, its billing text does not.
    let first = String::from_utf8(AnthropicMessages.render(&frozen, &earlier)).unwrap();
    let second = String::from_utf8(AnthropicMessages.render(&frozen, &full)).unwrap();
    assert_ne!(first, second);
}

/// Diagnosis was null on measured cache hits; empty thinking plus a signature is also a
/// measured stream shape. The future event is synthetic: it checks forward compatibility,
/// not an undocumented beta event name, and its fake usage must not affect accounting.
#[test]
fn beta_response_extensions_and_empty_thinking_preserve_usage() {
    use broca_protocol::{NormalizedEvent, ProviderControl, StepId};
    let mut frozen = anthropic_frozen_config("claude-sonnet-5-5");
    frozen.claude_code_persona = Some(fixed_persona(&vectors()));
    let mut normalizer = AnthropicMessages.normalizer(&frozen, StepId(1));
    let bytes = include_bytes!("../fixtures/anthropic_beta_forward_compat.sse");
    let mut events = Vec::new();
    for chunk in bytes.chunks(17) {
        events.extend(normalizer.push(chunk).unwrap());
    }
    events.extend(normalizer.finish().unwrap());
    let usage = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(*usage),
            _ => None,
        })
        .expect("step finished");
    assert_eq!(usage.input_tokens, Some(10));
    assert_eq!(usage.cached_input_tokens, Some(40));
    assert_eq!(usage.cache_write_tokens, Some(6));
    assert_eq!(usage.output_tokens, Some(7));
    assert_eq!(normalizer.input_transformations(), Some(Vec::new()));
    assert!(events.iter().any(|event| matches!(event,
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message })
            if matches!(&message.content[0], ContentBlock::Reasoning { text, signature } if text.is_empty() && signature.as_deref() == Some("sig_beta"))
    )));
}

#[test]
fn persona_final_text_schema_selects_the_structured_output_beta() {
    let cases = cases();
    let case = cases
        .iter()
        .find(|case| case["id"] == "opus48-structured-output-fast")
        .unwrap();
    let mut frozen = broca_frozen(case, &vectors());
    let opts = broca_request(case);
    for has_schema in [true, false] {
        if !has_schema {
            frozen.structured_output = None;
        }
        let body = AnthropicMessages.render(&frozen, &opts);
        let request = AnthropicMessages.endpoint(&frozen, &AuthHandle::None, body);
        let betas = &request
            .headers
            .iter()
            .find(|(key, _)| key == "anthropic-beta")
            .unwrap()
            .1;
        assert_eq!(
            betas
                .split(',')
                .any(|beta| beta == "structured-outputs-2025-12-15"),
            has_schema
        );
        if has_schema {
            assert!(betas.contains("extended-cache-ttl-2025-04-11,structured-outputs-2025-12-15,cache-diagnosis-2026-04-07"));
        }
    }
}

/// cortexkit-anthropic-policy's test-vectors/claude-oauth-wire/README.md names summarized
/// thinking as the deliberate body difference from the CLI's thinking.display=updates.
/// Measured updates responses contain no thinking text, so broca keeps summarized display.
#[test]
fn adaptive_persona_thinking_keeps_summarized_display() {
    let mut checked = 0;
    for case in cases() {
        let sent: Value = serde_json::from_str(output_body(&case)).unwrap();
        if sent.pointer("/thinking/type").and_then(Value::as_str) != Some("adaptive") {
            continue;
        }
        let frozen = broca_frozen(&case, &vectors());
        let body = AnthropicMessages.shape(&frozen, &broca_request(&case));
        assert_eq!(
            body.pointer("/thinking/display").and_then(Value::as_str),
            Some("summarized")
        );
        assert_eq!(
            sent.pointer("/thinking/display").and_then(Value::as_str),
            Some("summarized")
        );
        checked += 1;
    }
    assert!(
        checked > 0,
        "the vector set exercises adaptive summarized thinking"
    );
}
