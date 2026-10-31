use super::*;
use broca_protocol::{CallOptions, ContentBlock, Message, Role, StepContext};
use serde_json::json;

fn families() -> [WireFamilyId; 7] {
    [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ]
}

fn message(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn options() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        message(Role::System, "stable instructions"),
        message(Role::User, "A"),
        message(Role::Assistant, "B"),
    ]);
    opts.step_context = Some(StepContext {
        run_id: "r".into(),
        step_index: 1,
        max_output_tokens: None,
        session_address: None,
        credential_id: None,
        account_ref: None,
    });
    opts
}

fn render(id: WireFamilyId, opts: &CallOptions) -> Vec<u8> {
    let mut frozen = crate::families::frozen_config_for_family(id, "model");
    frozen.project_id = Some("project".into());
    crate::registry::wire_family(id)
        .unwrap()
        .render(&frozen, opts)
}

#[test]
fn every_family_clean_append_is_not_a_divergence() {
    for id in families() {
        let old = options();
        let mut new = old.clone();
        new.prompt.push(message(Role::User, "C"));
        new.step_context.as_mut().unwrap().step_index = 2;
        let before = render(id, &old);
        let after = render(id, &new);
        let evidence = compare_rendered_requests(id, &before, &after).unwrap();
        assert!(evidence.extends_previous, "{id:?}: {evidence:?}");
        assert_eq!(evidence.first_divergence, None, "{id:?}");
        assert_eq!(
            before,
            render(id, &old),
            "comparison must not mutate a request"
        );
    }
}

#[test]
fn every_family_reports_system_tool_and_in_prefix_edits() {
    for id in families() {
        let old = options();
        let before = render(id, &old);
        let mut new = old.clone();
        new.prompt[0] = message(Role::System, "changed instructions");
        let evidence = compare_rendered_requests(id, &before, &render(id, &new)).unwrap();
        assert!(!evidence.extends_previous, "{id:?}");
        assert_eq!(
            evidence.first_divergence.unwrap().region,
            Region::System,
            "{id:?}"
        );
        new = old.clone();
        new.tools = std::sync::Arc::new(vec![serde_json::from_value(json!({"name":"read",
            "description":"read a file","input_schema":{"type":"object","properties":{}}}))
        .unwrap()]);
        let evidence = compare_rendered_requests(id, &before, &render(id, &new)).unwrap();
        assert!(!evidence.extends_previous, "{id:?}");
        assert_eq!(
            evidence.first_divergence.unwrap().region,
            Region::Tools,
            "{id:?}"
        );
        new = old.clone();
        new.prompt[1] = message(Role::User, "edited A");
        let evidence = compare_rendered_requests(id, &before, &render(id, &new)).unwrap();
        let divergence = evidence.first_divergence.unwrap();
        assert_eq!(divergence.region, Region::Messages, "{id:?}");
        assert!(divergence.message_index.is_some(), "{id:?}");
    }
}

#[test]
fn insertion_removal_and_edit_after_breakpoint_are_visible() {
    let id = WireFamilyId::OpenAiChat;
    let old = CallOptions::new(vec![
        message(Role::User, "A"),
        message(Role::Assistant, "B"),
    ]);
    let mut new = old.clone();
    new.prompt.insert(1, message(Role::User, "inserted"));
    let evidence = compare_rendered_requests(id, &render(id, &old), &render(id, &new)).unwrap();
    assert!(!evidence.extends_previous);
    assert_eq!(evidence.first_divergence.unwrap().message_index, Some(1));
    new = old.clone();
    new.prompt.pop();
    assert!(
        !compare_rendered_requests(id, &render(id, &old), &render(id, &new))
            .unwrap()
            .extends_previous
    );
    let old = json!({"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],
        "messages":[{"role":"user","content":[{"type":"text","text":"uncached"}]}]});
    let mut new = old.clone();
    new["messages"][0]["content"][0]["text"] = json!("edited uncached tail");
    let evidence = compare_rendered_requests(
        WireFamilyId::AnthropicMessages,
        &serde_json::to_vec(&old).unwrap(),
        &serde_json::to_vec(&new).unwrap(),
    )
    .unwrap();
    assert!(!evidence.extends_previous);
    assert_eq!(
        evidence.previous_last_breakpoint.unwrap().region,
        Region::System
    );
    assert_eq!(evidence.first_divergence.unwrap().region, Region::Messages);
}

#[test]
fn moved_anthropic_marker_extends_but_thinking_signature_does_not() {
    let id = WireFamilyId::AnthropicMessages;
    let mut opts = options();
    opts.prompt[2].content.insert(
        0,
        ContentBlock::Reasoning {
            text: "thought".into(),
            signature: Some("signed-one".into()),
        },
    );
    let mut old: Value = serde_json::from_slice(&render(id, &opts)).unwrap();
    old["messages"][0]["content"][0]["cache_control"] = json!({"type":"ephemeral"});
    let mut moved = old.clone();
    moved["messages"][0]["content"][0]
        .as_object_mut()
        .unwrap()
        .remove("cache_control");
    moved["messages"][1]["content"][1]["cache_control"] = json!({"type":"ephemeral","ttl":"1h"});
    let compare = |new: &Value| {
        compare_rendered_requests(
            id,
            &serde_json::to_vec(&old).unwrap(),
            &serde_json::to_vec(new).unwrap(),
        )
        .unwrap()
    };
    assert!(compare(&moved).extends_previous);
    assert_eq!(
        compare(&moved)
            .previous_last_breakpoint
            .unwrap()
            .message_index,
        Some(0)
    );
    moved["messages"][1]["content"][0]["signature"] = json!("signed-two");
    let divergence = compare(&moved).first_divergence.unwrap();
    assert_eq!(divergence.field.as_deref(), Some("thinking.signature"));
    assert_eq!(divergence.message_index, Some(1));
    assert_eq!(divergence.part_index, Some(0));
}

#[test]
fn bedrock_marker_blocks_do_not_shift_content_identity() {
    let id = WireFamilyId::BedrockConverse;
    let mut old: Value = serde_json::from_slice(&render(id, &options())).unwrap();
    old["messages"][0]["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"cachePoint":{"type":"default"}}));
    let mut new = old.clone();
    new["messages"][0]["content"].as_array_mut().unwrap().pop();
    new["messages"][1]["content"]
        .as_array_mut()
        .unwrap()
        .insert(0, json!({"cachePoint":{"type":"default"}}));
    let e = compare_rendered_requests(
        id,
        &serde_json::to_vec(&old).unwrap(),
        &serde_json::to_vec(&new).unwrap(),
    )
    .unwrap();
    assert!(e.extends_previous);
    assert_eq!(e.previous_last_breakpoint.unwrap().region, Region::Messages);
}

#[test]
fn explicit_markers_follow_tools_system_messages_order_and_absence_is_explicit() {
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
    ] {
        let mut body: Value = serde_json::from_slice(&render(id, &options())).unwrap();
        let compare = |body: &Value| {
            let bytes = serde_json::to_vec(body).unwrap();
            compare_rendered_requests(id, &bytes, &bytes).unwrap()
        };
        assert_eq!(compare(&body).ttl_kind, TtlKind::Explicit);
        assert!(compare(&body).previous_last_breakpoint.is_none());
        if id == WireFamilyId::AnthropicMessages {
            body["tools"] = json!([{"name":"read","cache_control":{"type":"ephemeral"}}]);
        } else {
            body["toolConfig"] = json!({"tools":[{"cachePoint":{"type":"default"}}]});
        }
        assert_eq!(
            compare(&body).previous_last_breakpoint.unwrap().region,
            Region::Tools
        );
        if id == WireFamilyId::AnthropicMessages {
            body["system"][0]["cache_control"] = json!({"type":"ephemeral"});
        } else {
            body["system"]
                .as_array_mut()
                .unwrap()
                .push(json!({"cachePoint":{"type":"default"}}));
        }
        assert_eq!(
            compare(&body).previous_last_breakpoint.unwrap().region,
            Region::System
        );
    }
    for id in [
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::Gemini,
    ] {
        let body = render(id, &options());
        let evidence = compare_rendered_requests(id, &body, &body).unwrap();
        assert_eq!(evidence.ttl_kind, TtlKind::Implicit);
        assert!(evidence.previous_last_breakpoint.is_none());
    }
}

#[test]
fn output_caps_and_billing_correlation_rotate_but_reasoning_settings_do_not() {
    let id = WireFamilyId::AnthropicMessages;
    let mut old: Value = serde_json::from_slice(&render(id, &options())).unwrap();
    old["system"][0]["text"] =
        json!("x-anthropic-billing-header: cch=abcde; cc_prompt_id=one;\ninstructions");
    old["metadata"] =
        json!({"user_id":r#"{"device_id":"one","session_id":"one","account_uuid":"account"}"#});
    let mut new = old.clone();
    new["system"][0]["text"] =
        json!("x-anthropic-billing-header: cch=12345; cc_prompt_id=two;\ninstructions");
    new["max_tokens"] = json!(1);
    new["metadata"]["user_id"] =
        json!(r#"{"device_id":"two","session_id":"two","account_uuid":"account"}"#);
    let compare = |new: &Value| {
        compare_rendered_requests(
            id,
            &serde_json::to_vec(&old).unwrap(),
            &serde_json::to_vec(new).unwrap(),
        )
        .unwrap()
    };
    assert!(compare(&new).extends_previous);
    new["thinking"] = json!({"type":"enabled","budget_tokens":1000});
    assert_eq!(
        compare(&new).first_divergence.unwrap().region,
        Region::ProviderState
    );
}

#[test]
fn chatgpt_continuation_compares_canonical_full_input_not_the_socket_suffix() {
    let id = WireFamilyId::OpenAiResponsesChatgpt;
    let old: Value = serde_json::from_slice(&render(id, &options())).unwrap();
    let mut new = old.clone();
    let output = json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"reply"}]});
    new["input"].as_array_mut().unwrap().push(output.clone());
    new["input"].as_array_mut().unwrap().push(
        json!({"type":"message","role":"user","content":[{"type":"input_text","text":"next"}]}),
    );
    let receipt = crate::chatgpt_continuation::CompletedResponse {
        request: old.clone(),
        output: vec![output],
        response_id: "resp_one".into(),
    };
    assert!(receipt.suffix_start(&new).is_ok());
    assert!(
        compare_rendered_requests(
            id,
            &serde_json::to_vec(&old).unwrap(),
            &serde_json::to_vec(&new).unwrap()
        )
        .unwrap()
        .extends_previous
    );
}

#[test]
fn moved_bedrock_marker_and_edit_resolve_the_same_segment_by_ordinal() {
    let old = json!({"messages":[{"role":"user","content":[{"text":"A"},{"cachePoint":{"type":"default"}},{"text":"B"}]}]});
    let new = json!({"messages":[{"role":"user","content":[{"cachePoint":{"type":"default"}},{"text":"changed A"},{"text":"B"}]}]});
    let evidence = compare_rendered_requests(
        WireFamilyId::BedrockConverse,
        &serde_json::to_vec(&old).unwrap(),
        &serde_json::to_vec(&new).unwrap(),
    )
    .unwrap();
    let divergence = evidence.first_divergence.unwrap();
    assert_eq!(divergence.message_index, Some(0));
    assert_eq!(divergence.part_index, Some(1));
    assert_eq!(divergence.field.as_deref(), Some("text"));
}

#[test]
fn message_ordinal_is_optional_and_the_old_evidence_decoder_ignores_it() {
    let mut value =
        json!({"region":"messages","message_index":2,"part_index":0,"message_ordinal":41});
    let current: Divergence = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(current.message_ordinal, Some(41));
    let old: reference::Divergence = serde_json::from_value(value.clone()).unwrap();
    value.as_object_mut().unwrap().remove("message_ordinal");
    assert_eq!(serde_json::to_value(old).unwrap(), value);
    let absent: Divergence = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(absent.message_ordinal, None);
    assert_eq!(serde_json::to_value(absent).unwrap(), value);
}

fn render_with_provenance(
    id: WireFamilyId,
    opts: &CallOptions,
) -> (Vec<u8>, broca_protocol::render_provenance::RenderProvenance) {
    let mut frozen = crate::families::frozen_config_for_family(id, "model");
    frozen.project_id = Some("project".into());
    if id == WireFamilyId::OpenAiResponsesChatgpt {
        frozen.chatgpt_wire = Some(
            serde_json::from_str(include_str!(
                "../tests/fixtures/chatgpt_codex_0_163_identity.json"
            ))
            .unwrap(),
        );
    }
    let family = crate::registry::wire_family(id).unwrap();
    let plain = family.render(&frozen, opts);
    let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
    let traced = family.render_with_provenance(&frozen, opts, Some(&mut provenance));
    assert_eq!(plain, traced, "provenance must not change {id:?} bytes");
    (traced, provenance)
}

fn assert_durable_ordinal(id: WireFamilyId) {
    let mut old = options();
    old.prompt[0].ordinal = Some(7);
    old.prompt[1].ordinal = Some(42);
    old.prompt[2].ordinal = Some(90);
    let mut filtered = Message::new(
        Role::Assistant,
        vec![ContentBlock::Reasoning {
            text: "unsupported reasoning".into(),
            signature: None,
        }],
    );
    filtered.ordinal = Some(11);
    old.prompt.insert(1, filtered);
    let mut new = old.clone();
    new.prompt[2].content = vec![ContentBlock::Text {
        text: "edited A".into(),
    }];
    let (before, before_map) = render_with_provenance(id, &old);
    let (after, after_map) = render_with_provenance(id, &new);
    let evidence = compare_rendered_requests_with_provenance(
        id,
        &before,
        &after,
        Some(&before_map),
        Some(&after_map),
    )
    .unwrap();
    let divergence = evidence.first_divergence.unwrap();
    assert_eq!(divergence.region, Region::Messages);
    assert_eq!(divergence.message_ordinal, Some(42), "{id:?}");
    old.prompt[2].ordinal = None;
    new.prompt[2].ordinal = None;
    let (before, before_map) = render_with_provenance(id, &old);
    let (after, after_map) = render_with_provenance(id, &new);
    assert_eq!(
        compare_rendered_requests_with_provenance(
            id,
            &before,
            &after,
            Some(&before_map),
            Some(&after_map)
        )
        .unwrap()
        .first_divergence
        .unwrap()
        .message_ordinal,
        None
    );
}

macro_rules! ordinal_case {
    ($name:ident, $family:ident) => {
        #[test]
        fn $name() {
            assert_durable_ordinal(WireFamilyId::$family);
        }
    };
}
ordinal_case!(durable_ordinal_anthropic, AnthropicMessages);
ordinal_case!(durable_ordinal_bedrock, BedrockConverse);
ordinal_case!(durable_ordinal_chat, OpenAiChat);
ordinal_case!(durable_ordinal_responses, OpenAiResponses);
ordinal_case!(durable_ordinal_chatgpt, OpenAiResponsesChatgpt);
ordinal_case!(durable_ordinal_gemini, Gemini);
ordinal_case!(durable_ordinal_code_assist, GoogleCodeAssist);

#[test]
fn tool_result_splitting_and_grouping_preserve_the_actual_source_ordinal() {
    for id in families() {
        let mut opts = options();
        let call = |call_id: &str| {
            serde_json::from_value(
                json!({"type":"tool_call","tool_call_id":call_id,"tool_name":"lookup","input":{}}),
            )
            .unwrap()
        };
        let result = |call_id: &str, text: &str| {
            serde_json::from_value(json!({"type":"tool_result","tool_call_id":call_id,"tool_name":"lookup","output":{"kind":"text","text":text},"is_error":false})).unwrap()
        };
        let mut assistant = Message::new(Role::Assistant, vec![call("call-one"), call("call-two")]);
        assistant.ordinal = Some(100);
        opts.prompt.push(assistant);
        let mut first = Message::new(Role::Tool, vec![result("call-one", "one")]);
        first.ordinal = Some(101);
        opts.prompt.push(first);
        let mut second = Message::new(Role::Tool, vec![result("call-two", "two")]);
        second.ordinal = Some(102);
        opts.prompt.push(second);
        let mut changed = opts.clone();
        changed.prompt.last_mut().unwrap().content = vec![result("call-two", "edited two")];
        let (before, old_map) = render_with_provenance(id, &opts);
        let (after, new_map) = render_with_provenance(id, &changed);
        let evidence = compare_rendered_requests_with_provenance(
            id,
            &before,
            &after,
            Some(&old_map),
            Some(&new_map),
        )
        .unwrap();
        assert_eq!(
            evidence.first_divergence.unwrap().message_ordinal,
            Some(102),
            "{id:?}"
        );
    }
}

#[test]
fn system_and_tools_never_claim_a_transcript_message_ordinal() {
    for id in families() {
        let mut old = options();
        for (index, message) in old.prompt.iter_mut().enumerate() {
            message.ordinal = Some(index as u64);
        }
        let (before, old_map) = render_with_provenance(id, &old);
        let mut changed = old.clone();
        changed.prompt[0].content = vec![ContentBlock::Text {
            text: "changed system".into(),
        }];
        let (after, new_map) = render_with_provenance(id, &changed);
        let d = compare_rendered_requests_with_provenance(
            id,
            &before,
            &after,
            Some(&old_map),
            Some(&new_map),
        )
        .unwrap()
        .first_divergence
        .unwrap();
        assert_eq!(d.region, Region::System);
        assert_eq!(d.message_ordinal, None);
        changed = old.clone();
        changed.tools = std::sync::Arc::new(vec![serde_json::from_value(json!({"name":"lookup","description":"new tool","input_schema":{"type":"object","properties":{}}})).unwrap()]);
        let (after, new_map) = render_with_provenance(id, &changed);
        let d = compare_rendered_requests_with_provenance(
            id,
            &before,
            &after,
            Some(&old_map),
            Some(&new_map),
        )
        .unwrap()
        .first_divergence
        .unwrap();
        assert_eq!(d.region, Region::Tools);
        assert_eq!(d.message_ordinal, None);
    }
}
