//! A Responses function call keeps the provider's own fields of the call, and a later request
//! sends them back. OpenAI's Responses API checks every item before a replayed reasoning item
//! against its own copy of the earlier output, and drops the reasoning when a function call
//! there lacks its `status` or carries an `arguments` string other than the one the model wrote.
//! Any request that resends the history in full (a restart, a retry on a new socket, a
//! compaction view) replays the calls from storage, so without these fields every reasoning
//! item after the first call was dropped.

use broca_protocol::{
    content::{
        AssistantMessage, ContentBlock, Message, Role, TextProviderFormat, ToolCall, ToolOutput,
        ToolResult,
    },
    provider::{NormalizedEvent, ProviderControl},
    CallOptions, ModelHandle, StepId,
};
use broca_provider::{
    families::{
        openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, OpenAiResponses,
        OpenAiResponsesChatgpt,
    },
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

/// One step: a reasoning item, then two calls. The first call's arguments are spelled the way
/// a model writes them (a space after each colon and comma), which is not the canonical text;
/// the second's are canonical already.
fn stream() -> String {
    let events = [
        json!({"type":"response.created","response":{"id":"resp_1","output":[]}}),
        json!({"type":"response.output_item.done","output_index":0,
               "item":{"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"gAAAAB1"}}),
        json!({"type":"response.output_item.added","output_index":1,
               "item":{"id":"fc_1","type":"function_call","status":"in_progress","arguments":"",
                       "call_id":"call_1","name":"bash"}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,
               "delta":"{\"command\": \"ls\", \"description\": \"list\"}"}),
        json!({"type":"response.output_item.done","output_index":1,
               "item":{"id":"fc_1","type":"function_call","status":"completed",
                       "arguments":"{\"command\": \"ls\", \"description\": \"list\"}",
                       "call_id":"call_1","name":"bash"}}),
        json!({"type":"response.output_item.done","output_index":2,
               "item":{"id":"fc_2","type":"function_call","status":"completed",
                       "arguments":"{\"command\":\"pwd\"}","call_id":"call_2","name":"bash"}}),
        json!({"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[],
               "usage":{"input_tokens":10,"output_tokens":5}}}),
    ];
    events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect()
}

fn decoded(family: &dyn WireFamily, frozen: &FrozenRenderConfig) -> AssistantMessage {
    let mut normalizer = family.normalizer(frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in stream().as_bytes().chunks(7) {
        events.extend(normalizer.push(chunk).unwrap());
    }
    events.extend(normalizer.finish().unwrap());
    events
        .into_iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        })
        .expect("the stream produces a complete assistant message")
}

fn calls(message: &AssistantMessage) -> Vec<&ToolCall> {
    message.tool_calls().collect()
}

fn model() -> ModelHandle {
    ModelHandle::new("fixture-provider", "model")
}

/// The next request after `message`: its calls answered, then a new user turn.
fn next_request(message: &AssistantMessage) -> CallOptions {
    let mut assistant = Message::new(Role::Assistant, message.content.clone());
    assistant.origin = Some(model());
    let results = message
        .tool_calls()
        .map(|call| {
            ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text { text: "ok".into() },
                is_error: false,
            })
        })
        .collect();
    let mut opts = CallOptions::new(vec![
        Message::new(Role::User, vec![ContentBlock::Text { text: "go".into() }]),
        assistant,
        Message::new(Role::Tool, results),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "next".into(),
            }],
        ),
    ]);
    opts.target_model = Some(model());
    opts
}

/// The request's function call items, by call id.
fn rendered_calls(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
) -> Vec<Value> {
    let body: Value = serde_json::from_slice(&family.render(frozen, opts)).unwrap();
    body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["type"] == "function_call")
        .cloned()
        .collect()
}

fn keeps_and_replays(
    family: &dyn WireFamily,
    frozen: FrozenRenderConfig,
    format: TextProviderFormat,
) {
    let message = decoded(family, &frozen);
    let [spaced, canonical] = calls(&message)[..] else {
        panic!("two calls")
    };
    assert_eq!(
        spaced.input.text(),
        r#"{"command":"ls","description":"list"}"#
    );
    let state = spaced
        .provider_state
        .as_ref()
        .expect("the call keeps its fields");
    assert_eq!(state.format, format);
    assert_eq!(
        serde_json::from_str::<Value>(&state.data).unwrap(),
        json!({"status":"completed","arguments":"{\"command\": \"ls\", \"description\": \"list\"}"}),
        "the model's own spelling is kept because it differs from the canonical text"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&canonical.provider_state.as_ref().unwrap().data).unwrap(),
        json!({"status":"completed"}),
        "canonical arguments are not stored twice"
    );

    let opts = next_request(&message);
    assert_eq!(
        rendered_calls(family, &frozen, &opts),
        [
            json!({"type":"function_call","call_id":"call_1","name":"bash","status":"completed",
                   "arguments":"{\"command\": \"ls\", \"description\": \"list\"}"}),
            json!({"type":"function_call","call_id":"call_2","name":"bash","status":"completed",
                   "arguments":"{\"command\":\"pwd\"}"}),
        ],
        "each call goes back as the provider sent it, without its item id"
    );

    // State from another model, an unknown origin, or another endpoint's format is never sent:
    // those calls go back in the canonical form, as before the state existed.
    let canonical_form = [
        json!({"type":"function_call","call_id":"call_1","name":"bash",
               "arguments":"{\"command\":\"ls\",\"description\":\"list\"}"}),
        json!({"type":"function_call","call_id":"call_2","name":"bash",
               "arguments":"{\"command\":\"pwd\"}"}),
    ];
    for gate in [
        "foreign-model",
        "unknown-origin",
        "foreign-format",
        "no-state",
    ] {
        let mut opts = next_request(&message);
        match gate {
            "foreign-model" => {
                opts.target_model = Some(ModelHandle::new("fixture-provider", "another-model"))
            }
            "unknown-origin" => opts.prompt[1].origin = None,
            _ => {
                for block in &mut opts.prompt[1].content {
                    if let ContentBlock::ToolCall(call) = block {
                        if gate == "no-state" {
                            call.provider_state = None;
                        } else if let Some(state) = call.provider_state.as_mut() {
                            state.format = match state.format {
                                TextProviderFormat::OpenAiResponses => {
                                    TextProviderFormat::OpenAiResponsesChatgpt
                                }
                                _ => TextProviderFormat::OpenAiResponses,
                            };
                        }
                    }
                }
            }
        }
        assert_eq!(
            rendered_calls(family, &frozen, &opts),
            canonical_form,
            "{gate}"
        );
    }

    // A compaction view that rewrote a call's input sends the new input, never the model's
    // old text; the rest of the call's fields still go back.
    let mut opts = next_request(&message);
    if let ContentBlock::ToolCall(call) = opts.prompt[1]
        .content
        .iter_mut()
        .find(|block| matches!(block, ContentBlock::ToolCall(_)))
        .unwrap()
    {
        call.input = json!({"command":"ls"}).into();
    }
    assert_eq!(
        rendered_calls(family, &frozen, &opts)[0],
        json!({"type":"function_call","call_id":"call_1","name":"bash","status":"completed",
               "arguments":"{\"command\":\"ls\"}"})
    );
}

#[test]
fn a_responses_call_keeps_and_replays_its_provider_fields() {
    keeps_and_replays(
        &OpenAiResponses,
        openai_responses_frozen_config("model"),
        TextProviderFormat::OpenAiResponses,
    );
}

#[test]
fn a_chatgpt_call_keeps_and_replays_its_provider_fields() {
    keeps_and_replays(
        &OpenAiResponsesChatgpt,
        openai_responses_chatgpt_frozen_config("model"),
        TextProviderFormat::OpenAiResponsesChatgpt,
    );
}
