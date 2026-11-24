//! Gemini refuses a request whose current exchange has a step with tool calls but no thought
//! signature ("Function call is missing a thought_signature"). A compaction view that drops a
//! parallel step's first call, the only one Gemini signs, leaves such a step behind. These tests
//! pin that the renderer marks those steps, and only those, with Google's documented placeholder.

use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    request::{CallOptions, ModelHandle},
    StepContext, ToolCallId,
};
use broca_provider::families::{
    gemini_frozen_config, google_codeassist_frozen_config, Gemini, GoogleCodeAssist,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use serde_json::{json, Value};

const MODEL: &str = "gemini-3.5-flash-low";
const PLACEHOLDER: &str = "skip_thought_signature_validator";

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn step(calls: &[(&str, Option<&str>)]) -> Message {
    Message::assistant_from(
        calls
            .iter()
            .map(|(id, signature)| {
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId((*id).into()),
                    tool_name: "read".into(),
                    input: json!({"path": id}).into(),
                    thought_signature: signature.map(Into::into),
                    provider_state: None,
                })
            })
            .collect(),
        ModelHandle::new("google", MODEL),
    )
}

fn results(ids: &[&str]) -> Message {
    Message::new(
        Role::Tool,
        ids.iter()
            .map(|id| {
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId((*id).into()),
                    output: ToolOutput::Text { text: "ok".into() },
                    is_error: false,
                })
            })
            .collect(),
    )
}

/// A Code Assist config for `model`, with the project every Code Assist request names.
fn codeassist(model: &str) -> FrozenRenderConfig {
    let mut frozen = google_codeassist_frozen_config(model);
    frozen.project_id = Some("test-project".into());
    frozen
}

fn render(family: &dyn WireFamily, frozen: &FrozenRenderConfig, prompt: Vec<Message>) -> Value {
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(ModelHandle::new("google", MODEL));
    // Code Assist derives its request id from the run and step, so it needs both.
    opts.step_context = Some(StepContext {
        session_address: None,
        credential_id: None,
        account_ref: None,
        run_id: "run-unsigned".into(),
        step_index: 1,
        max_output_tokens: None,
    });
    let body: Value = serde_json::from_slice(&family.render(frozen, &opts)).unwrap();
    // Code Assist wraps the Gemini body in `request`.
    body.get("request").cloned().unwrap_or(body)
}

/// Each model step's thought signatures, in call order.
fn signatures(body: &Value) -> Vec<Vec<Option<String>>> {
    body["contents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|content| content["role"] == "model")
        .map(|content| {
            content["parts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|part| part.get("functionCall").is_some())
                .map(|part| part["thoughtSignature"].as_str().map(str::to_owned))
                .collect()
        })
        .collect()
}

/// An earlier exchange with an unsigned step, then the current one: a signed step, a step whose
/// signed first call a view dropped (two unsigned calls left), and a signed parallel step.
fn prompt() -> Vec<Message> {
    vec![
        user("first"),
        step(&[("a", None)]),
        results(&["a"]),
        user("second"),
        step(&[("b", Some("sig-b"))]),
        results(&["b"]),
        step(&[("c", None), ("d", None)]),
        results(&["c", "d"]),
        step(&[("e", Some("sig-e")), ("f", None)]),
        results(&["e", "f"]),
    ]
}

fn marked() -> Vec<Vec<Option<String>>> {
    let s = |v: &str| Some(v.to_owned());
    vec![
        vec![None],
        vec![s("sig-b")],
        vec![s(PLACEHOLDER), None],
        vec![s("sig-e"), None],
    ]
}

fn unmarked() -> Vec<Vec<Option<String>>> {
    let s = |v: &str| Some(v.to_owned());
    vec![
        vec![None],
        vec![s("sig-b")],
        vec![None, None],
        vec![s("sig-e"), None],
    ]
}

#[test]
fn gemini_marks_only_an_unsigned_step_of_the_current_exchange() {
    let body = render(&Gemini, &gemini_frozen_config(MODEL), prompt());
    assert_eq!(signatures(&body), marked());
}

#[test]
fn codeassist_marks_a_gemini_models_unsigned_step() {
    let body = render(&GoogleCodeAssist, &codeassist(MODEL), prompt());
    assert_eq!(signatures(&body), marked());
}

/// A run frozen before the marking renders its steps as it always did, so it resumes byte for
/// byte.
#[test]
fn configs_frozen_before_the_marking_keep_their_unsigned_steps() {
    let mut gemini = gemini_frozen_config(MODEL);
    gemini.renderer_version = 4;
    assert_eq!(signatures(&render(&Gemini, &gemini, prompt())), unmarked());
    let mut codeassist = codeassist(MODEL);
    codeassist.renderer_version = 9;
    assert_eq!(
        signatures(&render(&GoogleCodeAssist, &codeassist, prompt())),
        unmarked()
    );
}

/// Claude through Antigravity takes no thought signatures at all, so nothing is marked.
#[test]
fn a_claude_model_through_codeassist_gets_no_placeholder() {
    let body = render(
        &GoogleCodeAssist,
        &codeassist("claude-sonnet-5-5-high"),
        prompt(),
    );
    assert!(!body.to_string().contains(PLACEHOLDER));
}
