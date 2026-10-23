//! Live check: does the ChatGPT backend accept assistant text that carries `phase` after the
//! runner has changed it, and does the prompt cache still hold?
//!
//! Run `cargo run --locked -p broca-module-serve --example chatgpt-phase-tag-probe --
//! [--dry-run|--live] [--model openai/MODEL] [--variant NAME|--no-variant] [--gap-secs N]`.
//!
//! The default is a dry run: nothing is sent and no credential is read. The five planned
//! requests are built from a synthetic seed reply, rendered through broca's ChatGPT renderer
//! (the family-default frozen config plus the recorded Codex wire identity fixture), and printed
//! redacted, followed by self-checks of the tagging and cleaning.
//!
//! `--live` reads the standing `chatgpt:openai` dev-tools handle through the vault (the handle,
//! token and account id are never printed), resolves the model through the last-known-good
//! catalog with the ChatGPT auth method, and sends at most five requests through broca's
//! production ChatGPT renderer, parser and transport:
//!
//! 1. `seed`: a question with one `read_file` tool, asking for one short sentence before the
//!    call. The tool is not forced, because a forced call leaves no assistant text. If the seed
//!    is not accepted, or its parsed reply has no phase-bearing text part (`TextWithState` with
//!    a `phase`) or no tool call, the probe stops with INCONCLUSIVE and sends nothing else.
//! 2. `control_tool_loop` and 3. `control_next_turn`: the tool result, then a new user turn, with
//!    the history exactly as broca renders it today: text untagged, `phase` kept, encrypted
//!    reasoning echoed.
//! 4. `tagged_tool_loop` and 5. `tagged_next_turn`: the same two requests, with every assistant
//!    text part prefixed `§N§ `. N is the part's position counted over every part of every
//!    non-system message, so a part keeps its number in both requests. The prefix is applied to
//!    a copy at each request; the stored reply stays as the parser produced it. In the seed's
//!    first text part a `§7§` is injected mid-text (as if the model had written it) and then
//!    cleaned out. Cleaning removes only the mark, so the space injected after it stays and the
//!    bytes sent differ from what the model produced by one byte. A later part's own prefix may
//!    also read `§7§`; the cleaning check looks only at the injected mid-text mark.
//!
//! The probe tags phase text itself: broca's `render_tags_into` deliberately leaves text with
//! provider state untagged, which is the rule this probe tests. The arms differ only in
//! assistant text: the tagged arm's prefixes and the one byte cleaning leaves behind.
//!
//! Every request starts on a fresh transport, so each sends its full input and none continues
//! an earlier response on the same socket. Tagged history never equals the backend's own output,
//! so a tagged request in production would send full input too; forcing it on the control arm
//! keeps the two arms comparable. Each request prints its HTTP status, any error, broca's fresh
//! `input_tokens` and `cached_input_tokens`, the phases decoded from the reply, and the input
//! items as sent, including how many encrypted reasoning items were echoed.
//!
//! Live exit codes: 0 accepted with comparable cache reads, 3 tagged requests rejected while
//! the control was accepted, 4 accepted but the tagged arm's later cache read is below 80% of
//! the control's, 2 inconclusive, 1 probe failure. A dry run exits 0 when its self-checks pass
//! and 1 when they fail.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use broca_core::Provider;
use broca_protocol::{
    request::{CallOptions, ModelHandle, Tool, ToolChoice},
    tags::tag_mark,
    ContentBlock, EncryptedReasoningFormat, Message, ModelAttempt, NormalizedEvent,
    ProviderControl, Role, StepId, TextProviderFormat, TextProviderState, ToolCall, ToolCallId,
    ToolOutput, ToolResult,
};
use broca_provider::{
    families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt},
    wire::WireFamily,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const CREDENTIAL_ID: &str = "chatgpt:openai";
const DEFAULT_MODEL: &str = "openai/gpt-5.3-codex";
// A reasoning variant asks for reasoning; any encrypted reasoning returned is echoed in history.
const DEFAULT_VARIANT: &str = "low";
const TOOL: &str = "read_file";
const TOOL_OUTPUT: &str = "The sample file contains the number 42.";
// The mark injected mid-text in the seed's first text part, then cleaned out.
const INJECTED_TAG: u64 = 7;
const STEP_DEADLINE_SECS: u64 = 180;
// The tagged arm's later cache read must reach this share of the control's to count as comparable.
const COMPARABLE_CACHE_SHARE: f64 = 0.8;
const NEXT_TURN_QUESTION: &str =
    "New question: in one short sentence, what number did the sample file contain?";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn sha12(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A large, identical prefix in every request, so cache reads measure the same material.
fn instructions() -> String {
    let mut text = "You are a careful assistant. The CACHE_PREFIX lines below are inert cache probe material; never analyze or repeat them.\nCACHE_PREFIX\n".to_string();
    for index in 0..512 {
        text.push_str(&format!(
            "cache-prefix-{index:04}: deterministic neutral material retained across every request.\n"
        ));
    }
    text
}

fn file_tool() -> Tool {
    Tool {
        name: TOOL.into(),
        description: "Read the fixed sample file at sample.txt.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": false
        }),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn base_prompt() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: instructions(),
            }],
        ),
        user(&format!(
            "Before you call any tool, first write one short sentence telling me what you are about to do. Then call {TOOL} exactly once with path sample.txt. After the tool result arrives, answer in one short sentence and do not call any tool again."
        )),
    ]
}

fn request(prompt: Vec<Message>, target: &ModelHandle) -> CallOptions {
    let mut opts = CallOptions::new(prompt);
    opts.tools = vec![file_tool()].into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts.target_model = Some(target.clone());
    opts
}

/// One tool result per tool call in `content`, or `None` when it made no call.
fn tool_results(content: &[ContentBlock]) -> Option<Message> {
    let results: Vec<ContentBlock> = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: TOOL_OUTPUT.into(),
                },
                is_error: false,
            })),
            _ => None,
        })
        .collect();
    (!results.is_empty()).then(|| Message::new(Role::Tool, results))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Control,
    Tagged,
}

fn first_text(content: &[ContentBlock]) -> Option<&str> {
    content.iter().find_map(|block| match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
            if !text.is_empty() =>
        {
            Some(text.as_str())
        }
        _ => None,
    })
}

/// `text` with `§7§ ` inserted after the first space at or past its middle (or at the middle
/// when no such space exists), as if the model had written the mark itself.
fn inject_mark(text: &str) -> String {
    let middle = text.len() / 2;
    let at = text
        .char_indices()
        .find(|&(index, ch)| index >= middle && ch == ' ')
        .map(|(index, _)| index + 1)
        .or_else(|| {
            text.char_indices()
                .map(|(index, _)| index)
                .find(|&index| index >= middle)
        })
        .unwrap_or(text.len());
    let mut stored = text.to_owned();
    stored.insert_str(at, &format!("{} ", tag_mark(INJECTED_TAG)));
    stored
}

/// `text` with every well-formed `§N§` removed, and nothing else: surrounding spaces stay.
fn clean_marks(text: &str) -> String {
    const SIGIL: char = '§';
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find(SIGIL) {
        out.push_str(&rest[..start]);
        let after = &rest[start + SIGIL.len_utf8()..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        let mark_len = 2 * SIGIL.len_utf8() + digits;
        if after[digits..].starts_with(SIGIL)
            && cortexkit_role_llm_runner::tags::parse_tag(&rest[start..start + mark_len]).is_some()
        {
            rest = &rest[start + mark_len..];
        } else {
            out.push(SIGIL);
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// The seed's first text part as the tagged arm stores it: with the injected mark.
fn seed_message(seed: &[ContentBlock], arm: Arm, target: &ModelHandle) -> Message {
    let mut content = seed.to_vec();
    if arm == Arm::Tagged {
        if let Some(text) = content.iter_mut().find_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                if !text.is_empty() =>
            {
                Some(text)
            }
            _ => None,
        }) {
            *text = inject_mark(text);
        }
    }
    Message::assistant_from(content, target.clone())
}

/// Prefix every non-empty assistant text part with its tag, after cleaning any mark out of it.
/// Every part of every non-system message takes the next number, so a prompt that extends an
/// earlier one gives each shared part the same number.
fn render_assistant_tags(prompt: &mut [Message]) {
    let mut number = 0u64;
    for message in prompt.iter_mut().filter(|m| m.role != Role::System) {
        let role = message.role;
        for block in &mut message.content {
            number += 1;
            if role != Role::Assistant {
                continue;
            }
            if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = block {
                let cleaned = clean_marks(text);
                *text = if cleaned.is_empty() {
                    cleaned
                } else {
                    format!("{} {cleaned}", tag_mark(number))
                };
            }
        }
    }
}

fn tool_loop_prompt(seed: &[ContentBlock], arm: Arm, target: &ModelHandle) -> Vec<Message> {
    let mut prompt = base_prompt();
    prompt.push(seed_message(seed, arm, target));
    prompt.extend(tool_results(seed));
    prompt
}

fn next_turn_prompt(
    seed: &[ContentBlock],
    reply: &[ContentBlock],
    arm: Arm,
    target: &ModelHandle,
) -> Vec<Message> {
    let mut prompt = tool_loop_prompt(seed, arm, target);
    prompt.push(Message::assistant_from(reply.to_vec(), target.clone()));
    // A reply that called the tool again needs its results before the new user turn.
    prompt.extend(tool_results(reply));
    prompt.push(user(NEXT_TURN_QUESTION));
    prompt
}

/// The request for `prompt`; the tagged arm's assistant text is tagged on this copy only.
fn finish(mut prompt: Vec<Message>, arm: Arm, target: &ModelHandle) -> CallOptions {
    if arm == Arm::Tagged {
        render_assistant_tags(&mut prompt);
    }
    request(prompt, target)
}

fn tool_loop(seed: &[ContentBlock], arm: Arm, target: &ModelHandle) -> CallOptions {
    finish(tool_loop_prompt(seed, arm, target), arm, target)
}

fn next_turn(
    seed: &[ContentBlock],
    reply: &[ContentBlock],
    arm: Arm,
    target: &ModelHandle,
) -> CallOptions {
    finish(next_turn_prompt(seed, reply, arm, target), arm, target)
}

/// What cleaning did to the seed's first text part: the model's bytes against the bytes sent
/// after the tag prefix. Never prints the text itself.
fn cleaning_evidence(seed: &[ContentBlock]) -> Value {
    let Some(produced) = first_text(seed) else {
        return json!({"applied": false});
    };
    let stored = inject_mark(produced);
    let sent = clean_marks(&stored);
    json!({
        "applied": true,
        "injected_mark": tag_mark(INJECTED_TAG),
        "model_bytes": produced.len(),
        "stored_bytes": stored.len(),
        "sent_bytes_without_prefix": sent.len(),
        "model_sha12": sha12(produced.as_bytes()),
        "sent_sha12": sha12(sent.as_bytes()),
        "mark_removed": !sent.contains(&tag_mark(INJECTED_TAG)),
        "sent_differs_from_model": sent != produced,
    })
}

fn phase_of(state: &TextProviderState) -> Option<String> {
    serde_json::from_str::<Value>(&state.data)
        .ok()?
        .get("phase")?
        .as_str()
        .map(str::to_owned)
}

/// Phases of the ChatGPT text parts the parser decoded, in order.
fn phases(content: &[ContentBlock]) -> Vec<String> {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::TextWithState { provider_state, .. }
                if provider_state.format == TextProviderFormat::OpenAiResponsesChatgpt =>
            {
                phase_of(provider_state)
            }
            _ => None,
        })
        .collect()
}

fn reply_shape(content: &[ContentBlock]) -> Value {
    let mut reasoning = 0;
    let mut encrypted = 0;
    let mut text = 0;
    let mut text_with_state = 0;
    let mut tool_calls = 0;
    for block in content {
        match block {
            ContentBlock::Media(_) => {}
            ContentBlock::Reasoning { .. } => reasoning += 1,
            ContentBlock::RedactedReasoning {
                provider_format, ..
            } => {
                if *provider_format == Some(EncryptedReasoningFormat::OpenAiResponsesChatgpt) {
                    encrypted += 1;
                }
            }
            ContentBlock::Text { .. } => text += 1,
            ContentBlock::TextWithState { .. } => text_with_state += 1,
            ContentBlock::ToolCall(_) => tool_calls += 1,
            ContentBlock::ToolResult(_) => {}
        }
    }
    json!({
        "reasoning_summaries": reasoning, "encrypted_reasoning": encrypted, "plain_text": text,
        "text_with_state": text_with_state, "tool_calls": tool_calls,
    })
}

fn has_encrypted_reasoning(content: &[ContentBlock]) -> bool {
    content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::RedactedReasoning {
                provider_format: Some(EncryptedReasoningFormat::OpenAiResponsesChatgpt),
                ..
            }
        )
    })
}

fn message_text(item: &Value) -> String {
    match &item["content"] {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect(),
        _ => String::new(),
    }
}

/// The request's input items as sent, redacted: sizes, roles, phases, and the first 16
/// characters of assistant text (enough to show a tag prefix). Ciphertext is only measured.
fn describe_body(body: &Value) -> Value {
    let mut items = Vec::new();
    let mut echoed_reasoning = 0;
    let mut assistant_phases = Vec::new();
    let mut assistant_heads = Vec::new();
    for item in body["input"].as_array().into_iter().flatten() {
        let kind = item["type"].as_str().unwrap_or("unknown");
        items.push(match kind {
            "message" => {
                let role = item["role"].as_str().unwrap_or("unknown");
                let text = message_text(item);
                if role == "assistant" {
                    let head: String = text.chars().take(16).collect();
                    assistant_phases.push(item.get("phase").cloned().unwrap_or(Value::Null));
                    assistant_heads.push(head.clone());
                    json!({"kind":"message","role":role,"phase":item.get("phase"),"text_bytes":text.len(),"text_head":head})
                } else {
                    json!({"kind":"message","role":role,"text_bytes":text.len()})
                }
            }
            "reasoning" => {
                let cipher = item["encrypted_content"].as_str().unwrap_or_default().len();
                if cipher > 0 {
                    echoed_reasoning += 1;
                }
                json!({"kind":"reasoning","encrypted_content_bytes":cipher})
            }
            "function_call" => json!({"kind":"function_call","name":item["name"]}),
            "function_call_output" => json!({
                "kind":"function_call_output",
                "output_bytes": item["output"].as_str().map_or(0, str::len)
            }),
            other => json!({"kind": other}),
        });
    }
    json!({
        "model": body["model"], "reasoning": body["reasoning"], "include": body["include"],
        "store": body["store"], "tool_choice": body["tool_choice"],
        "prompt_cache_key_present": body.get("prompt_cache_key").is_some(),
        "encrypted_reasoning_echoed": echoed_reasoning,
        "assistant_phases_sent": assistant_phases,
        "assistant_text_heads": assistant_heads,
        "input_items": items,
    })
}

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    received_assistant_message: bool,
    finished: bool,
    status: Option<u16>,
    error: Option<Value>,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    attempts: Vec<ModelAttempt>,
}

impl Answer {
    fn accepted(&self) -> bool {
        // The ChatGPT backend serves Responses over a websocket, whose successful upgrade is
        // HTTP 101; the HTTPS fallback answers 200. Either is a success when the response
        // finished without an error.
        matches!(self.status, Some(200) | Some(101)) && self.error.is_none() && self.finished
    }

    fn cached(&self) -> u64 {
        self.cached_input_tokens.unwrap_or(0)
    }
}

/// `message` with every secret replaced and control characters flattened, at most 600 chars.
fn sanitize(message: &str, secrets: &[&str]) -> String {
    let mut clean = message.to_owned();
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        clean = clean.replace(secret, "[redacted]");
    }
    clean
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .take(600)
        .collect()
}

fn full_sha(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Send one request on a fresh transport and print what it sent and what came back.
async fn step(
    provider: &dyn Provider,
    number: u64,
    label: &str,
    opts: &CallOptions,
    secrets: &[&str],
) -> (Answer, Value) {
    let rendered = provider.render_request(opts);
    let rendered_sha = full_sha(&rendered);
    let described = serde_json::from_slice::<Value>(&rendered)
        .map(|body| describe_body(&body))
        .unwrap_or_else(|_| json!({"render":"not_json"}));
    // A fresh socket for every request: each sends its full input, none continues another.
    provider.close_run_transport();
    let cancel = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(STEP_DEADLINE_SECS);
    let mut rx = provider
        .step_with_cancel(StepId(number), opts, cancel.clone(), Some(deadline))
        .await;
    let mut answer = Answer::default();
    let observe = |answer: &mut Answer, event: NormalizedEvent| match event {
        NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
            answer.status = attempt.http_status.or(answer.status);
            answer.attempts.push(attempt);
        }
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
            answer.content = message.content;
            answer.received_assistant_message = true;
        }
        NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
            answer.input_tokens = usage.input_tokens;
            answer.cached_input_tokens = usage.cached_input_tokens;
            answer.finished = true;
        }
        NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
            answer.status = error.status.or(answer.status);
            answer.error = Some(json!({
                "class": error.class, "provider_code": error.provider_code,
                "status": error.status, "message": sanitize(&error.message, secrets),
            }));
        }
        _ => {}
    };
    let drained = tokio::time::timeout_at(deadline + Duration::from_secs(10), async {
        while let Some(event) = rx.recv().await {
            observe(&mut answer, event);
        }
    })
    .await
    .is_ok();
    if !drained {
        cancel.cancel();
        provider.close_run_transport();
        answer.error = Some(
            json!({"class":"probe_deadline","message":"no terminal event before the step deadline"}),
        );
    } else if !answer.finished && answer.error.is_none() {
        answer.error = Some(
            json!({"class":"incomplete_response","message":"stream ended without a terminal success"}),
        );
    }
    let last = answer.attempts.last();
    let codex = last
        .and_then(|attempt| attempt.websocket_request.as_deref())
        .and_then(|frame| frame.codex.as_deref());
    println!(
        "{}",
        json!({
            "event":"request", "label":label, "http_status":answer.status, "error":answer.error,
            "input_tokens":answer.input_tokens, "cached_input_tokens":answer.cached_input_tokens,
            "reply_phases":phases(&answer.content), "reply_shape":reply_shape(&answer.content),
            "physical_attempts":answer.attempts.len(),
            "transport":last.and_then(|attempt| attempt.transport.as_deref()),
            "continuation":codex.map(|codex| codex.continuation),
            "rendered_matches_sent":last.map(|attempt| attempt.request_body_sha256 == rendered_sha),
            "request":described,
        })
    );
    (answer, described)
}

fn print_not_run(label: &str, reason: &str) {
    println!(
        "{}",
        json!({"event":"request","label":label,"http_status":null,"error":{"class":"not_run","message":reason}})
    );
}

fn heads(described: &Value) -> Vec<String> {
    described["assistant_text_heads"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|head| head.as_str().map(str::to_owned))
        .collect()
}

/// Every non-empty assistant text in the request starts with a tag mark, and there is one.
fn all_tagged(described: &Value) -> bool {
    let heads = heads(described);
    heads.iter().any(|head| !head.is_empty())
        && heads
            .iter()
            .filter(|head| !head.is_empty())
            .all(|head| head.starts_with('§'))
}

fn none_tagged(described: &Value) -> bool {
    heads(described).iter().all(|head| !head.contains('§'))
}

fn phases_sent(described: &Value) -> usize {
    described["assistant_phases_sent"]
        .as_array()
        .map_or(0, |phases| {
            phases.iter().filter(|phase| phase.is_string()).count()
        })
}

fn model_slug(model: &str) -> Result<&str, String> {
    model
        .strip_prefix("openai/")
        .filter(|slug| !slug.is_empty())
        .ok_or_else(|| "the model must be an openai/ catalog id".into())
}

fn synthetic_reasoning(id: &str) -> ContentBlock {
    ContentBlock::RedactedReasoning {
        data: json!({"id":id,"type":"reasoning","summary":[],"encrypted_content":"dry-run-ciphertext"})
            .to_string(),
        provider_format: Some(EncryptedReasoningFormat::OpenAiResponsesChatgpt),
    }
}

fn synthetic_text(id: &str, phase: &str, text: &str) -> ContentBlock {
    ContentBlock::TextWithState {
        text: text.into(),
        provider_state: TextProviderState {
            format: TextProviderFormat::OpenAiResponsesChatgpt,
            data: json!({"id":id,"phase":phase}).to_string(),
        },
    }
}

fn dry_run(model: &str, variant: Option<&str>) -> Result<i32, String> {
    let slug = model_slug(model)?;
    let target = ModelHandle::new("openai", slug);
    let mut frozen = openai_responses_chatgpt_frozen_config(slug);
    frozen.chatgpt_wire = Some(
        serde_json::from_str(include_str!(
            "../../broca-provider/tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .map_err(|error| format!("invalid dry-run wire fixture: {error}"))?,
    );
    frozen.prompt_cache_key = Some("dry-run-cache-scope".into());
    frozen.reasoning_effort = variant.map(str::to_owned);
    frozen.responses_tools_non_strict = true;
    let seed = vec![
        synthetic_reasoning("rs_dry_seed"),
        synthetic_text(
            "msg_dry_seed",
            "commentary",
            "I will read the sample file now.",
        ),
        ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("call_dry_run".into()),
            tool_name: TOOL.into(),
            input: json!({"path": "sample.txt"}),
        }),
    ];
    let reply = vec![
        synthetic_reasoning("rs_dry_reply"),
        synthetic_text(
            "msg_dry_reply",
            "final_answer",
            "The sample file contains 42.",
        ),
    ];
    let render = |opts: &CallOptions| -> Result<Value, String> {
        serde_json::from_slice(&OpenAiResponsesChatgpt.render(&frozen, opts))
            .map(|body| describe_body(&body))
            .map_err(|error| format!("rendered body is not JSON: {error}"))
    };
    let planned = [
        ("seed", request(base_prompt(), &target)),
        ("control_tool_loop", tool_loop(&seed, Arm::Control, &target)),
        (
            "control_next_turn",
            next_turn(&seed, &reply, Arm::Control, &target),
        ),
        ("tagged_tool_loop", tool_loop(&seed, Arm::Tagged, &target)),
        (
            "tagged_next_turn",
            next_turn(&seed, &reply, Arm::Tagged, &target),
        ),
    ];
    let mut described = Vec::new();
    for (label, opts) in &planned {
        let shape = render(opts)?;
        println!(
            "{}",
            json!({"event":"planned_request","label":label,"request":shape})
        );
        described.push(shape);
    }
    let [_, control_loop, control_next, tagged_loop, tagged_next] = &described[..] else {
        unreachable!("five planned requests");
    };
    let cleaning = cleaning_evidence(&seed);
    let tagged_loop_heads = heads(tagged_loop);
    let checks = json!({
        "control_untagged": none_tagged(control_loop) && none_tagged(control_next),
        "tagged_prefixed": all_tagged(tagged_loop) && all_tagged(tagged_next),
        "tag_numbers_consistent": heads(tagged_next).starts_with(&tagged_loop_heads),
        "phases_kept_in_both_arms": phases_sent(control_loop) == 1 && phases_sent(tagged_loop) == 1
            && phases_sent(control_next) == 2 && phases_sent(tagged_next) == 2,
        "encrypted_reasoning_echoed": control_loop["encrypted_reasoning_echoed"].as_u64() > Some(0)
            && tagged_loop["encrypted_reasoning_echoed"] == control_loop["encrypted_reasoning_echoed"]
            && tagged_next["encrypted_reasoning_echoed"] == control_next["encrypted_reasoning_echoed"],
        "cleaned_part_differs_from_model": cleaning["mark_removed"] == true
            && cleaning["sent_differs_from_model"] == true,
    });
    let passed = checks
        .as_object()
        .is_some_and(|checks| checks.values().all(|value| value == true));
    println!(
        "{}",
        json!({
            "event":"dry_run_finished", "provider_calls":0, "credential_reads":0,
            "renderer":"family-default ChatGPT frozen config with the recorded Codex wire identity; --live resolves through the catalog instead",
            "cleaning":cleaning, "checks":checks, "all_checks_passed":passed,
        })
    );
    if passed {
        println!("VERDICT: DRY RUN: no request sent; five planned request shapes printed.");
        Ok(0)
    } else {
        println!("VERDICT: DRY RUN SELF-CHECK FAILED: the planned requests do not have the intended shape.");
        Ok(1)
    }
}

async fn live(model: &str, variant: Option<&str>, gap: Duration) -> Result<i32, String> {
    use broca_catalog::live::{
        resolve_catalog_episode_with_client, AuthMode, EpisodeRenderConfig, ResolveOptions,
    };
    use broca_module_serve::{
        chatgpt_identity, claude_code_identity, family_for_method,
        vault_config::load_devtools_handle, AuthMethod, CredentialId,
    };
    use broca_protocol::BindIdentity;
    use broca_subc::{SubcConnection, VaultClient};
    use std::path::PathBuf;

    let slug = model_slug(model)?;
    let target = ModelHandle::new("openai", slug);
    let scope = format!("chatgpt-phase-tag-{}-{}", std::process::id(), now_ms());
    let bind = BindIdentity::new(
        std::env::current_dir().map_err(|_| "project directory unavailable")?,
        "chatgpt-phase-tag-probe",
        scope.clone(),
    );
    let connection_file = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let state_root = broca_module_serve::state_root::resolve_for_launch(&connection_file)
        .map_err(|_| "state root unavailable")?;
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::open(
        state_root.path.join("catalog/fusiform-lkg.json"),
    )));
    let credential = CredentialId::parse(CREDENTIAL_ID).ok_or("invalid fixed credential id")?;
    let handle = load_devtools_handle(&credential)
        .map_err(|_| "the standing chatgpt:openai dev-tools handle is unavailable")?;
    let connection = SubcConnection::connect(&connection_file)
        .await
        .map_err(|_| "subc connection unavailable")?;
    let vault = VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &bind,
        Duration::from_secs(15),
    );
    // Nothing refreshes a dev-tools handle mid-run. Twenty minutes covers five requests at their
    // full deadline plus the default gaps between them.
    let secret = vault
        .get(&handle, Some(20 * 60 * 1000), false)
        .await
        .result
        .map_err(|_| "vault credential unavailable")?;
    let install_seed = claude_code_identity::load_or_create_install_id(&state_root.path)
        .map_err(|_| "installation identity unavailable")?;
    let wire = chatgpt_identity::fresh(&install_seed, &bind, &scope, now_ms())
        .map_err(|_| "admission identity unavailable")?;
    let method = AuthMethod::Chatgpt;
    let resolve = ResolveOptions {
        auth_method: Some(method),
        family_override: family_for_method(method),
        session_id: Some(&scope),
        prompt_cache_key: Some(&scope),
        account_id: secret.account_id.as_deref(),
        frozen_account_ref: secret.account_id.as_deref(),
        variant,
        ..ResolveOptions::default()
    };
    let token = secret.token.clone();
    let account = secret.account_id.clone().unwrap_or_default();
    let secrets = [token.as_str(), account.as_str()];
    let resolved = resolve_catalog_episode_with_client(
        model,
        None,
        &AuthMode::Secret(secret.token),
        &resolve,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            chatgpt_wire: Some(&wire),
            ..EpisodeRenderConfig::default()
        },
    )
    .map_err(|error| {
        format!(
            "catalog resolution failed: {}",
            sanitize(&error.to_string(), &secrets)
        )
    })?;
    let provider = resolved.provider.as_ref();
    println!(
        "{}",
        json!({
            "event":"probe_started", "model":model, "variant":variant, "max_requests":5,
            "per_request_deadline_secs":STEP_DEADLINE_SECS, "gap_secs":gap.as_secs(),
            "fresh_transport_per_request":true, "credential":"withheld", "headers":"withheld",
        })
    );

    let (seed, _) = step(
        provider,
        1,
        "seed",
        &request(base_prompt(), &target),
        &secrets,
    )
    .await;
    let seed_phases = phases(&seed.content);
    let seed_has_tool_call = tool_results(&seed.content).is_some();
    if !seed.accepted() || seed_phases.is_empty() || !seed_has_tool_call {
        let mut missing = Vec::new();
        if !seed.accepted() {
            missing.push("a successful response");
        }
        if seed_phases.is_empty() {
            missing.push("a phase-bearing text part");
        }
        if !seed_has_tool_call {
            missing.push("a tool call");
        }
        provider.close_run_transport();
        println!(
            "VERDICT: INCONCLUSIVE: the seed reply lacks {}; no other request was sent. Shape: {}",
            missing.join(", "),
            reply_shape(&seed.content)
        );
        return Ok(2);
    }
    let seed_reasoning = has_encrypted_reasoning(&seed.content);
    let cleaning = cleaning_evidence(&seed.content);
    println!(
        "{}",
        json!({"event":"seed_accepted","phases":seed_phases,"encrypted_reasoning":seed_reasoning,"cleaning":cleaning})
    );

    let mut arms = Vec::new();
    for (arm, base, name) in [(Arm::Control, 2, "control"), (Arm::Tagged, 4, "tagged")] {
        tokio::time::sleep(gap).await;
        let loop_label = format!("{name}_tool_loop");
        let next_label = format!("{name}_next_turn");
        let (tool_loop_answer, tool_loop_sent) = step(
            provider,
            base,
            &loop_label,
            &tool_loop(&seed.content, arm, &target),
            &secrets,
        )
        .await;
        let next = if tool_loop_answer.accepted() && tool_loop_answer.received_assistant_message {
            tokio::time::sleep(gap).await;
            Some(
                step(
                    provider,
                    base + 1,
                    &next_label,
                    &next_turn(&seed.content, &tool_loop_answer.content, arm, &target),
                    &secrets,
                )
                .await,
            )
        } else {
            print_not_run(&next_label, "the tool-loop request was not accepted");
            None
        };
        arms.push((tool_loop_answer, tool_loop_sent, next));
    }
    provider.close_run_transport();

    let [(control_loop, control_loop_sent, control_next), (tagged_loop, tagged_loop_sent, tagged_next)] =
        &arms[..]
    else {
        unreachable!("two arms");
    };
    let accepted =
        |next: &Option<(Answer, Value)>| next.as_ref().is_some_and(|(a, _)| a.accepted());
    let cached = |next: &Option<(Answer, Value)>| next.as_ref().map(|(a, _)| a.cached());
    let tags_sent = all_tagged(tagged_loop_sent)
        && tagged_next
            .as_ref()
            .is_none_or(|(_, sent)| all_tagged(sent));
    let control_untagged = none_tagged(control_loop_sent)
        && control_next
            .as_ref()
            .is_none_or(|(_, sent)| none_tagged(sent));
    let control_next_cached = cached(control_next);
    let tagged_next_cached = cached(tagged_next);
    println!(
        "{}",
        json!({
            "event":"probe_finished",
            "control_accepted":[control_loop.accepted(), accepted(control_next)],
            "tagged_accepted":[tagged_loop.accepted(), accepted(tagged_next)],
            "tool_loop_cached_input_tokens":{"control":control_loop.cached(),"tagged":tagged_loop.cached()},
            "next_turn_cached_input_tokens":{"control":control_next_cached,"tagged":tagged_next_cached},
            "tagged_requests_carried_prefix":tags_sent, "control_requests_untagged":control_untagged,
            "seed_encrypted_reasoning":seed_reasoning,
        })
    );
    if !tags_sent || !control_untagged {
        return Err(
            "the requests sent do not have the intended tagging; the result says nothing".into(),
        );
    }
    if !control_loop.accepted() || !accepted(control_next) {
        println!("VERDICT: INCONCLUSIVE: the untagged control arm was not accepted, so there is no baseline.");
        return Ok(2);
    }
    if !tagged_loop.accepted() || !accepted(tagged_next) {
        println!("VERDICT: REJECTED: the backend refused tagged phase text that the untagged control got accepted with.");
        return Ok(3);
    }
    let (control_cached, tagged_cached) = (
        control_next_cached.unwrap_or(0),
        tagged_next_cached.unwrap_or(0),
    );
    if control_cached == 0 {
        println!("VERDICT: ACCEPTED; CACHE INCONCLUSIVE: tagged requests succeeded, but the control's next turn read no cache, so there is nothing to compare.");
        return Ok(2);
    }
    let share = tagged_cached as f64 / control_cached as f64;
    if share >= COMPARABLE_CACHE_SHARE {
        println!(
            "VERDICT: ACCEPTED: tagged requests succeeded like the control; next-turn cache read tagged/control = {tagged_cached}/{control_cached} ({share:.2})."
        );
        Ok(0)
    } else {
        println!(
            "VERDICT: CACHE DIVERGED: tagged requests succeeded, but next-turn cache read tagged/control = {tagged_cached}/{control_cached} ({share:.2}) is below {COMPARABLE_CACHE_SHARE}."
        );
        Ok(4)
    }
}

const USAGE: &str = "chatgpt-phase-tag-probe [--dry-run|--live] [--model openai/MODEL] [--variant NAME|--no-variant] [--gap-secs N]";

fn usage_error(reason: &str) -> ! {
    eprintln!("probe_error={reason}\nusage: {USAGE}");
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let mut model = DEFAULT_MODEL.to_owned();
    let mut variant = Some(DEFAULT_VARIANT.to_owned());
    let mut mode = None;
    let mut gap_secs = 3u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" | "--live" => {
                let live = arg == "--live";
                if mode.is_some_and(|chosen| chosen != live) {
                    usage_error("conflicting_execution_modes");
                }
                mode = Some(live);
            }
            "--model" => model = args.next().unwrap_or_else(|| usage_error("missing_model")),
            "--variant" => {
                variant = Some(
                    args.next()
                        .unwrap_or_else(|| usage_error("missing_variant")),
                )
            }
            "--no-variant" => variant = None,
            "--gap-secs" => {
                gap_secs = args
                    .next()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_else(|| usage_error("gap_secs_must_be_a_number"))
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                return;
            }
            _ => usage_error("unknown_argument"),
        }
    }
    let result = if mode == Some(true) {
        live(&model, variant.as_deref(), Duration::from_secs(gap_secs)).await
    } else {
        dry_run(&model, variant.as_deref())
    };
    match result {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("probe failed: {error}");
            std::process::exit(1);
        }
    }
}
