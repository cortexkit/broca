use std::collections::HashMap;
use std::sync::Arc;

use cortexkit_role_harness::HarnessError;
use cortexkit_role_llm_runner_conformance::{ModelToolResultObservation, ScriptedToolOutcome};
use serde_json::Value;
use subc_client_rs::{HandlerOutcome, ModuleStopHandle};

use super::{BrocaSubject, Live};

pub(super) type ScriptedOutcomes = HashMap<(String, String), ScriptedToolOutcome>;

const PROVIDER_ERROR: &str = "conformance_tool_error";

pub(super) fn needs_private_runner(session: &str) -> bool {
    // The conformance harness opens cf-<label>-<serial> before configuring the outcome.
    // The lost-reply session needs its own provider: stopping the shared one breaks other cases.
    session
        .strip_prefix("cf-cut-off-reply-")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|byte| byte.is_ascii_digit()))
}

pub(super) async fn start_private_runner(
    subject: &BrocaSubject,
    main: &Live,
    session: &str,
) -> Result<Arc<Live>, HarnessError> {
    let root = main
        .state_root
        .parent()
        .ok_or_else(|| HarnessError::new("runner fixture has no parent"))?
        .join("tool-outcomes")
        .join(session);
    // The session route owns the Live instance and its Drop reaps the daemon and its children.
    Ok(Arc::new(subject.start(&root).await?))
}

pub(super) async fn answer(
    outcome: ScriptedToolOutcome,
    stop: &ModuleStopHandle,
) -> HandlerOutcome {
    match outcome {
        ScriptedToolOutcome::ProviderError { message } => HandlerOutcome::Error {
            code: PROVIDER_ERROR.into(),
            message,
        },
        ScriptedToolOutcome::CutOff => {
            // Stop the private tool provider with the request still pending so the daemon
            // tears down the route without a reply.
            stop.stop();
            std::future::pending().await
        }
        _ => HandlerOutcome::Error {
            code: "unsupported_scripted_outcome".into(),
            message: "the conformance tool provider does not support this outcome".into(),
        },
    }
}

pub(super) fn observe_chat_results(request: &Value) -> Vec<ModelToolResultObservation> {
    request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|message| message["role"] == "tool")
        .filter_map(|message| {
            let id = message["tool_call_id"].as_str()?;
            let text = message["content"].as_str()?;
            let (is_error, code) = chat_error(text);
            Some(ModelToolResultObservation::new(id, is_error, code, text))
        })
        .collect()
}

fn chat_error(text: &str) -> (bool, Option<String>) {
    // Chat has no is_error flag. The two scripted failures are visible in the tool
    // message itself: a route error retains the provider code, and a lost reply
    // carries the runner's rendered error block. Plain tool text is not a flag.
    let mut lines = text.lines();
    let first = lines.next().unwrap_or_default();
    if let Some((code, _)) = first
        .strip_prefix("tool/route rejected: ")
        .and_then(|error| error.split_once(" — "))
        .filter(|(code, _)| *code == PROVIDER_ERROR)
    {
        return (true, Some(code.to_owned()));
    }
    if first.starts_with("outcome_unknown: ") {
        let code = lines.find_map(|line| line.strip_prefix("code: "));
        return (true, code.map(str::to_owned));
    }
    if let Some((tool, message)) = text
        .strip_prefix("Tool call `")
        .and_then(|rest| rest.split_once("` was not run: "))
    {
        if !tool.is_empty()
            && message
                == "the reply hit the output limit before its arguments were complete. Re-issue it with complete arguments, and split large payloads into smaller calls."
        {
            return (
                true,
                Some(
                    cortexkit_role_llm_runner::errors::tool_result_reasons::ARGUMENTS_INCOMPLETE
                        .into(),
                ),
            );
        }
    }
    (false, None)
}

#[test]
fn only_the_cut_off_reply_session_needs_a_private_tool_provider() {
    assert!(needs_private_runner("cf-cut-off-reply-123"));
    for other in [
        "cf-provider-error-122",
        "cf-sibling-cut-off-reply-124",
        "cf-cut-off-reply-",
        "cf-cut-off-reply-123/other",
    ] {
        assert!(!needs_private_runner(other), "{other}");
    }
}

#[test]
fn observes_native_chat_tool_results_in_request_order() {
    let refused = "tool/route rejected: conformance_tool_error — provider refused this call";
    let unknown = "outcome_unknown: the call may have run\ncode: outcome_unknown";
    let cut = "Tool call `lookup` was not run: the reply hit the output limit before its arguments were complete. Re-issue it with complete arguments, and split large payloads into smaller calls.";
    let request = serde_json::json!({"messages": [
        {"role": "assistant", "content": unknown},
        {"role": "tool", "tool_call_id": "ok", "content": "success"},
        {"role": "tool", "tool_call_id": "refused", "content": refused},
        {"role": "tool", "tool_call_id": "lost", "content": unknown},
        {"role": "tool", "tool_call_id": "uncoded", "content": "outcome_unknown: no reply"},
        {"role": "tool", "tool_call_id": "cut", "content": cut},
        {"role": "tool", "tool_call_id": "other", "content": "Tool call `lookup` was not run: another reason"}
    ]});
    let observed = super::observe_model_request(&request).tool_results;
    assert_eq!(
        observed,
        [
            ModelToolResultObservation::new("ok", false, None, "success"),
            ModelToolResultObservation::new(
                "refused",
                true,
                Some("conformance_tool_error".into()),
                refused,
            ),
            ModelToolResultObservation::new("lost", true, Some("outcome_unknown".into()), unknown),
            ModelToolResultObservation::new("uncoded", true, None, "outcome_unknown: no reply"),
            ModelToolResultObservation::new("cut", true, Some("arguments_incomplete".into()), cut,),
            ModelToolResultObservation::new(
                "other",
                false,
                None,
                "Tool call `lookup` was not run: another reason",
            ),
        ]
    );
}
