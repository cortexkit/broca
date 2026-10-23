//! LIVE OpenAI-Responses reality-conformance (feature `live`, local-only, off by
//! default). Drives the real OpenAI `/v1/responses` API and asserts the real event
//! grammar (typed `response.output_text.delta` / `response.function_call_*` events,
//! finalize-on-EOF) still matches the Responses normalizer. Skips cleanly if the
//! openai OAuth token is absent/expired or the provider is unavailable.
//!
//! Run: `cargo test -p broca-provider --features live --test live openai_responses_live:: -- --nocapture`

use crate::live_common;

use broca_provider::families::{openai_responses_frozen_config, OpenAiResponses};
use broca_provider::wire::AuthHandle;
use live_common::{openai_platform_key, run_live, LiveResult, LiveSpec};
use serde_json::json;

const MODEL: &str = "gpt-4o-mini";

// /v1/responses requires the api.responses.write scope, which the ChatGPT OAuth token
// (ChatGPT/Codex) lacks (it 401s). So this sources the OpenAI PLATFORM key specifically.
fn auth() -> Result<AuthHandle, String> {
    openai_platform_key()
        .map(AuthHandle::Bearer)
        .ok_or_else(|| {
            "SKIP openai_responses_live: no OpenAI platform key ($OPENAI_API_KEY or \
         ~/.config/openai.key)."
                .into()
        })
}

fn frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut f = openai_responses_frozen_config(MODEL);
    f.max_output_tokens = 512;
    f
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_responses_text_completes() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let spec = LiveSpec::new("Reply with exactly the single word: PONG");
    let result = run_live(
        OpenAiResponses,
        frozen(),
        auth,
        "openai_responses",
        "text_completes",
        spec,
    )
    .await;
    let outcome = match result {
        LiveResult::Skip(reason) => {
            eprintln!("{reason}");
            return;
        }
        LiveResult::Ran(o) => o,
    };

    assert!(
        outcome.completed(),
        "live run completed (reason={:?})",
        outcome.summary.reason
    );
    assert!(
        !outcome.final_text().trim().is_empty(),
        "model produced a non-empty answer"
    );
    assert!(
        outcome.total_tokens() > 0,
        "provider reported usage (>0 tokens)"
    );
    eprintln!(
        "[openai_responses_live] text OK: {:?} usage={} tokens",
        outcome.final_text().trim(),
        outcome.total_tokens()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_responses_tool_call_args_accumulate() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let nonce = format!("OAI-RESP-{}", std::process::id());
    let spec = LiveSpec::new(
        "Use the read_file tool to read \"build-status.txt\", then reply with its exact \
         contents verbatim and nothing else.",
    )
    .tool(
        "read_file",
        json!({
            "type": "object",
            "properties": { "path": { "type": "string", "description": "the file path to read" } },
            "required": ["path"]
        }),
        &nonce,
    );

    let result = run_live(
        OpenAiResponses,
        frozen(),
        auth,
        "openai_responses",
        "tool_call",
        spec,
    )
    .await;
    let outcome = match result {
        LiveResult::Skip(reason) => {
            eprintln!("{reason}");
            return;
        }
        LiveResult::Ran(o) => o,
    };

    assert!(
        outcome.completed(),
        "live run completed (reason={:?})",
        outcome.summary.reason
    );
    let called: Vec<&str> = outcome
        .tool_calls()
        .iter()
        .map(|c| c.tool_name.as_str())
        .collect();
    if let Some(call) = outcome
        .tool_calls()
        .into_iter()
        .find(|c| c.tool_name == "read_file")
    {
        let path = call
            .input
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        assert!(
            !path.is_empty(),
            "Responses function_call argument fragments accumulated into a parseable object; got {:?}",
            call.input
        );
        assert!(
            outcome.tool_result_texts().iter().any(|t| t == &nonce),
            "stub tool result fed back: {:?}",
            outcome.tool_result_texts()
        );
        eprintln!("[openai_responses_live] tool-call wire OK: read_file({path:?})");
    } else {
        eprintln!("[openai_responses_live] tool not called this run (model nondeterminism) — not a failure: {called:?}");
    }
    assert!(outcome.total_tokens() > 0, "provider reported usage");
}
