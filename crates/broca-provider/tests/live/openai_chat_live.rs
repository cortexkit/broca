//! LIVE OpenAI-Chat reality-conformance (feature `live`, local-only, off by default).
//!
//! The OpenAI-Chat family backs the entire openai-compatible long tail, so its live
//! conformance target is a real openai-compatible server. We use DeepSeek (an
//! openai-chat-wire server, api-key auth, cheap + reliable) — it proves the family's
//! normalizer assumptions (index-keyed tool_call argument fragments, `[DONE]`
//! sentinel, finalize-on-EOF, usage shape) still match a real `/chat/completions`
//! stream. Skips cleanly if the deepseek key is absent or the provider is unavailable.
//!
//! Run: `cargo test -p broca-provider --features live --test live openai_chat_live:: -- --nocapture`

use crate::live_common;

use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::AuthHandle;
use live_common::{run_live, vault_api_key, LiveResult, LiveSpec};
use serde_json::json;

const BASE_URL: &str = "https://api.deepseek.com";
const MODEL: &str = "deepseek-v4-flash";

fn auth() -> Result<AuthHandle, String> {
    // TWO ABSENCES MUST NOT SHARE ONE MESSAGE. `vault_api_key` returns None both when the key
    // is missing AND when BROCA_LIVE_TESTS consent is absent, so naming only the key sends a
    // reader hunting a credential they may already have. Reported from outside: the skip line read
    // "no deepseek api key" on a machine that had one.
    vault_api_key("deepseek")
        .map(AuthHandle::Bearer)
        .ok_or_else(|| {
            if std::env::var("BROCA_LIVE_TESTS").is_ok_and(|v| v == "1") {
                "SKIP openai_chat_live: consent is given, but no deepseek API key available through its vault dev-tools handle."
                    .into()
            } else {
                "SKIP openai_chat_live: BROCA_LIVE_TESTS is not 1, so no live call was attempted \
                 (a deepseek key may or may not be present)."
                    .into()
            }
        })
}

fn frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut f = openai_chat_frozen_config(MODEL);
    f.base_url = Some(BASE_URL.to_string());
    f.max_output_tokens = 512;
    f
}

/// Behavioral: a one-turn text answer completes with populated usage and a known
/// finish_reason — proves the real `/chat/completions` stream still assembles through
/// the normalizer (finalize-on-EOF, `[DONE]` handling).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_text_completes() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let spec = LiveSpec::new("Reply with exactly the single word: PONG");
    let result = run_live(
        OpenAiChat,
        frozen(),
        auth,
        "openai_chat",
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
    // STRUCTURAL (drift tripwire): the real server reported usage. If the usage shape
    // changed, this would read 0.
    assert!(
        outcome.total_tokens() > 0,
        "provider reported usage (>0 tokens)"
    );
    eprintln!(
        "[openai_chat_live] text OK: {:?} usage={} tokens",
        outcome.final_text().trim(),
        outcome.total_tokens()
    );
}

/// Structural drift tripwire: a tool call over the real wire. The model calls the tool
/// with a parseable argument object — if the real server changed its index-keyed
/// tool_call argument-fragment framing, the accumulated input would not parse and this
/// would (correctly) fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_tool_call_args_accumulate() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let nonce = format!("OAI-CHAT-{}", std::process::id());
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

    let result = run_live(OpenAiChat, frozen(), auth, "openai_chat", "tool_call", spec).await;
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
    // The model may decline the tool; only assert wire-conformance when it DID call —
    // a no-tool run is model nondeterminism, reported not failed.
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
            "tool_call argument fragments accumulated into a parseable object with a path; got {:?}",
            call.input
        );
        assert!(
            outcome.tool_result_texts().iter().any(|t| t == &nonce),
            "stub tool result fed back: {:?}",
            outcome.tool_result_texts()
        );
        eprintln!("[openai_chat_live] tool-call wire OK: read_file({path:?})");
    } else {
        eprintln!("[openai_chat_live] tool not called this run (model nondeterminism) — not a failure: {called:?}");
    }
    assert!(outcome.total_tokens() > 0, "provider reported usage");
}
