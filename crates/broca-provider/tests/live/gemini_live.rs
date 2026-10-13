//! LIVE Gemini reality-conformance (feature `live`, local-only, off by default).
//!
//! Drives the real Gemini `streamGenerateContent` API and asserts the real
//! `candidates[].content.parts` grammar still matches the Gemini normalizer. This is
//! OPPORTUNISTIC conformance: the available key is FREE-TIER quota-limited and 429s
//! frequently, so a run that SKIPS-clean (quota exhausted) is HONEST — it just
//! couldn't verify this run; when it DOES get through it proves the real Gemini wire
//! matches the normalizer. A 429 is a provider-side condition (Transient) → SKIP, never
//! a failure.
//!
//! Auth: `$GEMINI_API_KEY` if set, else `~/.config/programbench/gemini.key`, else skip
//! (the key authenticates via the `x-goog-api-key` header). Model: gemini-2.5-flash.
//!
//! Run: `cargo test -p broca-provider --features live --test live gemini_live:: -- --nocapture`

use crate::live_common;

use broca_provider::families::{gemini_frozen_config, Gemini};
use broca_provider::wire::AuthHandle;
use live_common::{gemini_api_key, run_live, LiveResult, LiveSpec};
use serde_json::json;

const MODEL: &str = "gemini-2.5-flash";

fn auth() -> Result<AuthHandle, String> {
    gemini_api_key()
        .map(|key| AuthHandle::ApiKeyHeader { header: "x-goog-api-key".to_string(), value: key })
        .ok_or_else(|| {
            "SKIP gemini_live: no gemini key — set $GEMINI_API_KEY or ~/.config/programbench/gemini.key.".into()
        })
}

fn frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut f = gemini_frozen_config(MODEL);
    f.max_output_tokens = 512;
    f
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gemini_text_completes() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let spec = LiveSpec::new("Reply with exactly the single word: PONG");
    let result = run_live(Gemini, frozen(), auth, "gemini", "text_completes", spec).await;
    let outcome = match result {
        // A 429 (free-tier quota) lands here as a clean skip — opportunistic conformance.
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
    // STRUCTURAL drift tripwire: the real Gemini wire's candidates/parts grammar
    // assembled through the normalizer (non-empty text + usage), and finalize-on-EOF
    // produced the message (Gemini has no in-band terminal event).
    assert!(
        outcome.total_tokens() > 0,
        "provider reported usageMetadata (>0 tokens)"
    );
    eprintln!(
        "[gemini_live] text OK: {:?} usage={} tokens",
        outcome.final_text().trim(),
        outcome.total_tokens()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gemini_tool_call_args_accumulate() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let nonce = format!("GEMINI-{}", std::process::id());
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

    let result = run_live(Gemini, frozen(), auth, "gemini", "tool_call", spec).await;
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
            "Gemini functionCall args decoded into a parseable object with a path; got {:?}",
            call.input
        );
        assert!(
            outcome.tool_result_texts().iter().any(|t| t == &nonce),
            "stub tool result fed back: {:?}",
            outcome.tool_result_texts()
        );
        eprintln!("[gemini_live] tool-call wire OK: read_file({path:?})");
    } else {
        eprintln!("[gemini_live] tool not called this run (model nondeterminism) — not a failure: {called:?}");
    }
    assert!(outcome.total_tokens() > 0, "provider reported usage");
}
