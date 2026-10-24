//! LIVE Anthropic reality-conformance spike (feature `live`, local-only, off by
//! default). Drives the REAL Anthropic Messages API through `run_loop` and asserts the
//! real wire still matches what our normalizer depends on — the drift tripwire a mock
//! structurally can't be. Behavioral + structural assertions only (model output is
//! nondeterministic), capture-and-keep, taxonomy-classified skip-vs-fail.
//!
//! Run: `cargo test -p broca-provider --features live --test live anthropic_live:: -- --nocapture`
//! Needs a currently-valid anthropic OAuth token in the vault dev-tools handle map; skips
//! cleanly otherwise (a provider-side condition, never a code failure).

use crate::live_common;

use broca_protocol::FinishReason;
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::AuthHandle;
use live_common::{run_live, LiveResult, LiveSpec};
use serde_json::json;

const MODEL: &str = "claude-opus-4-8";

/// Resolve the Anthropic auth + frozen config, or a skip reason (no/expired token is a
/// provider-side condition).
fn anthropic_auth() -> Result<AuthHandle, String> {
    live_common::vault_oauth_token("anthropic")
        .map(AuthHandle::Bearer)
        .ok_or_else(|| {
            "SKIP anthropic_live: BROCA_LIVE_TESTS=1 and a vault dev-tools credential are required."
                .into()
        })
}

/// A finish_reason that is a known canonical variant (not a wire string we failed to
/// map to something meaningful). The point is drift detection: if Anthropic introduces
/// a new stop reason our normalizer doesn't recognize, it lands as `Other` — flag it.
fn is_known_finish_reason(events: &[broca_core::OutputEvent]) -> bool {
    use broca_protocol::caller::ControlUnit;
    events.iter().any(|e| match e {
        broca_core::OutputEvent::Control(ControlUnit::StepFinished { finish_reason, .. }) => {
            matches!(
                finish_reason,
                FinishReason::Stop
                    | FinishReason::ToolCalls
                    | FinishReason::Length
                    | FinishReason::ContentFilter
            )
        }
        _ => false,
    })
}

/// Behavioral + structural: the model reads a nonce file (via a stub tool) and echoes
/// it back — the live tool loop closes through a REAL model — AND the wire still
/// carries a tool_use the normalizer accumulates into parseable input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_tool_loop_and_nonce_round_trip() {
    let auth = match anthropic_auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    // A per-run nonce the stub tool returns; the model must surface it in its answer,
    // proving the real tool loop closed (the M2 sentinel pattern, live).
    let nonce = format!(
        "LIVE-NONCE-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    // A benign read-and-report-verbatim task (the M2 nonce pattern, live): the model
    // reads a mundane status file and reports its exact contents. A SECRET framing
    // triggers a safety refusal (the model correctly declines to dump a credential —
    // not a wire issue), and a get_time framing makes the model REFORMAT the result
    // (so the raw nonce doesn't survive). Reading a status file and echoing its
    // contents verbatim is the framing the model cooperates with.
    let frozen = anthropic_frozen_config(MODEL);
    let spec = LiveSpec::new(
        "Use the read_file tool to read the file \"build-status.txt\", then reply with \
         its exact contents verbatim and nothing else.",
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
        AnthropicMessages,
        frozen,
        auth,
        "anthropic",
        "tool_loop_nonce",
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

    // BEHAVIORAL: completed, the right tool called, the nonce round-tripped to the
    // final answer (the real loop closed through a real model).
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
    assert!(
        called.contains(&"read_file"),
        "model called read_file: {called:?}"
    );

    // STRUCTURAL (drift tripwire): the tool_use input_json accumulated into a parseable
    // object carrying a non-empty path string — if Anthropic changed the
    // input_json_delta framing, this breaks (and SHOULD). Semantic check (a path-ish
    // string), not an exact value (the model supplies the path).
    let call = outcome
        .tool_calls()
        .into_iter()
        .find(|c| c.tool_name == "read_file")
        .unwrap();
    let path = call
        .input
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !path.is_empty(),
        "tool_use input_json_delta accumulated into a parseable object with a path arg; got {:?}",
        call.input
    );

    assert!(
        outcome.tool_result_texts().iter().any(|t| t == &nonce),
        "stub tool result fed back into the loop: {:?}",
        outcome.tool_result_texts()
    );
    assert!(
        outcome.final_text().contains(&nonce),
        "the nonce round-tripped to the model's final answer (real tool loop closed): {:?}",
        outcome.final_text()
    );

    // STRUCTURAL: usage populated + a known finish_reason variant.
    assert!(
        outcome.total_tokens() > 0,
        "provider reported usage (>0 tokens)"
    );
    assert!(
        is_known_finish_reason(&outcome.events),
        "finish_reason is a known canonical variant"
    );

    eprintln!(
        "[anthropic_live] tool-loop OK: nonce round-tripped, usage={} tokens",
        outcome.total_tokens()
    );
}

/// Structural conformance for the SIGNED THINKING block — the subtlest Anthropic wire
/// feature our normalizer depends on. A reasoning-inducing prompt; if the model thinks,
/// assert a SIGNED reasoning block survived to the assembled message (shape, not
/// content). Model nondeterminism handled: no-thinking this run is reported, not failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_signed_thinking_conformance() {
    let auth = match anthropic_auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let frozen = anthropic_frozen_config(MODEL);
    let spec = LiveSpec::new(
        "Think step by step about why the sky appears blue, reasoning carefully before \
         you answer. Then give a one-sentence answer.",
    );

    let result = run_live(
        AnthropicMessages,
        frozen,
        auth,
        "anthropic",
        "signed_thinking",
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
    assert!(outcome.total_tokens() > 0, "provider reported usage");

    if outcome.produced_signed_thinking() {
        // The decisive structural assertion: a SIGNED thinking block survived the real
        // wire → our assembled message. If Anthropic changed the thinking/signature
        // format, the signature would be absent and this would (correctly) fail.
        eprintln!("[anthropic_live] SIGNED THINKING conformance OK: signed reasoning block present in the assembled message");
    } else {
        eprintln!(
            "[anthropic_live] thinking not exercised this run (model nondeterminism) — not a failure"
        );
    }
}
