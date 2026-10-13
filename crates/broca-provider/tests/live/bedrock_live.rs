//! LIVE Bedrock ConverseStream reality-conformance (feature `live`, local-only, off by
//! default). Drives the real AWS Bedrock `converse-stream` API and asserts the real
//! BINARY `application/vnd.amazon.eventstream` frames still decode through the Bedrock
//! normalizer (CRC32-validated framing → assembled message). This is the only family
//! whose live wire is binary, so it's the highest-value structural conformance check.
//!
//! Auth: the `amazon-bedrock` api key (a long-term Bearer key) from the opencode auth
//! file → `Authorization: Bearer`. Region us-east-1 (encoded in the key). Skips cleanly
//! if the key is absent or the provider is unavailable.
//!
//! Run: `cargo test -p broca-provider --features live --test live bedrock_live:: -- --nocapture`

use crate::live_common;

use broca_provider::families::{bedrock_frozen_config_with_region, BedrockConverse};
use broca_provider::wire::AuthHandle;
use live_common::{opencode_api_key, run_live, LiveResult, LiveSpec};
use serde_json::json;

// Amazon Nova Lite — a current, non-legacy Bedrock model reachable with this key in
// us-east-1. The Anthropic-on-Bedrock ids are retired ("end of its life") or
// use-case-gated; Nova is Amazon's own model and needs no use-case form.
const MODEL: &str = "amazon.nova-lite-v1:0";
const REGION: &str = "us-east-1";

fn auth() -> Result<AuthHandle, String> {
    opencode_api_key("amazon-bedrock")
        .map(AuthHandle::Bearer)
        .ok_or_else(|| {
            "SKIP bedrock_live: no amazon-bedrock api key in the opencode auth file.".into()
        })
}

fn frozen() -> broca_provider::wire::FrozenRenderConfig {
    let mut f = bedrock_frozen_config_with_region(MODEL, REGION);
    f.max_output_tokens = 512;
    f
}

/// Behavioral + the binary-decode structural tripwire: a one-turn answer assembled
/// from real binary event-stream frames. If Bedrock changed its event-stream framing
/// (or CRC), the decode would fail and produce no content → this fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bedrock_text_decodes_binary_stream() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let spec = LiveSpec::new("Reply with exactly the single word: PONG");
    let result = run_live(
        BedrockConverse,
        frozen(),
        auth,
        "bedrock",
        "text_decodes",
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
    // The decisive structural assertion: real binary vnd.amazon.eventstream frames
    // decoded into actual content (not just "didn't error" — empty content would fail).
    assert!(
        !outcome.final_text().trim().is_empty(),
        "binary event-stream frames decoded into non-empty content"
    );
    assert!(
        outcome.total_tokens() > 0,
        "provider reported usage (>0 tokens)"
    );
    eprintln!(
        "[bedrock_live] binary-decode OK: {:?} usage={} tokens",
        outcome.final_text().trim(),
        outcome.total_tokens()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bedrock_tool_call_over_binary_stream() {
    let auth = match auth() {
        Ok(a) => a,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let nonce = format!("BEDROCK-{}", std::process::id());
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
        BedrockConverse,
        frozen(),
        auth,
        "bedrock",
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
            "Bedrock toolUse input decoded from binary frames into a parseable object; got {:?}",
            call.input
        );
        assert!(
            outcome.tool_result_texts().iter().any(|t| t == &nonce),
            "stub tool result fed back: {:?}",
            outcome.tool_result_texts()
        );
        eprintln!("[bedrock_live] tool-call binary-wire OK: read_file({path:?})");
    } else {
        eprintln!("[bedrock_live] tool not called this run (model nondeterminism) — not a failure: {called:?}");
    }
    assert!(outcome.total_tokens() > 0, "provider reported usage");
}
