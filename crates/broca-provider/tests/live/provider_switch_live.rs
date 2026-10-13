//! LIVE provider-switch continuation (feature `live`, local-only, off by default).
//!
//! The portable-session proof: drive a conversation under provider A (with real tool
//! calls that establish a fact only present in A's tool-RESULT history), then start a
//! run under provider B seeded with A's full canonical history, and assert B continues
//! A's conversation — references the A-established fact AND produces no provider error.
//!
//! Why this is a real proof, not a vacuous one:
//!  - The fact (a per-run nonce) exists ONLY in A's tool-result history — it is not
//!    derivable from the prompt — so B can only answer by actually reading the seeded
//!    history. A nonce reference is therefore PROOF the canonical history transferred
//!    and B consumed it.
//!  - The seed carries A's assistant turns VERBATIM, including any provider-signed
//!    reasoning blocks. So B's renderer genuinely exercises its drop/translate-foreign-
//!    state path. The failure mode this catches — "real B REJECTS an A-shaped history"
//!    (a foreign signed-thinking block, or a tool id B can't bind) — is exactly what a
//!    mock can't surface, because a mock B accepts whatever we feed it.
//!
//! Run: `cargo test -p broca-provider --features live --test live provider_switch_live:: -- --nocapture`
//!
//! Deferred (enrich later): the A→B→A re-echo edge — whether a SECOND Anthropic render
//! tries to echo A's now-out-of-position original thinking signature, and whether real
//! Anthropic rejects a context-bound signature out of order. Subtle; not built here.

use crate::live_common;

use broca_protocol::content::Message;
use broca_provider::families::{
    anthropic_frozen_config, gemini_frozen_config, openai_chat_frozen_config, AnthropicMessages,
    Gemini, OpenAiChat,
};
use broca_provider::wire::{AuthHandle, FrozenRenderConfig};
use live_common::{
    gemini_api_key, opencode_api_key, opencode_oauth_token, run_live, LiveOutcome, LiveResult,
    LiveSpec,
};
use serde_json::json;

const ANTHROPIC_MODEL: &str = "claude-opus-4-8";
const GEMINI_MODEL: &str = "gemini-2.5-flash";
const DEEPSEEK_MODEL: &str = "deepseek-v4-flash";

// ───────────────────────── auth (skip-clean when absent) ─────────────────────────

fn anthropic_auth() -> Option<AuthHandle> {
    opencode_oauth_token("anthropic").map(AuthHandle::Bearer)
}

fn gemini_auth() -> Option<AuthHandle> {
    gemini_api_key().map(|key| AuthHandle::ApiKeyHeader {
        header: "x-goog-api-key".into(),
        value: key,
    })
}

/// DeepSeek backs the OpenAI-Chat family live (openai-compatible at a different host).
fn deepseek_auth() -> Option<(AuthHandle, FrozenRenderConfig)> {
    let key = opencode_api_key("deepseek")?;
    let mut frozen = openai_chat_frozen_config(DEEPSEEK_MODEL);
    frozen.base_url = Some("https://api.deepseek.com".into());
    Some((AuthHandle::Bearer(key), frozen))
}

// ───────────────────────── shared scenario ─────────────────────────

/// The tool every "provider A" leg offers: it returns a per-run nonce as its result, so
/// the nonce lands ONLY in A's tool-result history. The model is asked to look it up.
fn nonce_tool_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": { "key": { "type": "string", "description": "the record key to read" } },
        "required": ["key"]
    })
}

/// Drive provider A: ask it to read a "build record" via the nonce tool, so A's history
/// contains the assistant tool_call + the tool_result carrying the nonce. Returns A's
/// reconstructed canonical history, or a skip reason.
async fn run_provider_a(
    family: impl broca_provider::wire::WireFamily + 'static,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    a_name: &str,
    switch: &str,
    nonce: &str,
) -> Result<Vec<Message>, String> {
    let spec = LiveSpec::new(
        "Call the read_record tool with key \"build\" to look up the build record, then \
         tell me the exact value it returned.",
    )
    .tool("read_record", nonce_tool_schema(), nonce);

    // The scenario must be UNIQUE per test: it derives the run's temp dir + session
    // (the lease key), so two tests that share an `a_name` (e.g. both anthropic-as-A)
    // would otherwise collide on the lease when the suite runs in parallel. The
    // `switch` pair tag makes each test's leg its own session.
    match run_live(
        family,
        frozen,
        auth,
        "provider_switch",
        &format!("{switch}_{a_name}_leg_a"),
        spec,
    )
    .await
    {
        LiveResult::Skip(reason) => Err(reason),
        LiveResult::Ran(outcome) => {
            // Sanity: A must actually have called the tool, or the history wouldn't
            // carry the nonce and the switch test would be vacuous.
            if !outcome
                .tool_calls()
                .iter()
                .any(|c| c.tool_name == "read_record")
            {
                return Err(format!(
                    "provider A ({a_name}) did not call read_record — history would not carry \
                     the nonce; cannot prove a switch. final: {:?}",
                    outcome.final_text()
                ));
            }
            Ok(outcome.canonical_history())
        }
    }
}

/// Seed provider B with A's history and ask a question only answerable from it.
async fn run_provider_b(
    family: impl broca_provider::wire::WireFamily + 'static,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    b_name: &str,
    switch: &str,
    history: Vec<Message>,
) -> Result<LiveOutcome, String> {
    let spec = LiveSpec::new(
        "What exact value did the build record contain? Answer with just that value.",
    )
    .seed(history);

    // Unique per test (see run_provider_a): two tests sharing a `b_name` (e.g. both
    // *-to-gemini) must not collide on the leg-B session lease under parallel runs.
    match run_live(
        family,
        frozen,
        auth,
        "provider_switch",
        &format!("{switch}_{b_name}_leg_b"),
        spec,
    )
    .await
    {
        LiveResult::Skip(reason) => Err(reason),
        LiveResult::Ran(outcome) => Ok(outcome),
    }
}

fn nonce(tag: &str) -> String {
    format!("SWITCH-{tag}-{}", std::process::id())
}

// ───────────────────────── the switches ─────────────────────────

/// PRIORITY case (most likely real gap): Anthropic → Gemini. Anthropic tool calls carry
/// `toolu_` ids and the assistant turns carry SIGNED thinking; Gemini matches
/// functionResponse→functionCall by NAME (not id) and rejects foreign thinking. So this
/// exercises BOTH the drop-foreign-thinking path AND the tool-id→name binding on a
/// foreign-id history. If Gemini's functionResponse doesn't bind (name lookup misses
/// because the seeded result's id is an Anthropic id), that is a REAL finding — Gemini's
/// convert needs cross-provider id→name handling — NOT a test to weaken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_to_gemini_continues() {
    let (Some(a_auth), Some(b_auth)) = (anthropic_auth(), gemini_auth()) else {
        eprintln!("SKIP anthropic_to_gemini: missing anthropic token or gemini key");
        return;
    };
    let n = nonce("ANT-GEM");

    let history = match run_provider_a(
        AnthropicMessages,
        anthropic_frozen_config(ANTHROPIC_MODEL),
        a_auth,
        "anthropic",
        "ant_gem",
        &n,
    )
    .await
    {
        Ok(h) => h,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let outcome = match run_provider_b(
        Gemini,
        gemini_frozen_config(GEMINI_MODEL),
        b_auth,
        "gemini",
        "ant_gem",
        history,
    )
    .await
    {
        Ok(o) => o,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    assert!(
        outcome.completed(),
        "Gemini accepted the Anthropic-shaped history (foreign tool ids + dropped signed \
         thinking) and completed (reason={:?})",
        outcome.summary.reason
    );
    assert!(
        outcome.final_text().contains(&n),
        "Gemini continued the Anthropic conversation — surfaced the nonce {n:?}. answer: {:?}",
        outcome.final_text()
    );
    assert!(outcome.total_tokens() > 0, "Gemini reported usage");
    eprintln!("[provider_switch] anthropic→gemini OK: nonce {n:?} survived the switch");
}

/// Anthropic → OpenAI-Chat (DeepSeek): the thinking-drop proof. A's signed thinking is
/// carried in the seed; OpenAi-Chat's renderer drops it (it doesn't echo thinking). B
/// must still continue from the tool-result nonce.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anthropic_to_openai_chat_continues() {
    let (Some(a_auth), Some((b_auth, b_frozen))) = (anthropic_auth(), deepseek_auth()) else {
        eprintln!("SKIP anthropic_to_openai_chat: missing anthropic token or deepseek key");
        return;
    };
    let n = nonce("ANT-OAI");

    let history = match run_provider_a(
        AnthropicMessages,
        anthropic_frozen_config(ANTHROPIC_MODEL),
        a_auth,
        "anthropic",
        "ant_oai",
        &n,
    )
    .await
    {
        Ok(h) => h,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    let outcome = match run_provider_b(
        OpenAiChat,
        b_frozen,
        b_auth,
        "openai_chat",
        "ant_oai",
        history,
    )
    .await
    {
        Ok(o) => o,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    assert!(
        outcome.completed(),
        "OpenAI-Chat accepted the Anthropic-shaped history (signed thinking dropped by its \
         renderer) and completed (reason={:?})",
        outcome.summary.reason
    );
    assert!(
        outcome.final_text().contains(&n),
        "OpenAI-Chat continued the Anthropic conversation — surfaced the nonce {n:?}. answer: {:?}",
        outcome.final_text()
    );
    assert!(outcome.total_tokens() > 0, "OpenAI-Chat reported usage");
    eprintln!("[provider_switch] anthropic→openai-chat OK: nonce {n:?} survived");
}

/// OpenAI-Chat (DeepSeek) → Anthropic: the inverse — a history WITHOUT any thinking
/// blocks seeded into Anthropic. Proves Anthropic accepts a continuation with no prior
/// signed thinking (it should, since thinking is optional), and that OpenAI `call_` tool
/// ids round-trip into Anthropic's tool_use/tool_result shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_to_anthropic_continues() {
    let (Some((a_auth, a_frozen)), Some(b_auth)) = (deepseek_auth(), anthropic_auth()) else {
        eprintln!("SKIP openai_chat_to_anthropic: missing deepseek key or anthropic token");
        return;
    };
    let n = nonce("OAI-ANT");

    let history =
        match run_provider_a(OpenAiChat, a_frozen, a_auth, "openai_chat", "oai_ant", &n).await {
            Ok(h) => h,
            Err(skip) => {
                eprintln!("{skip}");
                return;
            }
        };

    let outcome = match run_provider_b(
        AnthropicMessages,
        anthropic_frozen_config(ANTHROPIC_MODEL),
        b_auth,
        "anthropic",
        "oai_ant",
        history,
    )
    .await
    {
        Ok(o) => o,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    assert!(
        outcome.completed(),
        "Anthropic accepted the OpenAI-shaped history (no prior thinking) and completed \
         (reason={:?})",
        outcome.summary.reason
    );
    assert!(
        outcome.final_text().contains(&n),
        "Anthropic continued the OpenAI conversation — surfaced the nonce {n:?}. answer: {:?}",
        outcome.final_text()
    );
    assert!(outcome.total_tokens() > 0, "Anthropic reported usage");
    eprintln!("[provider_switch] openai-chat→anthropic OK: nonce {n:?} survived");
}

/// OpenAI-Chat (DeepSeek) → Gemini: the second tool-id-translation pair — OpenAI `call_`
/// ids into Gemini's name-matched functionResponse. Complements the Anthropic→Gemini
/// case with a different foreign id format.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openai_chat_to_gemini_continues() {
    let (Some((a_auth, a_frozen)), Some(b_auth)) = (deepseek_auth(), gemini_auth()) else {
        eprintln!("SKIP openai_chat_to_gemini: missing deepseek key or gemini key");
        return;
    };
    let n = nonce("OAI-GEM");

    let history =
        match run_provider_a(OpenAiChat, a_frozen, a_auth, "openai_chat", "oai_gem", &n).await {
            Ok(h) => h,
            Err(skip) => {
                eprintln!("{skip}");
                return;
            }
        };

    let outcome = match run_provider_b(
        Gemini,
        gemini_frozen_config(GEMINI_MODEL),
        b_auth,
        "gemini",
        "oai_gem",
        history,
    )
    .await
    {
        Ok(o) => o,
        Err(skip) => {
            eprintln!("{skip}");
            return;
        }
    };

    assert!(
        outcome.completed(),
        "Gemini accepted the OpenAI-shaped history (call_ ids name-matched) and completed \
         (reason={:?})",
        outcome.summary.reason
    );
    assert!(
        outcome.final_text().contains(&n),
        "Gemini continued the OpenAI conversation — surfaced the nonce {n:?}. answer: {:?}",
        outcome.final_text()
    );
    assert!(outcome.total_tokens() > 0, "Gemini reported usage");
    eprintln!("[provider_switch] openai-chat→gemini OK: nonce {n:?} survived");
}
