//! LIVE whole-stack demo (the dogfooding slice): a REAL Anthropic model driven
//! through `run_loop` to the REAL subc tool plane.
//!
//!   prompt → live Anthropic stream (reasoning + text) → model picks `get_time`
//!   → real subc route → fake-aft-stub returns → model answers.
//!
//! Gated on explicit `BROCA_LIVE_TESTS` consent and a valid OAuth token (read from the
//! OpenCode auth file; this test does not refresh tokens). Skips cleanly (clear message) if
//! consent is absent, the token is missing/expired, or the subc binaries are absent — never a
//! silent pass.
//!
//! This proves the live-model→loop→subc→tool path IN-PROCESS (LiveAnthropicProvider
//! is the 2nd Provider::step impl). It does NOT yet exercise a real stream through
//! the separate-process provider-module socket.

mod common;

use std::time::Instant;

use broca_core::{run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{ContentBlock, ToolOutput},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, DisplayEvent, ErrorClass, GenerationConfig, RunId, Usage,
};
use broca_provider::LiveAnthropicProvider;
use serde_json::json;
use tokio::sync::mpsc;

use common::{wait_for_aft, ModuleSpec, Rig};

const MODEL: &str = "claude-opus-4-8";
const TOOL_RESULT: &str = "14:32 (Europe/Madrid)";

fn live_anthropic_provider(label: &str, max_tokens: u32) -> Option<LiveAnthropicProvider> {
    let opted_in = std::env::var("BROCA_LIVE_TESTS")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if !opted_in {
        eprintln!("SKIP {label}: set BROCA_LIVE_TESTS to permit real provider calls.");
        return None;
    }

    match LiveAnthropicProvider::from_opencode_auth(MODEL) {
        Ok(provider) => Some(provider.with_max_tokens(max_tokens)),
        Err(error) => {
            eprintln!("SKIP {label}: {error}");
            None
        }
    }
}

/// THE full-stack-live integration proof: a REAL Anthropic model → run_loop → REAL
/// subc → the REAL `aft` binary. Every layer is real (no mock provider, no fake-aft
/// stub) — the model reads a per-run nonce file that real AFT resolves against the
/// sandbox project_root, and the nonce round-trips to the model's final answer,
/// proving the entire spine closed end to end with a live model in the loop.
///
/// Anthropic on purpose: once a run is explicitly opted in, this headline proof should use the
/// least quota-sensitive provider. The per-family in-process live tests cover the conformance
/// breadth (including quota/scope-prone Gemini and OpenAI Responses); this one stays on Anthropic,
/// whose token is valid for hours with no per-model quota.
///
/// Gated on: explicit live-call consent, a valid Anthropic OAuth token, and the real `aft` binary
/// (AFT_BIN, else the sibling checkout). Skips cleanly (provider-side / binary-absent) — never a
/// silent pass, never a code failure for a missing credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_model_reads_a_real_file_through_real_aft() {
    // 1. Live Anthropic provider (skip cleanly without consent or a valid token).
    let Some(provider) = live_anthropic_provider("full_stack_live", 1024) else {
        return;
    };

    // 2. Real subc-core supervising the REAL aft binary (AFT_BIN, else the sibling
    //    checkout's target/debug/aft).
    let aft_bin = std::env::var("AFT_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../aft/target/debug/aft")
        });
    if !aft_bin.exists() {
        eprintln!(
            "SKIP full_stack_live: real aft binary absent ({}). Set AFT_BIN.",
            aft_bin.display()
        );
        return;
    }
    let module = ModuleSpec {
        module_id: "aft".to_string(),
        program: aft_bin,
        args: vec![],
        env: vec![],
    };
    let Some(rig) = Rig::start_with_module("full-stack-live", module).await else {
        return; // subc-core or aft binary absent → skipped with a message
    };

    // The file the real aft reads: a per-run nonce at the project_root the run binds
    // to (aft sandboxes reads to that root, so the model asks for a RELATIVE name).
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let nonce = format!("FULLSTACK-LIVE-{}-{}", std::process::id(), unique);
    std::fs::write(rig.dir.join("status.txt"), &nonce).expect("write nonce file at project_root");

    let conn = rig.connect().await;
    // broca binds as "runner" — the real aft validates the harness against an
    // allowlist {opencode, pi, runner, mcp:<client>} (the fake-aft stub skips this).
    let plane = broca_subc::SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "read").await;

    let store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let read_tool = Tool {
        name: "read".into(),
        description: "Read a file's contents by path.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "filePath": { "type": "string", "description": "path to the file to read" } },
            "required": ["filePath"]
        }),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    };
    let req = RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("full-stack-live".into()),
        session: BindIdentity::new(&rig.dir, "runner", "fs-live-1"),
        model: ModelHandle::new("anthropic", MODEL),
        input: vec![broca_protocol::content::Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: broca_protocol::content::Role::User,
            content: vec![ContentBlock::Text {
                text: "Use the read tool to read the file \"status.txt\", then reply with its \
       exact contents verbatim and nothing else."
                    .into(),
            }],
        }],
        tools: vec![read_tool],
        policy_allowed: ["read".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 4 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };

    let (tx, mut rx) = mpsc::channel(2048);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(req, &provider, &plane, &store, &AllowAll, &wal, &tx)
        .await
        .expect("run ok");
    drop(tx);
    let events = collector.await.unwrap();

    // Auth failure → clean skip + signal a re-mint (provider-side, not a code failure).
    if let Some(err) = first_error(&events) {
        if err.class == ErrorClass::Auth {
            eprintln!(
                "SKIP full_stack_live: TOKEN EXPIRED/REJECTED — need a fresh mint. {}",
                err.message
            );
            return;
        }
        panic!("full-stack live error ({:?}): {}", err.class, err.message);
    }

    assert_eq!(
        summary.reason,
        RunFinishReason::Completed,
        "the full-stack live run completed"
    );
    assert!(
        tool_call_names(&events).iter().any(|n| n == "read"),
        "live model called read"
    );
    // The decisive end-to-end assertion: the nonce that REAL aft read from OUR file
    // round-tripped through the loop back into the live model's final answer.
    assert!(
        tool_result_texts(&events)
            .iter()
            .any(|t| t.contains(&nonce)),
        "real aft returned the nonce file content through the loop: {:?}",
        tool_result_texts(&events)
    );
    assert!(
        final_assistant_text(&events).contains(&nonce),
        "the nonce round-tripped to the live model's final answer (full spine closed): {:?}",
        final_assistant_text(&events)
    );
    eprintln!(
        "[full_stack_live] live model → real subc → real aft → nonce round-tripped, steps={}",
        summary.steps
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_model_drives_a_real_subc_tool_call() {
    // 1. Anthropic OAuth token. Skip cleanly without explicit consent or a valid token.
    let Some(provider) = live_anthropic_provider("live_e2e", 1536) else {
        return;
    };

    // 2. Real subc rig with a deterministic get_time tool.
    let Some(rig) = Rig::start(
        "live",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "get_time"),
            ("FAKE_AFT_TOOLCALL_RESULT", TOOL_RESULT),
        ],
    )
    .await
    else {
        return; // subc binaries absent
    };

    let conn = rig.connect().await;
    let plane = broca_subc::SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "get_time").await;

    // 3. Run the loop: live model + real tool plane.
    let store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let get_time = Tool {
        name: "get_time".into(),
        description: "Get the current time in a given IANA timezone.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "timezone": { "type": "string", "description": "IANA timezone, e.g. Europe/Madrid" } },
            "required": ["timezone"]
        }),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    };
    let req = RunRequest { session_class: None, retention: None, title: None, on_restart: None, keep_warm: None, send_id: None, request_identity_digest: None, project_id: None, principal: None, run_id: RunId("live-demo".into()),
    scope: None,
    session: BindIdentity::new(&rig.dir, "dogfood", "live-1"),
    model: ModelHandle::new("anthropic", MODEL),
    input: vec![broca_protocol::content::Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        role: broca_protocol::content::Role::User,
        content: vec![ContentBlock::Text {
            text: "What time is it in Madrid right now? Use the get_time tool to find out, then tell me.".into(),
        }],
    }],
    tools: vec![get_time],
    policy_allowed: ["get_time".to_string()].into_iter().collect(),
    tool_choice: ToolChoice::Auto,
    generation: GenerationConfig::default(),
    // Bound the live run so a misbehaving model can't loop.
    cache: None,
    context_management: None,
    stop_when: vec![StopCondition::StepCount { n: 4 }], submission_id: None, budget: None, context_limit: None, work_class: None, variant: None, service_tier: None, inherit_frozen_cache: false, };

    let (tx, mut rx) = mpsc::channel(2048);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });

    let t0 = Instant::now();
    let summary = run_loop(req, &provider, &plane, &store, &AllowAll, &wal, &tx)
        .await
        .expect("run ok");
    drop(tx);
    let elapsed = t0.elapsed();
    let events = collector.await.unwrap();

    // If the provider hit an auth failure, skip clean + signal a re-mint.
    if let Some(err) = first_error(&events) {
        if err.class == ErrorClass::Auth {
            eprintln!(
                "SKIP live_e2e: TOKEN EXPIRED/REJECTED — need a fresh mint. {}",
                err.message
            );
            return;
        }
        panic!("live provider error ({:?}): {}", err.class, err.message);
    }

    // Print a capture of the run (usage, steps, timing) for inspection.
    print_capture(&events, &summary.usage, summary.steps, elapsed);

    // 4. Assertions on the whole-stack path.
    assert_eq!(
        summary.reason,
        RunFinishReason::Completed,
        "the live run completed"
    );
    assert!(
        summary.steps >= 2,
        "tool turn + answer turn (got {} steps)",
        summary.steps
    );

    let tool_calls = tool_call_names(&events);
    assert!(
        tool_calls.iter().any(|n| n == "get_time"),
        "model called get_time: {tool_calls:?}"
    );

    let results = tool_result_texts(&events);
    assert!(
        results.iter().any(|t| t == TOOL_RESULT),
        "the deterministic fake-aft result flowed live-model->loop->subc->stub->loop: {results:?}"
    );

    // The model produced a FINAL assistant text turn after the tool result.
    let final_text = final_assistant_text(&events);
    assert!(
        !final_text.trim().is_empty(),
        "model produced a final text answer"
    );
    eprintln!("\n[live_e2e] FINAL MODEL ANSWER:\n{final_text}\n");
}

/// Exercises the interleaved-thinking ECHO-BACK risk: a reasoning-inducing
/// prompt that ALSO calls a tool, so step 2 must replay step 1's signed thinking
/// block. If the model thinks + calls the tool and the run COMPLETES, the
/// echo-back was accepted by Anthropic (a broken echo-back would 400 → the run
/// would end in Error, not Completed). Model nondeterminism is handled: if no
/// thinking or no tool call occurs this run, it's reported as not-exercised
/// (pass), not a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_interleaved_thinking_echo_back_survives_multiturn() {
    let Some(provider) = live_anthropic_provider("live_thinking", 2048) else {
        return;
    };
    let Some(rig) = Rig::start(
        "live-think",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "get_time"),
            ("FAKE_AFT_TOOLCALL_RESULT", TOOL_RESULT),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = broca_subc::SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "get_time").await;

    let store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let get_time = Tool {
        name: "get_time".into(),
        description: "Get the current time in a given IANA timezone.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "timezone": { "type": "string" } },
            "required": ["timezone"]
        }),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    };
    let req = RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("live-think".into()),
        session: BindIdentity::new(&rig.dir, "dogfood", "think-1"),
        model: ModelHandle::new("anthropic", MODEL),
        input: vec![broca_protocol::content::Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: broca_protocol::content::Role::User,
            content: vec![ContentBlock::Text {
                text: "Think carefully, step by step, about which IANA timezone identifier Madrid \
       uses and why, before doing anything. Then call get_time with that exact \
       timezone, and finally tell me the time."
                    .into(),
            }],
        }],
        tools: vec![get_time],
        policy_allowed: ["get_time".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 4 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };

    let (tx, mut rx) = mpsc::channel(2048);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(req, &provider, &plane, &store, &AllowAll, &wal, &tx)
        .await
        .expect("run ok");
    drop(tx);
    let events = collector.await.unwrap();

    let thought = first_step_produced_reasoning(&events);
    let called_tool = tool_call_names(&events).iter().any(|n| n == "get_time");
    eprintln!(
        "[live_thinking] step1 reasoning_block={thought} called_get_time={called_tool} steps={} reason={:?}",
        summary.steps, summary.reason
    );

    if let Some(err) = first_error(&events) {
        if err.class == ErrorClass::Auth {
            eprintln!("SKIP live_thinking: token rejected — {}", err.message);
            return;
        }
        // A non-auth error on a multi-turn-with-thinking run is the echo-back risk.
        let looks_like_thinking = err.message.to_lowercase().contains("thinking")
            || err.message.to_lowercase().contains("signature");
        panic!(
            "live run errored ({:?}){}: {}",
            err.class,
            if looks_like_thinking {
                " — LIKELY the interleaved-thinking echo-back; flag the driver"
            } else {
                ""
            },
            err.message
        );
    }

    if thought && called_tool {
        // The decisive assertion: a multi-turn carrying a replayed signed thinking
        // block was ACCEPTED by Anthropic.
        assert_eq!(
            summary.reason,
            RunFinishReason::Completed,
            "interleaved-thinking echo-back accepted on the multi-turn"
        );
        eprintln!("[live_thinking] ECHO-BACK PROVEN: multi-turn with replayed thinking completed.");
    } else {
        eprintln!(
            "[live_thinking] not exercised this run (thinking={thought}, tool={called_tool}) — \
             model nondeterminism; not a failure."
        );
    }
}

// ───────────────────────── helpers ─────────────────────────

/// True if the FIRST assistant message carried a thinking/redacted-thinking block
/// (i.e. the model actually reasoned on step 1).
fn first_step_produced_reasoning(events: &[OutputEvent]) -> bool {
    events
        .iter()
        .find_map(|e| match e {
            OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
                Some(message.content.iter().any(|b| {
                    matches!(
                        b,
                        ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. }
                    )
                }))
            }
            _ => None,
        })
        .unwrap_or(false)
}

fn first_error(events: &[OutputEvent]) -> Option<broca_protocol::ProviderError> {
    events.iter().find_map(|e| match e {
        OutputEvent::Control(ControlUnit::Error { error }) => Some(error.clone().into_inner()),
        _ => None,
    })
}

fn tool_call_names(events: &[OutputEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolCall { call }) => Some(call.tool_name.clone()),
            _ => None,
        })
        .collect()
}

fn tool_result_texts(events: &[OutputEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolResult { result }) => match &result.output {
                ToolOutput::Text { text } => Some(text.clone()),
                ToolOutput::Error { message } => Some(message.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// The text of the LAST assistant message (the final answer turn).
fn final_assistant_text(events: &[OutputEvent]) -> String {
    events
        .iter()
        .rev()
        .find_map(|e| match e {
            OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
                let t: String = message
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                Some(t)
            }
            _ => None,
        })
        .unwrap_or_default()
}

fn print_capture(events: &[OutputEvent], total: &Usage, steps: u64, elapsed: std::time::Duration) {
    eprintln!("\n════════ live_e2e CAPTURE ════════");
    eprintln!("wall: {:.2}s  steps: {steps}", elapsed.as_secs_f64());
    eprintln!(
        "TOTAL usage: input={:?} cached_in={:?} cache_write={:?} output={:?} reasoning={:?} | fill={:?}",
        total.input_tokens,
        total.cached_input_tokens,
        total.cache_write_tokens,
        total.output_tokens,
        total.reasoning_tokens,
        total.fill_tokens(),
    );
    let mut reasoning_streamed = false;
    let mut text_streamed = false;
    for e in events {
        match e {
            OutputEvent::Display(DisplayEvent::ReasoningDelta { .. }) => reasoning_streamed = true,
            OutputEvent::Display(DisplayEvent::TextDelta { .. }) => text_streamed = true,
            OutputEvent::Control(ControlUnit::StepFinished {
                step_id,
                usage,
                finish_reason,
            }) => {
                eprintln!(
                    "  step {} finished: reason={:?} usage(in={:?} cached_in={:?} cache_w={:?} out={:?})",
                    step_id.0,
                    finish_reason,
                    usage.input_tokens,
                    usage.cached_input_tokens,
                    usage.cache_write_tokens,
                    usage.output_tokens,
                );
            }
            OutputEvent::Control(ControlUnit::ToolCall { call }) => {
                eprintln!("  tool_call: {} input={}", call.tool_name, call.input);
            }
            _ => {}
        }
    }
    eprintln!("display streamed: reasoning={reasoning_streamed} text={text_streamed}");
    eprintln!(
        "cache fields populated: {} (multi-turn prompt-cache observable?)",
        total.cached_input_tokens.is_some_and(|n| n > 0)
            || total.cache_write_tokens.is_some_and(|n| n > 0)
    );
    eprintln!("═════════════════════════════════════════════════════════════\n");
}
