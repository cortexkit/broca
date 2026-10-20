//! M2 Phase A spike: prove the run_loop drives a MULTI-TURN tool call through the
//! whole real spine — an LLM (aimock) decides the call, the loop orchestrates, the
//! call executes through the REAL subc daemon and the REAL `aft` binary, and the
//! tool result flows back to drive the final turn.
//!
//!   prompt → OpenAiChat live stream @ aimock → model emits read{filePath:"<nonce>"}
//!   → real subc route → REAL aft reads the file the rig wrote → result flows back
//!   → model emits a final answer.
//!
//! This is the piece the M1 contract test can't cover: loop ORCHESTRATION (step
//! sequencing, tool-result feedback, stop conditions) against real AFT, with the
//! model in the driver's seat. It reuses the OpenAiChat family + the generic
//! transport pointed at aimock's base_url — the same path proven live against
//! DeepSeek, just base_url → aimock.
//!
//! Driven entirely by env so the harness side can tune it without a recompile:
//!   AIMOCK_BASE_URL  the aimock base (POSTs <base>/chat/completions). REQUIRED.
//!   AFT_BIN          path to the real `aft` binary to supervise as the module.
//!   SUBC_CORE_BIN    path to subc-core (else the sibling checkout's target/debug).
//!   AIMOCK_MODEL     wire model id to send (default "mock-model"; aimock may ignore).
//!   AIMOCK_BEARER    if set, sent as `Authorization: Bearer <v>`; else no auth header.
//!   AFT_TOOL         the AFT tool the model calls (default "read").
//!   AFT_FILE_ARG     the tool's path argument name (default "filePath").
//!   AFT_ARGS         whitespace-split args for the aft module (default none).
//!   AFT_MODULE_ID    the subc module id for aft (default "aft").
//!
//! Skips cleanly (never a silent pass) when AIMOCK_BASE_URL or the binaries are
//! absent.

use crate::common;

use std::time::Instant;

use broca_core::{run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{ContentBlock, Message, Role, ToolOutput},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, GenerationConfig, RunId,
};
use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
use broca_provider::wire::AuthHandle;
use broca_provider::LiveProvider;
use serde_json::json;
use tokio::sync::mpsc;

use common::{wait_for_aft, ModuleSpec, Rig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loop_drives_aimock_through_real_aft() {
    // ── env / gating ───────────────────────────────────────────────
    let Ok(base_url) = std::env::var("AIMOCK_BASE_URL") else {
        eprintln!("SKIP aimock_real_aft: AIMOCK_BASE_URL unset (the harness hosts aimock).");
        return;
    };
    let aft_bin = std::env::var("AFT_BIN").map(std::path::PathBuf::from).ok();
    let Some(aft_bin) = aft_bin.filter(|p| p.exists()) else {
        eprintln!("SKIP aimock_real_aft: AFT_BIN unset or missing (need the real aft binary).");
        return;
    };
    let model = std::env::var("AIMOCK_MODEL").unwrap_or_else(|_| "mock-model".to_string());
    let module_id = std::env::var("AFT_MODULE_ID").unwrap_or_else(|_| "aft".to_string());
    let tool_name = std::env::var("AFT_TOOL").unwrap_or_else(|_| "read".to_string());
    let file_arg = std::env::var("AFT_FILE_ARG").unwrap_or_else(|_| "filePath".to_string());
    let aft_args: Vec<String> = std::env::var("AFT_ARGS")
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let auth = match std::env::var("AIMOCK_BEARER") {
        Ok(t) if !t.is_empty() => AuthHandle::Bearer(t),
        _ => AuthHandle::None,
    };

    // ── rig: real subc-core + real aft as the module ───────────────
    let module = ModuleSpec {
        module_id: module_id.clone(),
        program: aft_bin,
        args: aft_args,
        env: vec![],
    };
    let Some(rig) = Rig::start_with_module("aimock-aft", module).await else {
        return; // subc-core or aft binary absent → skipped with a message
    };

    // The file AFT will read. It lives at the project_root (the rig dir), which is
    // what the run binds to — AFT sandboxes reads to that root, so the model asks
    // for a RELATIVE filename and AFT resolves it against the root. A per-run nonce
    // proves the content came back THROUGH real AFT, not from anywhere else.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let nonce = format!("M2-SPIKE-{}-{}", std::process::id(), unique);
    let rel_name = "hello.txt";
    std::fs::write(rig.dir.join(rel_name), &nonce).expect("write nonce file at project_root");

    let conn = rig.connect().await;
    let plane = broca_subc::SubcToolPlane::new(conn, &module_id);
    wait_for_aft(&plane, &tool_name).await;

    // ── the run: model (aimock) decides, loop orchestrates ─────────
    let store = FileLeaseStore::new(rig.dir.join("leases"));
    let wal = FileWalStore::new(rig.dir.join("wal"));

    let read_tool = Tool {
        name: tool_name.clone(),
        description: "Read a file's contents by path.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { file_arg.clone(): { "type": "string", "description": "Path to the file to read." } },
            "required": [file_arg.clone()]
        }),
        module: None,
        // Batch-scheduling class only; read is side-effect-free, so AFT won't gate it
        // and AllowAll passes it. (This is NOT AFT's execution/safety class.)
        execution_mode: ExecutionMode::Parallel,
    };

    let mut frozen = openai_chat_frozen_config(&model);
    frozen.base_url = Some(base_url.clone());
    let provider = LiveProvider::new(OpenAiChat, frozen, auth);

    let req = RunRequest {
        directives: None,
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
        run_id: RunId("m2-spike".into()),
        // The real AFT validates `harness` against an allowlist
        // {opencode, pi, runner, mcp:<client>} — "runner" is THIS harness's
        // designated identity (the fake-aft-stub skipped this check, so the
        // in-process dogfood tests could use an arbitrary string; the real binary
        // can't).
        session: BindIdentity::new(&rig.dir, "runner", "spike-1"),
        model: ModelHandle::new(&module_id, &model),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: format!("Read the file {rel_name} and report its contents."),
            }],
        }],
        tools: vec![read_tool],
        policy_allowed: [tool_name.clone()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        // Bound the run so a misbehaving script can't loop forever.
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
        structured_output: None,
    };

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
    let events = collector.await.unwrap();
    eprintln!(
        "[aimock_real_aft] steps={} reason={:?} wall={:.2}s",
        summary.steps,
        summary.reason,
        t0.elapsed().as_secs_f64()
    );

    // A cold AFT (fresh XDG_DATA_HOME → index + semantic-model load) can make subc's
    // route.bind/configure exceed its internal timeout on the FIRST bind, which
    // happens inside this run on the model's first tool call. That's a subc/AFT
    // cold-start property, NOT a loop bug — label it clearly so it isn't mis-diagnosed.
    let looks_like_cold_bind = |s: &str| {
        let s = s.to_lowercase();
        s.contains("route.bind")
            || s.contains("module_timeout")
            || s.contains("configure")
            || (s.contains("within") && s.contains('s') && s.contains("answer"))
    };
    if let Some(err) = first_error(&events) {
        let hint = if looks_like_cold_bind(&err.message) {
            " — looks like a COLD-AFT route.bind/configure timeout (subc-side cold start, not the loop). Flag the harness."
        } else {
            ""
        };
        panic!(
            "provider/loop error ({:?}){hint}: {}",
            err.class, err.message
        );
    }
    // Surface a not-sent / cold-bind reason that rode through as a tool result too.
    for (is_err, text) in tool_results(&events) {
        if is_err && looks_like_cold_bind(&text) {
            panic!(
                "tool dispatch failed on a COLD-AFT route.bind/configure timeout (subc-side, \
                 not the loop): {text}"
            );
        }
    }

    // ── assertions: the loop drove the turns through REAL AFT ──────
    // 1. The model called the read tool.
    let called: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolCall { call }) => Some(call.tool_name.clone()),
            _ => None,
        })
        .collect();
    assert!(
        called.iter().any(|n| n == &tool_name),
        "model called {tool_name}: {called:?}"
    );

    // 2. The tool result carries the nonce — proving the content came back through
    //    the REAL aft read, resolved against the project_root, fed into the loop.
    let results = tool_results(&events);
    assert!(
        results
            .iter()
            .any(|(is_err, text)| !is_err && text.contains(&nonce)),
        "real aft returned the nonce file content through the loop; got results: {results:?}"
    );

    // 3. The loop completed both turns (tool turn + answer turn), not a timeout.
    assert_eq!(
        summary.reason,
        RunFinishReason::Completed,
        "the run completed cleanly"
    );
    assert!(
        summary.steps >= 2,
        "tool turn + answer turn (got {} steps)",
        summary.steps
    );

    // 4. THE decisive full-round-trip assertion: the model's FINAL text echoes the
    //    nonce. aimock's turn-1 echoes back the tool-result content, so the nonce
    //    appearing in the final answer proves the whole loop closed:
    //    model → read → real AFT read OUR file → content → back into the model → final answer.
    let final_text = final_assistant_text(&events);
    assert!(
        !final_text.trim().is_empty(),
        "model produced a final answer turn"
    );
    assert!(
        final_text.contains(&nonce),
        "final answer echoes the nonce (full round-trip model→AFT→model); nonce={nonce:?} final={final_text:?}"
    );
    eprintln!("[aimock_real_aft] FINAL ANSWER: {final_text}");
}

/// All tool results as (is_error, text) pairs, in order.
fn tool_results(events: &[OutputEvent]) -> Vec<(bool, String)> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolResult { result }) => Some((
                result.is_error,
                match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Error { message } => message.clone(),
                    _ => String::new(),
                },
            )),
            _ => None,
        })
        .collect()
}

fn first_error(events: &[OutputEvent]) -> Option<broca_protocol::ProviderError> {
    events.iter().find_map(|e| match e {
        OutputEvent::Control(ControlUnit::Error { error }) => Some(error.clone().into_inner()),
        _ => None,
    })
}

fn final_assistant_text(events: &[OutputEvent]) -> String {
    events
        .iter()
        .rev()
        .find_map(|e| match e {
            OutputEvent::Control(ControlUnit::AssistantMessage { message }) => Some(
                message
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}
