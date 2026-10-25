//! End-to-end fault classification through the real subc spine. These tests prove
//! that runner → `SubcToolPlane` → subc daemon → fake AFT maps each real wire
//! outcome into the loop behavior that protects tool side effects.

use crate::common;

use std::time::{SystemTime, UNIX_EPOCH};

use broca_core::{
    run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest, RunSummary,
    StubProvider,
};
// ToolManifest is used only by the unix-only route-invalidation spine test below.
#[cfg(unix)]
use broca_core::ToolManifest;
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, Tool, ToolChoice},
    BindIdentity, DisplayEvent, FinishReason, GenerationConfig, MessageId, RunId, ToolCallId,
    Usage,
};
use broca_subc::SubcToolPlane;
use common::{wait_for_aft, Rig};
use serde_json::{json, Value};
use tokio::sync::mpsc;

// ───────────────────────── model script ─────────────────────────

fn tool_step(calls: &[(&str, &str, serde_json::Value)]) -> Vec<NormalizedEvent> {
    let content = calls
        .iter()
        .map(|(id, name, input)| {
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId((*id).into()),
                tool_name: (*name).into(),
                input: input.clone(),
            })
        })
        .collect();
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_step(text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Display(DisplayEvent::TextDelta {
            mid: None,
            part: Some(0),
            message_id: MessageId("m2".into()),
            tool_call_id: None,
            delta: text.into(),
        }),
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m2".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn request(session: &str, project_root: &std::path::Path, tools: Vec<Tool>) -> RunRequest {
    let policy_allowed = tools.iter().map(|t| t.name.clone()).collect();
    RunRequest {
        input_written_at_ms: None,
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
        run_id: RunId(format!("tool-fault-spine-{session}")),
        session: BindIdentity::new(project_root, "tool-fault-spine", session),
        model: ModelHandle::new("aft-stub", "stub-model"),
        input: vec![],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: "fault-classification probe".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    }
}

async fn drive_run(
    rig: &Rig,
    plane: &SubcToolPlane,
    session: &str,
    tool_name: &str,
    provider: &StubProvider,
) -> (RunSummary, Vec<OutputEvent>) {
    let store = FileLeaseStore::new(rig.dir.join(format!("leases-{session}")));
    let wal = FileWalStore::new(rig.dir.join(format!("wal-{session}")));
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });

    let summary = run_loop(
        request(session, &rig.dir, vec![tool(tool_name)]),
        provider,
        plane,
        &store,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("run ok");
    drop(tx);
    let events = collector.await.expect("collector task joined");
    (summary, events)
}

fn collect_results(events: &[OutputEvent]) -> Vec<ToolResultText> {
    events
        .iter()
        .filter_map(|e| match e {
            OutputEvent::Control(ControlUnit::ToolResult { result }) => Some(ToolResultText {
                is_error: result.is_error,
                text: match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Error { message } => message.clone(),
                    _ => String::new(),
                },
            }),
            _ => None,
        })
        .collect()
}

fn collect_display_text(events: &[OutputEvent]) -> String {
    events.iter().fold(String::new(), |mut acc, event| {
        if let OutputEvent::Display(DisplayEvent::TextDelta { delta, .. }) = event {
            acc.push_str(delta);
        }
        acc
    })
}

#[derive(Debug)]
struct ToolResultText {
    is_error: bool,
    text: String,
}

fn event_log_path(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    broca_test_support::scratch_root().join(format!(
        "broca-tool-fault-spine-{tag}-{}-{nanos}.jsonl",
        std::process::id()
    ))
}

fn event_log_contains_tool_request(log: &str, tool_name: &str) -> bool {
    log.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("request_received")
                && event.pointer("/body_json/name").and_then(Value::as_str) == Some(tool_name)
        })
}

fn event_log_terminal_count(log: &str) -> usize {
    log.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event.get("kind").and_then(Value::as_str) == Some("terminal"))
        .count()
}

// ───────────────────────── fault classification tests ─────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_level_error_is_completed_model_visible_result() {
    let Some(rig) = Rig::start(
        "tool-level-error",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "echo"),
            ("FAKE_AFT_TOOLCALL_ERROR", "1"),
            ("FAKE_AFT_TOOLCALL_RESULT", "TOOL-LEVEL-ERROR"),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_tool_error", "echo", json!({"value": "boom"}))]),
        text_step("model continued after tool-level error"),
    ]);
    let (summary, events) = drive_run(&rig, &plane, "tool-level-error", "echo", &provider).await;

    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(summary.steps, 2, "the error result was fed to the model");
    let results = collect_results(&events);
    assert_eq!(results.len(), 1, "exactly one tool result");
    assert!(results[0].is_error, "tool-level error stayed is_error");
    assert_eq!(
        results[0].text, "TOOL-LEVEL-ERROR",
        "the fake AFT CallToolResult with isError=true surfaced as the tool result"
    );
    assert_eq!(
        collect_display_text(&events),
        "model continued after tool-level error",
        "Completed(is_error) lets the model receive the failure and continue"
    );
    eprintln!(
        "tool_fault_spine tool_level_error: observed RunFinishReason::{:?}, steps={}, result={:?}",
        summary.reason, summary.steps, results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subc_error_frame_is_completed_model_visible_rejection() {
    let Some(rig) = Rig::start(
        "subc-error-frame",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "echo"),
            ("FAKE_AFT_TOOLCALL_SUBC_ERROR", "1"),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_subc_error", "echo", json!({"value": "reject"}))]),
        text_step("model continued after subc error frame"),
    ]);
    let (summary, events) = drive_run(&rig, &plane, "subc-error-frame", "echo", &provider).await;

    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        summary.steps, 2,
        "a typed subc Error frame is definite and model-visible, not an interrupt"
    );
    let results = collect_results(&events);
    assert_eq!(results.len(), 1, "exactly one tool result");
    assert!(results[0].is_error, "subc rejection surfaced as is_error");
    assert!(
        results[0]
            .text
            .starts_with("tool/route rejected: target_unavailable"),
        "real mapping should be Completed(ToolResult::Error), got {:?}",
        results[0]
    );
    assert!(
        results[0]
            .text
            .contains("fake AFT injected subc-level tool-call failure"),
        "the fake AFT error body should survive the real subc wire: {:?}",
        results[0]
    );
    assert_eq!(
        collect_display_text(&events),
        "model continued after subc error frame",
        "the rejection was fed back as a normal tool result the model could react to"
    );
    eprintln!(
        "tool_fault_spine subc_error_frame: observed RunFinishReason::{:?}, steps={}, result={:?}",
        summary.reason, summary.steps, results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_send_interrupts_without_feeding_model() {
    let event_log = event_log_path("crash-after-send");
    let event_log_value = event_log.to_string_lossy().into_owned();
    let Some(rig) = Rig::start(
        "crash-after-send",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "echo"),
            ("FAKE_AFT_EVENTS_PATH", event_log_value.as_str()),
            // The request is accepted, then the response is delayed long enough for
            // the supervised provider process to die while the call is in flight.
            ("FAKE_AFT_TOOLCALL_DELAY_MS", "30000"),
            ("FAKE_AFT_CRASH_AFTER_MS", "2500"),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_crash", "echo", json!({"value": "maybe-mutated"}))]),
        // A second step is scripted so the assertion can prove the loop did not feed
        // an ambiguous outcome back to the model. Retrying could duplicate a side
        // effect if the provider crashed after applying the tool work.
        text_step("should not be reached after ambiguous dispatch"),
    ]);
    let (summary, events) = drive_run(&rig, &plane, "crash-after-send", "echo", &provider).await;

    let log = std::fs::read_to_string(&event_log).unwrap_or_default();
    let _ = std::fs::remove_file(&event_log);
    assert!(
        event_log_contains_tool_request(&log, "echo"),
        "the fake AFT log should prove the request left the runner and reached the provider; log={log}"
    );
    assert_eq!(
        event_log_terminal_count(&log),
        0,
        "the provider crashed before writing a Response/Error terminal; log={log}"
    );

    assert_eq!(
        summary.reason,
        RunFinishReason::Interrupted,
        "sent-but-no-terminal dispatch must interrupt for review"
    );
    assert_eq!(
        summary.steps, 1,
        "the second model step did not run, so the unknown outcome was not fed back"
    );
    let results = collect_results(&events);
    assert_eq!(results.len(), 1, "the loop emits one provider-valid result");
    assert!(
        results[0].is_error,
        "unknown outcome is represented as is_error"
    );
    assert!(
        results[0]
            .text
            .contains("tool outcome unknown (interrupted for review)"),
        "ambiguous dispatch should surface the synthetic unknown result, got {:?}",
        results[0]
    );
    assert_eq!(
        collect_display_text(&events),
        "",
        "the scripted second model response must not be emitted after an interrupt"
    );
    eprintln!(
        "tool_fault_spine crash_after_send: request_reached_stub=true, terminal_events=0, observed RunFinishReason::{:?}, steps={}, result={:?}",
        summary.reason, summary.steps, results[0]
    );
}

/// `close_session_routes` while `warm_bind` is still retrying must cancel the warm task and
/// leave no cached route for that session (prevents a leaked daemon channel after session end).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_session_during_warm_bind_drops_cached_route() {
    let Some(rig) = Rig::start(
        "warm-close-race",
        &[("FAKE_AFT_MODULE_ID", "aft"), ("FAKE_AFT_TOOLS", "echo")],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    // A missing module keeps `warm_bind` in its retry/backoff loop so `close_session_routes`
    // can cancel mid-flight (a fast successful bind would finish before we close).
    let plane = std::sync::Arc::new(SubcToolPlane::new(conn, "module-that-does-not-exist"));
    let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", "warm-close");
    let plane_bg = std::sync::Arc::clone(&plane);
    let bind_bg = bind.clone();
    // CAPTURE BEFORE SPAWN, which is what production does (main.rs takes the token, then
    // spawns). `warm_bind` captures on its FIRST POLL instead, so a close landing before that
    // poll mints a fresh uncancelled token and the warm runs its full 30s retry window -- the
    // test still passed, on the timeout rather than on cancellation, and cost ~30s per gate.
    // Driving the same shape production drives is what makes this a cancellation test.
    let lifecycle = plane.capture_session_lifecycle_token(&bind);
    let warm = tokio::spawn(async move {
        plane_bg
            .warm_bind_for_lifecycle(
                &bind_bg,
                None,
                std::time::Duration::from_secs(30),
                lifecycle,
            )
            .await
    });

    plane.close_session_routes(&bind).await;
    // Bound the wait well under the 30s warm window: without it a regression that reverts to
    // first-poll capture passes again, slowly, which is how this went unnoticed.
    let warm_out = tokio::time::timeout(std::time::Duration::from_secs(10), warm)
        .await
        .expect("close must cancel the warm promptly, not let it run out the retry window")
        .expect("warm_bind task join");
    assert!(
        warm_out.is_err(),
        "warm_bind should stop after session close, got {warm_out:?}"
    );
    assert_eq!(
        plane.cached_route_channel(&bind),
        None,
        "no route may remain cached for a closed session"
    );
}

/// Same `BindIdentity` after idle-close must reopen: cancel+remove mints a fresh lifecycle
/// token (coordinator close/reopen ship-gate; must not use a permanent tombstone).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_session_then_reopen_mints_fresh_lifecycle_token() {
    let Some(rig) = Rig::start(
        "warm-reopen",
        &[("FAKE_AFT_MODULE_ID", "aft"), ("FAKE_AFT_TOOLS", "echo")],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = std::sync::Arc::new(SubcToolPlane::new(conn, "module-that-does-not-exist"));
    let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", "reopen");
    let plane_bg = std::sync::Arc::clone(&plane);
    let bind_bg = bind.clone();
    // Capture before spawn, as production does; see the note in the sibling test above.
    let lifecycle = plane.capture_session_lifecycle_token(&bind);
    let warm = tokio::spawn(async move {
        plane_bg
            .warm_bind_for_lifecycle(
                &bind_bg,
                None,
                std::time::Duration::from_secs(30),
                lifecycle,
            )
            .await
    });

    plane.close_session_routes(&bind).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), warm)
        .await
        .expect("close must cancel the warm promptly, not let it run out the retry window");

    assert!(
        plane.test_mint_lifecycle_token_uncancelled(&bind),
        "after close, the same session key must mint a fresh uncancelled token for reopen"
    );
}

/// ROUTE INVALIDATION: a route learned-dead (here, invalidated explicitly to simulate what
/// a route-close handler would do) is DROPPED from the cache, so the next bind re-opens a
/// FRESH channel — the plane never keeps dispatching to a stale dead channel. The test:
/// open a route, capture its channel, invalidate, re-open, assert a different (fresh)
/// channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalidated_route_is_dropped_and_reopens_fresh() {
    let Some(rig) = Rig::start(
        "route-invalidation",
        &[("FAKE_AFT_MODULE_ID", "aft"), ("FAKE_AFT_TOOLS", "echo")],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", "inval");

    // Open the route; a channel is now cached.
    plane
        .warm_bind(&bind, std::time::Duration::from_secs(5))
        .await
        .expect("first route open");
    let first = plane
        .cached_route_channel(&bind)
        .expect("a route channel is cached after warm_bind");

    // Invalidate (what a GOODBYE / transport-error path does): the cache is cleared.
    plane.invalidate_route(&bind);
    assert_eq!(
        plane.cached_route_channel(&bind),
        None,
        "an invalidated route is dropped from the cache, not retained stale"
    );

    // The next bind re-opens — a fresh channel, proving the plane re-opens rather than
    // reusing the dead one. (subc assigns a new route_channel per route.open.)
    plane
        .warm_bind(&bind, std::time::Duration::from_secs(5))
        .await
        .expect("re-open after invalidation");
    let second = plane
        .cached_route_channel(&bind)
        .expect("a fresh route channel is cached after re-open");
    assert_ne!(
        first, second,
        "re-opening an invalidated route yields a FRESH channel, not the stale one"
    );
}

/// A tool-module restart between dispatches costs ONE transparent retry, not a failed
/// call: the first dispatch caches a route; the module is then killed (the supervisor
/// respawns it), which frees the daemon-side channel — a dispatch on the stale channel is
/// rejected `unknown_channel`. The plane must evict the stale route, re-open against the
/// respawned module, and complete the call, so a routine module restart never surfaces to
/// the model. Unix-only: the kill helper uses `pgrep`/`kill` and is a no-op on Windows
/// (the retry logic under test is platform-independent).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn module_restart_between_dispatches_is_one_transparent_retry() {
    use broca_core::{BatchCtx, DispatchResult, ToolPlane};
    use broca_protocol::BatchId;

    let Some(rig) = Rig::start(
        "restart-retry",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "echo"),
            ("FAKE_AFT_TOOLCALL_RESULT", "SURVIVED-RESTART"),
        ],
    )
    .await
    else {
        return;
    };

    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;

    let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", "restart-retry");
    plane
        .warm_bind(&bind, std::time::Duration::from_secs(10))
        .await
        .expect("first route open");
    let stale = plane
        .cached_route_channel(&bind)
        .expect("a route channel is cached");

    // Kill the supervised module; the supervisor respawns it. The cached channel is now
    // freed daemon-side. The catalog can keep listing the old registration while the
    // respawn is still in flight, so waiting on the catalog alone races a
    // `target_unavailable` re-open — wait until the respawned module is genuinely
    // BINDABLE (a probe route.open on a throwaway session succeeds).
    let killed = rig.crash_supervised_modules().await;
    assert!(killed >= 1, "the supervised fake-aft was killed");
    let probe_conn = rig.connect().await;
    let probe_bind = BindIdentity::new(&rig.dir, "tool-fault-spine", "restart-probe");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match probe_conn.route_open("aft", &probe_bind).await {
            Ok(ch) => {
                let _ = probe_conn.route_close(&ch).await;
                break;
            }
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("respawned module never became bindable: {e}"),
        }
    }
    assert_eq!(
        plane.cached_route_channel(&bind),
        Some(stale),
        "the plane still holds the stale route (nothing invalidated it yet)"
    );

    // Dispatch on the stale route: the daemon rejects unknown_channel; the plane must
    // evict, re-open, and complete against the respawned module.
    let ctx = BatchCtx {
        media_capture: None,
        batch_id: BatchId("b-restart".into()),
        bind: bind.clone(),
        run_id: broca_protocol::RunId("r1".into()),
        scope: None,
    };
    let call = ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId("toolu_restart_1".into()),
        tool_name: "echo".into(),
        input: json!({"value": "hello-after-restart"}),
    };
    let manifest = ToolManifest::from_tools(vec![tool("echo")]);
    plane
        .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
        .await
        .expect("batch_started against the respawned module");
    let outcome = plane.dispatch(&ctx, &call).await;
    match outcome {
        DispatchResult::Completed(result) => {
            assert!(!result.is_error, "the retried call completed clean");
            assert!(
                matches!(&result.output, broca_protocol::content::ToolOutput::Text { text } if text == "SURVIVED-RESTART"),
                "the respawned module served the retried call: {result:?}"
            );
        }
        other => panic!("expected Completed via the transparent retry, got {other:?}"),
    }
    let fresh = plane
        .cached_route_channel(&bind)
        .expect("a fresh route is cached after the retry");
    assert_ne!(fresh, stale, "the retry re-opened a fresh channel");
}

// Keep a direct ref to subc_protocol so the manifest match in common resolves.
use subc_protocol as _;

// Register through the module SDK so refusals cross the real daemon, rather than
// bypassing its route binding and tool-call forwarding with a mock connection.
struct RefusalStub {
    codes: std::sync::Mutex<std::collections::VecDeque<String>>,
    binds: std::sync::atomic::AtomicUsize,
    calls: std::sync::Mutex<Vec<Value>>,
}

struct RefusalHandler(std::sync::Arc<RefusalStub>);

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for RefusalHandler {
    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        self.0
            .binds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        self.0
            .calls
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).expect("tool request JSON"));
        if let Some(code) = self.0.codes.lock().unwrap().pop_front() {
            subc_client_rs::HandlerOutcome::Error {
                code,
                message: "scripted route refusal".into(),
            }
        } else {
            subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(
                    &json!({"content": [{"type": "text", "text": "FRESH-ROUTE-SUCCESS"}]}),
                )
                .unwrap(),
            )
        }
    }
}

async fn check_route_refusal(code: &str, refusals: usize, retry: bool, unknown: bool) {
    use std::sync::{atomic::Ordering, Arc};
    use subc_protocol::manifest::{
        Concurrency, ExecutionMode, IdentityScope, ModuleManifest, ProviderRole, Tool,
    };
    let tag = format!("refusal-{code}-{refusals}");
    let Some(rig) = Rig::start_with_modules(&tag, vec![]).await else {
        return;
    };
    let conn = rig.connect().await;
    let stub = Arc::new(RefusalStub {
        codes: std::sync::Mutex::new(std::iter::repeat_n(code.to_owned(), refusals).collect()),
        binds: std::sync::atomic::AtomicUsize::new(0),
        calls: std::sync::Mutex::new(vec![]),
    });
    let manifest = ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "echo".into(),
                description: None,
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build();
    let (_, serve) = subc_client_rs::serve_with_handle(
        &rig.connection_file,
        manifest,
        RefusalHandler(Arc::clone(&stub)),
    )
    .await
    .expect("register refusal stub");
    let serving = tokio::spawn(serve);
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, "echo").await;
    let bind = BindIdentity::new(&rig.dir, "tool-fault-spine", &tag);
    plane
        .warm_bind(&bind, std::time::Duration::from_secs(10))
        .await
        .expect("initial bind");
    assert_eq!(stub.binds.load(Ordering::SeqCst), 1);
    let initial = plane
        .cached_route_channel(&bind)
        .expect("cached initial route");
    let provider = StubProvider::new(vec![
        tool_step(&[("toolu_refusal", "echo", json!({"value": "unchanged-input"}))]),
        text_step("model resumed"),
    ]);
    let (summary, events) = drive_run(&rig, &plane, &tag, "echo", &provider).await;
    let results = collect_results(&events);
    assert_eq!(results.len(), 1);
    let calls = stub.calls.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        if retry { 2 } else { 1 },
        "at most one resend for {code}"
    );
    assert_eq!(
        stub.binds.load(Ordering::SeqCst),
        if retry { 2 } else { 1 },
        "fresh bind for {code}"
    );
    if retry {
        assert_eq!(calls[0], calls[1], "resend preserves the entire call");
    }
    if unknown {
        assert_eq!(summary.reason, RunFinishReason::Interrupted);
        assert_eq!(summary.steps, 1);
        assert!(results[0].is_error);
        assert!(results[0]
            .text
            .contains("tool outcome unknown (interrupted for review)"));
        assert_eq!(collect_display_text(&events), "");
        use broca_core::WalStore;
        let records = FileWalStore::new(rig.dir.join(format!("wal-{tag}")))
            .read_records(&bind)
            .expect("read run WAL");
        assert!(records.iter().any(|framed| matches!(&framed.record,
            broca_wal::WalRecord::ToolResult { result, outcome: broca_wal::DispatchOutcome::Unknown, .. }
                if result.tool_call_id.0 == "toolu_refusal")), "unknown outcome must be durable");
        assert_eq!(
            plane.cached_route_channel(&bind),
            None,
            "ambiguous reclaimed route is evicted"
        );
        // Use a new batch on the same session: replaying the interrupted WAL would
        // require review of the prior unknown side effect before another run can proceed.
        use broca_core::{BatchCtx, DispatchResult, ToolManifest, ToolPlane};
        let next_ctx = BatchCtx {
            media_capture: None,
            batch_id: broca_protocol::BatchId("next-batch".into()),
            bind: bind.clone(),
            run_id: RunId("next-run".into()),
            scope: None,
        };
        let next_call = ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("toolu_next".into()),
            tool_name: "echo".into(),
            input: json!({}),
        };
        plane
            .batch_started(
                &next_ctx,
                std::slice::from_ref(&next_call),
                &ToolManifest::from_tools(vec![tool("echo")]),
            )
            .await
            .expect("next batch");
        assert!(
            matches!(plane.dispatch(&next_ctx, &next_call).await, DispatchResult::Completed(result) if !result.is_error)
        );
        plane
            .batch_finished(&next_ctx)
            .await
            .expect("finish next batch");
        assert_eq!(stub.calls.lock().unwrap().len(), 2);
        assert_eq!(
            stub.binds.load(Ordering::SeqCst),
            2,
            "next send opens fresh binding"
        );
        assert_ne!(plane.cached_route_channel(&bind), Some(initial));
    } else if retry && refusals == 1 {
        assert!(!results[0].is_error);
        assert_eq!(results[0].text, "FRESH-ROUTE-SUCCESS");
        assert_ne!(plane.cached_route_channel(&bind), Some(initial));
    } else {
        assert!(results[0].is_error);
        assert!(results[0].text.contains(code), "refusal is returned intact");
        assert_ne!(summary.reason, RunFinishReason::Interrupted);
        if retry {
            assert_eq!(
                plane.cached_route_channel(&bind),
                None,
                "replacement refusal evicts route too"
            );
        } else {
            assert_eq!(
                plane.cached_route_channel(&bind),
                Some(initial),
                "unfamiliar refusal keeps route"
            );
        }
    }
    serving.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_not_bound_reopens_and_resends_once() {
    check_route_refusal("route_not_bound", 1, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_reclaimed_reopens_and_resends_once() {
    check_route_refusal("route_reclaimed", 1, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_predispatch_refusal_returns_second_error_without_retrying_again() {
    check_route_refusal("route_not_bound", 2, true, false).await;
    check_route_refusal("route_reclaimed", 2, true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_unknown_outcome_interrupts_without_resend_and_next_send_rebinds() {
    check_route_refusal("outcome_unknown_root_reclaimed", 1, false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unfamiliar_route_refusal_remains_plain_error_and_keeps_route() {
    check_route_refusal("unfamiliar_route_refusal", 1, false, false).await;
}
