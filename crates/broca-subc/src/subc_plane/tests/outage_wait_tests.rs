//! A tool call's route open waits out a short provider-module restart, and nothing about that
//! wait may outlive the run: cancel, shutdown (stop) and the run's deadline end it at once, an
//! open attempt still in flight included, and a call they end is never sent.
//!
//! The run-level tests drive the real Core loop (`run_loop_with_control`), so every call goes
//! through Core's batch adapter exactly as in production, over the scripted connection with a
//! real module gate and a paused clock. A route that "opens closed" makes the first send find
//! it gone before anything is written, which puts the call into the tool-dispatch re-open wait.

use super::*;
use broca_core::{
    run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunControl,
    RunRequest, RunSummary, StubProvider,
};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, ToolChoice},
    FinishReason, GenerationConfig, MessageId, RunId, Usage,
};
use tokio::sync::mpsc;

const UNAVAILABLE: &str = "target_unavailable — module_id 'aft' is unavailable";

fn tool_step() -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(call("read"))],
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

fn request(tag: &str, root: &std::path::Path) -> RunRequest {
    let tool = Tool {
        name: "read".into(),
        description: "read".into(),
        input_schema: serde_json::json!({"type": "object"}),
        module: Some("aft".into()),
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    };
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
        run_id: RunId(format!("outage-{tag}")),
        session: BindIdentity::new(root, "outage-wait", tag),
        model: ModelHandle::new("stub", "stub-model"),
        input: vec![],
        policy_allowed: std::iter::once(tool.name.clone()).collect(),
        tools: vec![tool],
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

/// A connection whose catalog always lists `aft` with `read`, with a real module gate.
fn outage_connection(
    route_opens: Vec<RouteOpenScript>,
    tool_replies: Vec<Result<serde_json::Value, ConnError>>,
    probe_jitter: Duration,
) -> Arc<ScriptedToolPlaneConnection> {
    let conn = ScriptedToolPlaneConnection::new(
        (0..16)
            .map(|_| vec![catalog_entry("aft", &["read"])])
            .collect(),
        tool_replies,
    );
    conn.script_route_open_sequence("aft", route_opens);
    conn.install_real_gate_with_jitter("aft", move |_| probe_jitter);
    conn
}

/// The batch's first route opens, but the daemon has already closed it.
fn opens_closed() -> RouteOpenScript {
    RouteOpenScript {
        closed: true,
        ..RouteOpenScript::default()
    }
}

fn refused_after(delay: Duration) -> RouteOpenScript {
    RouteOpenScript {
        delay,
        ..RouteOpenScript::rejected(UNAVAILABLE)
    }
}

fn reloading() -> Result<serde_json::Value, ConnError> {
    Err(ConnError::Rejected(crate::connection::decode_error_body(
        &serde_json::to_vec(&subc_protocol::ErrorBody::new(
            "module_reloading",
            "module 'aft' is draining for a restart",
        ))
        .unwrap(),
    )))
}

struct CoreRun {
    summary: RunSummary,
    results: Vec<ToolResult>,
    elapsed: Duration,
}

/// Run one model turn that calls `read`, then answers, through the real Core loop.
async fn run_through_core(
    tag: &str,
    conn: Arc<ScriptedToolPlaneConnection>,
    control: RunControl,
) -> CoreRun {
    let root = broca_test_support::scratch_root()
        .join(format!("outage-wait-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let plane = SubcToolPlane::with_connection(conn, "aft");
    let provider = StubProvider::new(vec![tool_step(), text_step("done")]);
    let leases = FileLeaseStore::new(root.join("leases"));
    let wal = FileWalStore::new(root.join("wal"));
    let (tx, mut rx) = mpsc::channel(256);
    let collector = tokio::spawn(async move {
        let mut results = Vec::new();
        while let Some(event) = rx.recv().await {
            if let OutputEvent::Control(ControlUnit::ToolResult { result }) = event {
                results.push(result);
            }
        }
        results
    });
    let started = Instant::now();
    let summary = run_loop_with_control(
        request(tag, &root),
        &provider,
        &plane,
        &leases,
        &AllowAll,
        &wal,
        None,
        control,
        &tx,
    )
    .await
    .expect("the run is admitted");
    let elapsed = started.elapsed();
    drop(tx);
    let results = collector.await.unwrap();
    let _ = std::fs::remove_dir_all(&root);
    CoreRun {
        summary,
        results,
        elapsed,
    }
}

/// Wait until the call is in its re-open wait: the batch's open and at least two refused
/// re-open attempts.
async fn wait_for_reopen_attempts(conn: &ScriptedToolPlaneConnection) {
    while conn.route_open_calls.load(Ordering::SeqCst) < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Run cancel during the re-open wait ends the call at once through Core's adapter: the run
/// ends Cancelled without probing on through the 90 s patience, and nothing was sent.
#[tokio::test(start_paused = true)]
async fn run_cancel_during_the_reopen_wait_ends_it_through_core() {
    let conn = outage_connection(
        vec![opens_closed(), RouteOpenScript::rejected(UNAVAILABLE)],
        vec![successful_tool_reply("must not be sent")],
        Duration::from_millis(50),
    );
    let cancel = CancellationToken::new();
    let control = RunControl {
        cancel: Some(cancel.clone()),
        ..RunControl::default()
    };
    let run = run_through_core("cancel-reopen", conn.clone(), control);
    let interrupt = async {
        wait_for_reopen_attempts(&conn).await;
        cancel.cancel();
        conn.route_open_calls.load(Ordering::SeqCst)
    };
    let (run, opens_at_cancel) = tokio::join!(run, interrupt);
    assert_eq!(run.summary.reason, RunFinishReason::Cancelled);
    // Counted in probes rather than paused time: the run's own file I/O lets the paused clock
    // jump ahead, but a wait that kept going would keep probing (one every 50 ms here, for
    // the whole 90 s patience).
    let opens_after = conn.route_open_calls.load(Ordering::SeqCst) - opens_at_cancel;
    assert!(
        opens_after <= 2,
        "cancel must end the re-open wait at once, not hold the patience: {opens_after} more \
         opens after the cancel"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}

/// Shutdown (the module's stop request) during the re-open wait ends it the same way: the
/// run ends Interrupted promptly and nothing was sent.
#[tokio::test(start_paused = true)]
async fn shutdown_during_the_reopen_wait_ends_it_through_core() {
    let conn = outage_connection(
        vec![opens_closed(), RouteOpenScript::rejected(UNAVAILABLE)],
        vec![successful_tool_reply("must not be sent")],
        Duration::from_millis(50),
    );
    let stop = CancellationToken::new();
    let control = RunControl {
        stop_requested: Some(stop.clone()),
        ..RunControl::default()
    };
    let run = run_through_core("stop-reopen", conn.clone(), control);
    let interrupt = async {
        wait_for_reopen_attempts(&conn).await;
        stop.cancel();
        conn.route_open_calls.load(Ordering::SeqCst)
    };
    let (run, opens_at_stop) = tokio::join!(run, interrupt);
    assert_eq!(run.summary.reason, RunFinishReason::Interrupted);
    // Counted in probes rather than paused time, as in the cancel test above.
    let opens_after = conn.route_open_calls.load(Ordering::SeqCst) - opens_at_stop;
    assert!(
        opens_after <= 2,
        "shutdown must end the re-open wait at once: {opens_after} more opens after the stop"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}

/// Cancel while a re-open attempt is in flight, and that attempt then succeeds: the open is
/// abandoned at the cancel, and the route that opens afterwards carries no send.
#[tokio::test(start_paused = true)]
async fn cancel_during_an_in_flight_open_that_then_succeeds_sends_nothing() {
    let gate = RouteOpenGate::new();
    let conn = outage_connection(
        vec![
            opens_closed(),
            RouteOpenScript {
                gate: Some(gate.clone()),
                ..RouteOpenScript::default()
            },
        ],
        vec![successful_tool_reply("must not be sent")],
        Duration::ZERO,
    );
    let cancel = CancellationToken::new();
    let control = RunControl {
        cancel: Some(cancel.clone()),
        ..RunControl::default()
    };
    let run = run_through_core("cancel-in-flight", conn.clone(), control);
    let interrupt = async {
        gate.entered.notified().await;
        cancel.cancel();
        // Let the run see the cancel first, then let the abandoned open succeed.
        tokio::time::sleep(Duration::from_millis(100)).await;
        gate.release.notify_one();
    };
    let (run, ()) = tokio::join!(run, interrupt);
    assert_eq!(run.summary.reason, RunFinishReason::Cancelled);
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "an open that succeeds after cancel must carry no send"
    );
}

/// A module that refuses opens for 25 s and then accepts: the call waits it out through
/// Core's adapter, completes with the tool's own answer, and is sent exactly once. Under the
/// old 3 s patience this call failed the turn.
#[tokio::test(start_paused = true)]
async fn a_delayed_success_open_completes_the_call_through_core() {
    let mut opens = vec![opens_closed()];
    opens.extend((0..25).map(|_| refused_after(Duration::from_secs(1))));
    opens.push(RouteOpenScript::default());
    let conn = outage_connection(
        opens,
        vec![successful_tool_reply("served after restart")],
        Duration::ZERO,
    );
    let run = run_through_core("delayed-success", conn.clone(), RunControl::default()).await;
    assert_eq!(run.summary.reason, RunFinishReason::Completed);
    assert!(
        run.elapsed >= Duration::from_secs(25),
        "the call must have outlasted the 3 s budget to prove anything: {:?}",
        run.elapsed
    );
    assert_eq!(run.results.len(), 1);
    assert!(!run.results[0].is_error, "{:?}", run.results[0]);
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        1,
        "sent exactly once"
    );
}

/// A second `module_reloading` on the one re-send after a successful re-open is reported as
/// the attributed module outage (nothing sent, retry guidance), not as a definite tool
/// refusal, and the call is not sent a third time.
#[tokio::test(start_paused = true)]
async fn a_second_reloading_refusal_after_reopen_is_the_attributed_outage() {
    let conn = outage_connection(
        vec![RouteOpenScript::default()],
        vec![reloading(), reloading()],
        Duration::ZERO,
    );
    let run = run_through_core("second-refusal", conn.clone(), RunControl::default()).await;
    assert_eq!(run.summary.reason, RunFinishReason::Completed);
    assert_eq!(run.results.len(), 1);
    match &run.results[0].output {
        ToolOutput::Error {
            message,
            module,
            code,
            detail,
            nothing_sent,
            retry,
        } => {
            assert_eq!(message, broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE);
            assert_eq!(module.as_deref(), Some("aft"));
            assert_eq!(code.as_deref(), Some("module_reloading"));
            assert_eq!(
                detail.as_deref(),
                Some("module 'aft' is draining for a restart")
            );
            assert_eq!(*nothing_sent, Some(true));
            assert_eq!(
                retry.as_deref(),
                Some(broca_protocol::TOOL_PLANE_UNAVAILABLE_RETRY)
            );
        }
        other => panic!("expected the attributed outage, got {other:?}"),
    }
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        2,
        "the first request and its one re-send; no third"
    );
}

/// A run deadline earlier than the 90 s patience ends the re-open wait at that deadline with
/// the attributed outage. Core passes no deadline today (a run has no step deadline for tool
/// dispatch), so this drives the guarded dispatch entry Core's adapter forwards to.
#[tokio::test(start_paused = true)]
async fn a_run_deadline_earlier_than_the_patience_ends_the_wait() {
    let conn = outage_connection(
        vec![opens_closed(), RouteOpenScript::rejected(UNAVAILABLE)],
        vec![],
        Duration::from_millis(50),
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("run-deadline");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let started = Instant::now();
    let guard =
        ToolOpenGuard::new(None, None).with_deadline(Some(started + Duration::from_secs(20)));
    let result = plane.dispatch_guarded(&ctx, &tool_call, None, &guard).await;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_secs(20) && waited < Duration::from_secs(25),
        "the wait ends at the run deadline, not the patience: {waited:?}"
    );
    let DispatchResult::Completed(result) = result else {
        panic!("expected the attributed outage, got {result:?}");
    };
    assert!(
        matches!(&result.output, ToolOutput::Error { nothing_sent: Some(true), code: Some(code), .. } if code == "target_unavailable"),
        "{result:?}"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}

/// The run deadline also cuts an open attempt still in flight: an attempt that would answer
/// a minute later is abandoned at the deadline, reported with the last refusal.
#[tokio::test(start_paused = true)]
async fn a_run_deadline_cuts_an_in_flight_open_attempt() {
    let conn = outage_connection(
        vec![
            opens_closed(),
            RouteOpenScript::rejected(UNAVAILABLE),
            RouteOpenScript {
                delay: Duration::from_secs(60),
                ..RouteOpenScript::default()
            },
        ],
        vec![],
        Duration::ZERO,
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("run-deadline-in-flight");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let started = Instant::now();
    let guard =
        ToolOpenGuard::new(None, None).with_deadline(Some(started + Duration::from_secs(10)));
    let result = plane.dispatch_guarded(&ctx, &tool_call, None, &guard).await;
    let waited = started.elapsed();
    assert!(
        waited < Duration::from_secs(15),
        "the in-flight attempt ends at the deadline: {waited:?}"
    );
    assert!(
        matches!(&result, DispatchResult::Completed(result) if matches!(&result.output, ToolOutput::Error { nothing_sent: Some(true), .. })),
        "{result:?}"
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}

/// On a serial route a call that is re-opening gives its window credit back first: a sibling
/// waiting for that credit is not held for the length of the re-open wait.
#[tokio::test(start_paused = true)]
async fn a_serial_sibling_is_not_blocked_by_a_reopening_call() {
    let mut opens = vec![RouteOpenScript::default()];
    opens.extend((0..30).map(|_| refused_after(Duration::from_secs(1))));
    opens.push(RouteOpenScript::default());
    let conn = outage_connection(
        opens,
        vec![reloading(), successful_tool_reply("after the restart")],
        Duration::ZERO,
    );
    let send_gate = RouteOpenGate::new();
    *conn.send_gate.lock().unwrap() = Some(send_gate.clone());
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let ctx = batch_ctx("serial-sibling");
    let first = ToolCall {
        tool_call_id: ToolCallId("call-first".into()),
        ..call("read")
    };
    let sibling = ToolCall {
        tool_call_id: ToolCallId("call-sibling".into()),
        ..call("read")
    };
    plane
        .batch_started(
            &ctx,
            &[first.clone(), sibling.clone()],
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let started = Instant::now();
    let first_task = tokio::spawn({
        let (plane, ctx) = (plane.clone(), ctx.clone());
        async move {
            let result = plane.dispatch(&ctx, &first).await;
            (result, started.elapsed())
        }
    });
    // The first call holds the serial route's only credit while its request is in flight.
    send_gate.entered.notified().await;
    let sibling_task = tokio::spawn({
        let (plane, ctx) = (plane.clone(), ctx.clone());
        async move {
            let result = plane.dispatch(&ctx, &sibling).await;
            (result, started.elapsed())
        }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The request is refused `module_reloading`: the first call re-opens, which waits out a
    // 30 s outage.
    send_gate.release.notify_one();
    let (_, sibling_done) = sibling_task.await.unwrap();
    let (first_result, first_done) = first_task.await.unwrap();
    assert!(
        first_done >= Duration::from_secs(30),
        "the first call waited out the outage: {first_done:?}"
    );
    assert_completed_success(first_result);
    assert!(
        sibling_done < Duration::from_secs(5),
        "the sibling must not wait behind the re-open: {sibling_done:?}"
    );
}

/// A daemon refusal whose structured detail is an object, spread over lines and over 1 KiB,
/// reaches the model as one line of compact JSON cut to 1 KiB.
#[tokio::test(start_paused = true)]
async fn a_structured_refusal_detail_is_one_bounded_line() {
    let long = format!("line one\nline two {}", "y".repeat(2048));
    let refusal = RouteOpenScript::rejected_body(&subc_protocol::ErrorBody {
        code: "target_unavailable".into(),
        message: "module_id 'aft' is unavailable".into(),
        detail: Some(serde_json::json!({ "reason": long })),
    });
    // Instant refusals with probes 50 ms apart, so the paused clock moves through the wait.
    let conn = outage_connection(
        vec![opens_closed(), refusal],
        vec![],
        Duration::from_millis(50),
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let ctx = batch_ctx("structured-detail");
    let tool_call = call("read");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&tool_call),
            &ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]),
        )
        .await
        .unwrap();
    let DispatchResult::Completed(result) = plane.dispatch(&ctx, &tool_call).await else {
        panic!("expected the attributed outage");
    };
    let ToolOutput::Error {
        detail: Some(detail),
        ..
    } = &result.output
    else {
        panic!("expected a detail: {result:?}");
    };
    assert!(
        detail.starts_with(r#"{"reason":"line one\nline two yyy"#),
        "{detail}"
    );
    assert!(!detail.contains(['\n', '\r']), "one line: {detail}");
    assert_eq!(
        detail.len(),
        broca_protocol::MODULE_UNAVAILABLE_DETAIL_MAX_BYTES
    );
    assert_eq!(
        conn.tool_calls.load(Ordering::SeqCst),
        0,
        "nothing was sent"
    );
}
