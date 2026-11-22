//! A tool call's route open waits out a short provider-module restart, and nothing about that
//! wait may outlive the run: the run's deadline ends it at once, an open attempt still in flight
//! included, and a serial sibling call waits for the reopening call and sends after it. The
//! tests drive the tool plane over a scripted connection with a real module gate and a paused
//! clock. A route that "opens closed" makes the first send find it gone before anything is
//! written, which puts the call into the tool-dispatch re-open wait.

use super::*;

const UNAVAILABLE: &str = "target_unavailable — module_id 'aft' is unavailable";

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

#[test]
fn legacy_unavailable_replays_with_identical_result_bytes() {
    let reason: broca_records::record::NoDispatchReason =
        serde_json::from_str(r#"{"kind":"unavailable","detail":"tool plane unavailable"}"#)
            .unwrap();
    assert_eq!(
        serde_json::to_string(&reason).unwrap(),
        r#"{"kind":"unavailable","detail":"tool plane unavailable"}"#
    );
    let result = broca_core::no_dispatch_result(&ToolCallId("call-old".into()), &reason);
    assert_eq!(
        serde_json::to_string(&result).unwrap(),
        r#"{"tool_call_id":"call-old","output":{"kind":"error","message":"tool plane unavailable"},"is_error":true}"#
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

/// The text a completed call's tool answered with.
fn completed_text(result: &DispatchResult) -> String {
    match result {
        DispatchResult::Completed(result) if !result.is_error => {
            serde_json::to_string(&result.output).unwrap()
        }
        other => panic!("expected a successful reply, got {other:?}"),
    }
}

/// On a serial route a call refused `module_reloading` gives its window credit back and
/// re-opens, waiting out a 30 s restart. The sibling that was waiting for that credit finds
/// the route replaced, has sent nothing, and re-opens once alongside it through the same
/// gate: both wait out the one outage together (one shared probe per tick) instead of the
/// sibling failing, or waiting out the outage a second time behind the first call. The
/// sibling gets its own reply, and sends only after the first call has finished, so the
/// serial route keeps batch order.
#[tokio::test(start_paused = true)]
async fn a_serial_sibling_waits_out_the_restart_with_the_reopening_call_and_sends_after_it() {
    let mut opens = vec![RouteOpenScript::default()];
    opens.extend((0..30).map(|_| refused_after(Duration::from_secs(1))));
    opens.push(RouteOpenScript::default());
    let conn = outage_connection(
        opens,
        vec![
            reloading(),
            successful_tool_reply("first, after the restart"),
            successful_tool_reply("sibling, after the restart"),
        ],
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
    let (sibling_result, sibling_done) = sibling_task.await.unwrap();
    let (first_result, first_done) = first_task.await.unwrap();
    assert!(
        completed_text(&first_result).contains("first, after the restart"),
        "{first_result:?}"
    );
    assert!(
        completed_text(&sibling_result).contains("sibling, after the restart"),
        "the sibling gets its own real reply: {sibling_result:?}"
    );
    assert_eq!(
        *conn.dispatched_ids.lock().unwrap(),
        ["call-first", "call-first", "call-sibling"].map(|id| Some(id.to_owned())),
        "the first request, its one re-send, and only then the sibling's request"
    );
    assert!(
        first_done >= Duration::from_secs(25),
        "the first call waited out the outage: {first_done:?}"
    );
    assert!(
        sibling_done >= first_done && sibling_done < first_done + Duration::from_secs(1),
        "the sibling finishes when the module is back, right after the first call, not after \
         an outage wait of its own: first {first_done:?}, sibling {sibling_done:?}"
    );
    assert_eq!(
        conn.route_open_calls.load(Ordering::SeqCst),
        1 + 30 + 2,
        "the batch's open, one shared probe per refusal while the module was down, then one \
         open per call once it was back"
    );
}

/// Batch order on a serial route when the sibling's route is ready first: the call refused
/// `module_reloading` re-opens slowly (2 s) while the sibling, which found the route replaced
/// once it got the credit, opens a fresh route at once. The sibling still sends only after
/// the first call has finished.
#[tokio::test(start_paused = true)]
async fn a_serial_sibling_whose_route_opens_first_still_sends_after_the_reopening_call() {
    let conn = outage_connection(
        vec![
            RouteOpenScript::default(),
            // The first call's re-open.
            RouteOpenScript {
                delay: Duration::from_secs(2),
                ..RouteOpenScript::default()
            },
            // The sibling's re-open, and any after it.
            RouteOpenScript::default(),
        ],
        vec![
            reloading(),
            successful_tool_reply("first, re-sent"),
            successful_tool_reply("sibling, sent after it"),
        ],
        Duration::ZERO,
    );
    let send_gate = RouteOpenGate::new();
    *conn.send_gate.lock().unwrap() = Some(send_gate.clone());
    let plane = Arc::new(SubcToolPlane::with_connection(conn.clone(), "aft"));
    let ctx = batch_ctx("serial-order");
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
    let first_task = tokio::spawn({
        let (plane, ctx) = (plane.clone(), ctx.clone());
        async move { plane.dispatch(&ctx, &first).await }
    });
    send_gate.entered.notified().await;
    let sibling_task = tokio::spawn({
        let (plane, ctx) = (plane.clone(), ctx.clone());
        async move { plane.dispatch(&ctx, &sibling).await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_gate.release.notify_one();
    let sibling_result = sibling_task.await.unwrap();
    let first_result = first_task.await.unwrap();
    assert!(completed_text(&first_result).contains("first, re-sent"));
    assert!(completed_text(&sibling_result).contains("sibling, sent after it"));
    assert_eq!(
        *conn.dispatched_ids.lock().unwrap(),
        ["call-first", "call-first", "call-sibling"].map(|id| Some(id.to_owned())),
        "the sibling's route was ready first, but it sent only after the first call finished"
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
