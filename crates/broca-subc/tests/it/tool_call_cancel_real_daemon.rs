//! A tool call broca gives up on is cancelled at the provider, through the real subc daemon.
//!
//! The failure this guards against: a tool call outlived broca's reply deadline, broca stopped
//! waiting and recorded the outcome as unknown, but nothing told the provider. The provider ran
//! the call for minutes more with nobody waiting for it and held a project lock the whole time,
//! so every later route open on that project timed out. Broca now sends a data-plane `Cancel`
//! for the call's corr whenever it abandons a sent call: when the deadline expires, and when the
//! call's future is dropped (its run aborted). These tests drive the real `ck-subc` daemon and
//! `fake-aft-stub`, whose event log records each `Cancel` it receives and whether it still held
//! the call (`claimed`).

use crate::common;

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use broca_protocol::BindIdentity;
use broca_subc::{ConnError, SubcConnection, SubcToolPlane};
use common::{wait_for_aft, Rig};
use serde_json::{json, Value};

/// How long the stub holds a slow call. Far longer than any test here runs, so a call only
/// ends early if a `Cancel` ended it.
const SLOW_CALL_MS: u64 = 30_000;

fn event_log_path(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    broca_test_support::scratch_root().join(format!(
        "broca-tool-call-cancel-{tag}-{}-{nanos}.jsonl",
        std::process::id()
    ))
}

fn events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Wait for the first stub event matching `pred`, failing with the whole log if none arrives.
async fn wait_for_event(path: &Path, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(event) = events(path).into_iter().find(|event| pred(event)) {
            return event;
        }
        assert!(
            Instant::now() < deadline,
            "the stub never recorded {what}; log={:?}",
            events(path)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The stub's (channel, corr) for the tool call whose arguments carry `tag`: the cancel the
/// daemon forwards for that call names the same pair.
async fn received_call(path: &Path, tag: &str) -> (u64, u64) {
    let event = wait_for_event(path, &format!("the request tagged {tag}"), |event| {
        event["kind"] == "request_received" && event["body_json"]["arguments"]["tag"] == tag
    })
    .await;
    (
        event["channel"].as_u64().expect("request channel"),
        event["corr"].as_u64().expect("request corr"),
    )
}

fn cancels_for(path: &Path, (channel, corr): (u64, u64)) -> Vec<Value> {
    events(path)
        .into_iter()
        .filter(|event| {
            event["kind"] == "cancel"
                && event["channel"].as_u64() == Some(channel)
                && event["corr"].as_u64() == Some(corr)
        })
        .collect()
}

/// A rig whose `aft` stub records its events to a fresh log, plus a connection holding an
/// open tool route to it. None when the subc binaries are absent (the rig reports the skip).
async fn rig_with_route(
    tag: &str,
) -> Option<(Rig, PathBuf, SubcConnection, broca_subc::RouteHandle)> {
    let log = event_log_path(tag);
    let log_value = log.to_string_lossy().into_owned();
    let rig = Rig::start(
        tag,
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", "slow"),
            ("FAKE_AFT_EVENTS_PATH", log_value.as_str()),
        ],
    )
    .await?;
    let plane = SubcToolPlane::new(rig.connect().await, "aft");
    wait_for_aft(&plane, "slow").await;
    let conn = rig.connect().await;
    let route = conn
        .route_open("aft", &BindIdentity::new(&rig.dir, "tool-call-cancel", tag))
        .await
        .expect("route.open to the stub");
    Some((rig, log, conn, route))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_tool_call_is_cancelled_at_the_provider() {
    let Some((_rig, log, conn, route)) = rig_with_route("expiry").await else {
        return;
    };

    let error = conn
        .tool_call_with_deadline(
            &route,
            "slow",
            &json!({"delay_ms": SLOW_CALL_MS, "tag": "expiry"}),
            None,
            Duration::from_millis(500),
        )
        .await
        .expect_err("the stub holds the call far past the deadline");
    assert!(
        matches!(&error, ConnError::Transport(message) if message.starts_with("timed out after")),
        "the caller still sees its own timeout: {error:?}"
    );

    let call = received_call(&log, "expiry").await;
    let cancel = wait_for_event(&log, "a Cancel for the expired call", |event| {
        event["kind"] == "cancel"
            && event["channel"].as_u64() == Some(call.0)
            && event["corr"].as_u64() == Some(call.1)
    })
    .await;
    assert_eq!(
        cancel["claimed"], true,
        "the Cancel reached the stub while it still held the call, so the work stopped there"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        cancels_for(&log, call).len(),
        1,
        "exactly one Cancel per abandoned call"
    );
    let _ = std::fs::remove_file(&log);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_tool_call_is_cancelled_at_the_provider_exactly_once() {
    let Some((_rig, log, conn, route)) = rig_with_route("drop").await else {
        return;
    };

    // The connection outlives the aborted call, as the shared connection does in production.
    // Moving the only handle into the task would close the socket with the abort, and the
    // daemon would then end the call by detaching the route rather than by a Cancel.
    let conn = std::sync::Arc::new(conn);
    let task_conn = std::sync::Arc::clone(&conn);
    let call_task = tokio::spawn(async move {
        task_conn
            .tool_call_with_deadline(
                &route,
                "slow",
                &json!({"delay_ms": SLOW_CALL_MS, "tag": "drop"}),
                None,
                Duration::from_secs(60),
            )
            .await
    });
    // Abort only once the stub holds the call, so the Request was certainly sent.
    let call = received_call(&log, "drop").await;
    call_task.abort();
    assert!(call_task.await.unwrap_err().is_cancelled());

    let cancel = wait_for_event(&log, "a Cancel for the dropped call", |event| {
        event["kind"] == "cancel"
            && event["channel"].as_u64() == Some(call.0)
            && event["corr"].as_u64() == Some(call.1)
    })
    .await;
    assert_eq!(
        cancel["claimed"], true,
        "the stub still held the dropped call"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        cancels_for(&log, call).len(),
        1,
        "dropping the call cancels it exactly once"
    );
    let _ = std::fs::remove_file(&log);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answered_tool_call_is_not_cancelled() {
    let Some((_rig, log, conn, route)) = rig_with_route("answered").await else {
        return;
    };

    conn.tool_call_with_deadline(
        &route,
        "slow",
        &json!({"delay_ms": 0, "tag": "answered"}),
        None,
        Duration::from_secs(10),
    )
    .await
    .expect("the stub answers at once");

    let call = received_call(&log, "answered").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        cancels_for(&log, call).is_empty(),
        "a call the provider answered must not be cancelled; log={:?}",
        events(&log)
    );
    let _ = std::fs::remove_file(&log);
}
