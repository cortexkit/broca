//! The tool plane's routes through the real subc daemon and a stand-in tool module (the rig's
//! fake of AFT, the code-tools module, serving an `echo` tool): closing a session
//! cancels its warm binds and drops its cached route, a session reopened after close gets a
//! fresh lifecycle, an invalidated route reopens on a new channel, and a module restart between
//! dispatches costs one transparent retry.

use crate::common;

use broca_core::ToolManifest;
use broca_protocol::request::{ExecutionMode, Tool};
use broca_protocol::BindIdentity;
use broca_protocol::{content::ToolCall, ToolCallId};
use broca_subc::SubcToolPlane;
use common::{wait_for_aft, Rig};
use serde_json::json;

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: "fault-classification probe".into(),
        input_schema: json!({"type": "object"}).into(),
        module: None,
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    }
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

/// Same `BindIdentity` after idle-close must reopen: closing cancels the session's lifecycle
/// token and removes it, so the next capture mints a fresh one rather than finding a
/// permanent tombstone that would refuse every later route for the session.
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
/// the model.
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
    rig.crash_supervised_module("aft");
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
        input: json!({"value": "hello-after-restart"}).into(),
        provider_state: None,
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
