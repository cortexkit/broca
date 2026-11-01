//! Pin the default receive-silence policy: 120 seconds since the last frame, reset by Push.
//! A healthy silent provider can lose its reply; held bash/watch calls to module `aft` wait longer.
//! Virtual time keeps the production deadline intact without making the tests wait two minutes.

use super::*;
use crate::tool_deadline::tool_reply_deadline;
use serde_json::json;

async fn silent_tool(
    name: &str,
    arguments: serde_json::Value,
    provider_delay: Duration,
) -> (ConnResult<serde_json::Value>, bool, Duration) {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(9, 1);
    let deadline = tool_reply_deadline("aft", name, &arguments);
    let started = time::Instant::now();
    let ((result, elapsed), accepted) = tokio::join!(
        async {
            let result = conn
                .tool_call_with_deadline(&route, name, &arguments, Some("silent-call"), deadline)
                .await;
            (result, started.elapsed())
        },
        async {
            let request = module.next_request().await;
            time::sleep(provider_delay).await;
            module.reply(&request, br#"{"content":[],"isError":false}"#.to_vec())
        }
    );
    (result, accepted, elapsed)
}

#[tokio::test(start_paused = true)]
async fn healthy_silent_inspect_loses_its_reply_at_the_default_cutoff() {
    let (result, accepted, elapsed) =
        silent_tool("inspect", json!({"scope": "src"}), Duration::from_secs(121)).await;
    let error = result.expect_err("a silent inspect inherits the 120-second receive deadline");
    assert!(
        matches!(error, ConnError::Transport(ref message) if message == "timed out after 120s"),
        "the production cutoff must be reproduced, not a closed connection: {error:?}"
    );
    assert!(!accepted, "the healthy provider's late reply has no waiter");
    assert_eq!(elapsed, Duration::from_secs(120));
}

#[tokio::test(start_paused = true)]
async fn push_resets_the_default_receive_silence_cutoff() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(9, 1);
    let started = time::Instant::now();
    let ((result, elapsed), accepted) = tokio::join!(
        async {
            let result = conn.tool_call(&route, "inspect", &json!({}), None).await;
            (result, started.elapsed())
        },
        async {
            let request = module.next_request().await;
            time::sleep(Duration::from_secs(119)).await;
            assert!(module.terminal(&request, FrameType::Push, b"{}".to_vec()));
            time::sleep(Duration::from_secs(121)).await;
            module.reply(&request, br#"{"content":[],"isError":false}"#.to_vec())
        }
    );
    assert!(
        matches!(result, Err(ConnError::Transport(ref message)) if message == "timed out after 120s")
    );
    assert_eq!(elapsed, Duration::from_secs(239));
    assert!(!accepted, "the provider answered 121 s after its last Push");
}

#[tokio::test(start_paused = true)]
async fn healthy_silent_waiting_bash_survives_the_default_cutoff() {
    let (result, accepted, elapsed) = silent_tool(
        "bash",
        json!({"command": "long build", "wait": true}),
        Duration::from_secs(121),
    )
    .await;
    assert!(accepted, "a held bash reply still has its waiter at 121 s");
    assert_eq!(result.unwrap(), json!({"content": [], "isError": false}));
    assert_eq!(elapsed, Duration::from_secs(121));
}

#[tokio::test(start_paused = true)]
async fn healthy_silent_bash_watch_survives_the_default_cutoff() {
    let (result, accepted, elapsed) = silent_tool(
        "bash_watch",
        json!({"taskId": "build", "timeoutMs": 1_800_000}),
        Duration::from_secs(1800),
    )
    .await;
    assert!(
        accepted,
        "a silent 30-minute watch still has its waiter when the tool answers"
    );
    assert_eq!(result.unwrap(), json!({"content": [], "isError": false}));
    assert_eq!(elapsed, Duration::from_secs(1800));
}
