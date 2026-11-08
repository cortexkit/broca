//! A live session moved from today's storage into the new storage through the real serve path,
//! as an operator moves one: `session.move_to_store` on the operator's own route, into a module
//! whose runs go through the real core loop on today's storage and the new loop on the new one.
//! Afterwards the session answers every read as it did before the move, refuses a cursor
//! today's storage issued, takes a send, and serves the same after a restart.

use std::time::Duration;

use serde_json::{json, Value};
use subc_protocol::{Frame, FrameType};

use crate::delivery_rig::*;

async fn call(rig: &mut Rig, method: &str, params: Value) -> Frame {
    rig.request(7, json!({"method": method, "params": params}))
        .await
}

fn refusal(frame: &Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

/// `value` without what a move changes by design: the log's incarnation and history version,
/// what is named by them (an output-reference set's id), and the cursors that name positions in
/// the log (a read's `head` among them).
fn masked(mut value: Value) -> Value {
    fn walk(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for name in [
                    "incarnation",
                    "history_version",
                    "set_id",
                    "cursor",
                    "older",
                    "newer",
                    "next",
                    "head",
                ] {
                    object.remove(name);
                }
                object.values_mut().for_each(walk);
            }
            Value::Array(items) => items.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut value);
    value
}

/// What the session answers to every read the move must keep: each `session.read` mode,
/// `session.history` in both forms, `session.head`, and each of `runs`' status, result and calls.
async fn reads(rig: &mut Rig, runs: &[String]) -> Vec<(String, Value)> {
    let mut asked = vec![
        ("session.read", json!({"limit": 500})),
        (
            "session.read",
            json!({"limit": 500, "include_display_facts": true}),
        ),
        ("session.read", json!({"limit": 500, "include_tools": true})),
        (
            "session.read",
            json!({"limit": 500, "include_rich_ui": true}),
        ),
        ("session.history", json!({"form": "raw"})),
        ("session.history", json!({"form": "display"})),
        ("session.head", json!({})),
    ];
    for run in runs {
        for method in ["run.status", "run.result", "run.tool_calls"] {
            asked.push((method, json!({ "run_id": run })));
        }
    }
    let mut answers = Vec::new();
    for (method, params) in asked {
        let reply = call(rig, method, params.clone()).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{method} {params}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        answers.push((format!("{method} {params}"), masked(result(&reply))));
    }
    answers
}

/// Every message's text, in order, as `session.read` serves it.
async fn texts(rig: &mut Rig) -> String {
    let page = result(&call(rig, "session.read", json!({"limit": 500})).await);
    page["messages"].to_string()
}

/// The run a send's reply names.
fn run_of(sent: &Frame) -> String {
    let reply = result(sent);
    reply["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("a send names its run: {reply}"))
        .to_owned()
}

#[tokio::test]
async fn a_live_session_moves_to_the_new_storage_and_keeps_serving() {
    let mut rig = Rig::start_movable("move-to-store").await;
    // Requests render under a real render config, which the move checks the copy's next
    // request against.
    let frozen = || Some(broca_provider::families::openai_chat_frozen_config("fake"));
    *rig.script.media_frozen.lock().unwrap() = frozen();

    // History: a tool turn whose result carries a note sent while the tool ran, then a plain
    // turn.
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("answer")]);
    rig.script.hold_tool(true);
    let started = rig.send("turn-1", "hi", None).await;
    assert_eq!(result(&started)["state"], "active");
    let mut runs = vec![run_of(&started)];
    rig.wait_entered().await;
    let note = rig
        .send_with(
            "note-1",
            "a note for the tool",
            None,
            json!({"delivery": ["tool_result"]}),
        )
        .await;
    result(&note);
    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.script.push(&[Step::Text("second")]);
    runs.push(run_of(&rig.send("turn-2", "again", None).await));
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let shown = result(
        &call(
            &mut rig,
            "session.read",
            json!({"limit": 500, "include_display_facts": true}),
        )
        .await,
    )
    .to_string();
    assert!(
        shown.contains("a note for the tool"),
        "the note rode the tool's result: {shown}"
    );
    let before = reads(&mut rig, &runs).await;
    let newest = result(&call(&mut rig, "session.history", json!({"limit": 2})).await);
    let old_cursor = newest["older"].clone();
    assert!(old_cursor.is_string(), "{newest}");

    // A live subscriber, attached before the move.
    rig.bind(8).await;
    let live = rig.subscribe_open(8, json!({})).await;

    // The move, on the operator's own route.
    rig.bind_as(2, Some(subc_protocol::Principal::Direct)).await;
    let moved = rig
        .request(2, json!({"method": "session.move_to_store", "params": {}}))
        .await;
    assert_eq!(
        moved.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&moved.body)
    );
    let moved = result(&moved);
    assert_eq!(moved["moved"], true, "{moved}");
    assert_eq!(moved["runs"], 2, "{moved}");
    assert_eq!(moved["media"], 0, "{moved}");
    assert!(
        moved["checks"]
            .as_array()
            .unwrap()
            .contains(&json!("next_request")),
        "{moved}"
    );
    // Today's storage no longer holds the session: its WAL is sealed, naming the new storage.
    assert!(rig.wal_path.with_extension("moved").exists());
    let sealed = rig.wal_bytes();

    // The live subscriber's stream ended, so its client subscribes again.
    broca_test_support::wait_for(
        "the live subscriber's stream ends",
        async {
            loop {
                if rig.skipped.iter().any(|frame| {
                    frame.header.corr == live && frame.header.ty == FrameType::StreamEnd
                }) {
                    return;
                }
                call(&mut rig, "session.head", json!({})).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || "no stream end".to_owned(),
    )
    .await;

    // Every read answers as before, now from the new storage.
    let after = reads(&mut rig, &runs).await;
    for ((asked, today), (_, store)) in before.iter().zip(&after) {
        assert_eq!(store, today, "{asked}");
    }
    // A cursor today's storage issued names a log the session no longer has.
    let expired = call(&mut rig, "session.history", json!({ "cursor": old_cursor })).await;
    assert_eq!(refusal(&expired)["code"], "cursor_expired");

    // A send after the move runs on the new storage and writes nothing to today's.
    rig.script.push(&[Step::Text("after the move")]);
    runs.push(run_of(&rig.send("turn-3", "moved now", None).await));
    rig.wait_idle().await;
    assert!(texts(&mut rig).await.contains("after the move"));
    assert_eq!(rig.wal_bytes(), sealed, "today's WAL is untouched");
    // A new subscriber reads the session's stream from the new storage.
    let frames = rig
        .subscribe_collect(9, json!({"from": "start"}), Duration::from_millis(300))
        .await;
    assert!(
        frames
            .first()
            .is_some_and(|frame| frame.header.ty != FrameType::Error),
        "{:?}",
        frames
            .first()
            .map(|frame| String::from_utf8_lossy(&frame.body).into_owned())
    );

    // After a restart the session is still the new storage's, and reads the same. The module
    // stops first, as a daemon's does, so no actor of the old one holds the session's lease.
    let settled = reads(&mut rig, &runs).await;
    assert!(rig.registry().stop_and_seal().await, "the module stopped");
    let mut rig = rig.restart().await;
    *rig.script.media_frozen.lock().unwrap() = frozen();
    let restarted = reads(&mut rig, &runs).await;
    for ((asked, before), (_, after)) in settled.iter().zip(&restarted) {
        assert_eq!(after, before, "{asked} after a restart");
    }
    rig.script.push(&[Step::Text("after the restart")]);
    result(&rig.send("turn-4", "restarted", None).await);
    rig.wait_idle().await;
    assert!(texts(&mut rig).await.contains("after the restart"));
    assert_eq!(rig.wal_bytes(), sealed, "today's WAL is untouched");
    rig.finish().await;
}

/// The session's send params for `send_id` with `prompt`, as [`Rig::send`] sends them.
fn send_params(send_id: &str, prompt: &str) -> Value {
    json!({"method": "session.send", "params": {
        "prompt": prompt,
        "send_id": send_id,
        "model": {"provider": "fake", "model": "fake"},
    }})
}

/// A send racing the move's fence is never lost. Either it lands before the fence, on today's
/// storage, and the move refuses the busy session until the send's run ends, then carries the
/// send along; or it lands after, and is refused as the session moving or served on the new
/// storage. Both orders of the two requests are tried, with an open actor and a live
/// subscriber, so the fence retires the actor while the send is in flight.
#[tokio::test]
async fn a_send_racing_the_move_is_never_lost() {
    for send_first in [true, false] {
        let mut rig = Rig::start_movable(&format!("move-race-{send_first}")).await;
        *rig.script.media_frozen.lock().unwrap() =
            Some(broca_provider::families::openai_chat_frozen_config("fake"));
        rig.script.push(&[Step::Text("first")]);
        result(&rig.send("turn-1", "hi", None).await);
        rig.wait_idle().await;
        rig.bind(8).await;
        rig.subscribe_open(8, json!({})).await;
        rig.bind_as(2, Some(subc_protocol::Principal::Direct)).await;

        rig.script.push(&[Step::Text("raced")]);
        let move_to_store = json!({"method": "session.move_to_store", "params": {}});
        let send = send_params("racer", "the racing send");
        let (send, moved) = if send_first {
            let send = rig.request_open(7, send).await;
            (send, rig.request_open(2, move_to_store).await)
        } else {
            let moved = rig.request_open(2, move_to_store).await;
            (rig.request_open(7, send).await, moved)
        };
        let moved = rig.reply_to(moved).await;
        let sent = rig.reply_to(send).await;
        let sent_body: Value = serde_json::from_slice(&sent.body).unwrap();
        let moved_body: Value = serde_json::from_slice(&moved.body).unwrap();
        let landed = match sent.header.ty {
            FrameType::Response => true,
            _ => {
                assert_eq!(sent_body["code"], "session_moving", "{sent_body}");
                false
            }
        };
        if moved.header.ty != FrameType::Response {
            // The send landed first: its run made the session busy, or its run's index row
            // wasn't settled yet. The move refused without changing anything, so it is retried
            // until the run is done and settled.
            assert_eq!(moved_body["code"], "not_movable", "{moved_body}");
            let reason = moved_body["detail"]["reason"].clone();
            assert!(
                reason == "session_busy" || reason == "reconcile_owed",
                "{moved_body}"
            );
            assert!(
                landed,
                "a send refused as moving can't make the session busy"
            );
            rig.wait_idle().await;
            broca_test_support::wait_for(
                "the move goes through once the raced run settled",
                async {
                    loop {
                        let again = rig
                            .request(2, json!({"method": "session.move_to_store", "params": {}}))
                            .await;
                        if again.header.ty == FrameType::Response {
                            return;
                        }
                        let body: Value = serde_json::from_slice(&again.body).unwrap();
                        assert_eq!(body["detail"]["reason"], "reconcile_owed", "{body}");
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                },
                || "the move kept refusing".to_owned(),
            )
            .await;
        }
        rig.wait_idle().await;
        let transcript = texts(&mut rig).await;
        if landed {
            assert!(
                transcript.contains("the racing send"),
                "an acknowledged send is in the moved session: {transcript}"
            );
        } else {
            assert!(!transcript.contains("the racing send"), "{transcript}");
        }
        assert!(rig.wal_path.with_extension("moved").exists());
        rig.finish().await;
    }
}
