//! A build that predates `steer-queue/v1` refuses a session whose WAL holds a steered message,
//! through the real serve path, as a newer writer naming the feature. It never skips the record:
//! that would drop a user message from the transcript.
//!
//! It plays the older build with `broca_wal::forget_feature_for_test`, which makes this
//! process's WAL decoder treat `steer-queue/v1` as unknown, exactly as a build released before
//! the name existed would. That setting is process-wide, so this is the only test in its binary.

mod delivery_rig;

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_wal::{FileWal, Wal, WalRecord, STEER_QUEUE_FEATURE};
use delivery_rig::*;
use subc_protocol::FrameType;

#[tokio::test]
async fn a_build_without_steer_queue_refuses_a_steered_session_as_newer_writer() {
    let mut rig = Rig::start("older-build").await;
    // A finished episode with a steer written during its first step. Seeded before any send,
    // so no lineage actor holds the session or caches its state.
    let record = |value: serde_json::Value| -> WalRecord { serde_json::from_value(value).unwrap() };
    let started = record(serde_json::json!({
        "type": "run_started", "run_id": "run-1",
        "session": serde_json::to_value(&rig.session).unwrap(),
        "config": {"model": {"provider_module_id": "fake", "model_id": "fake"}, "tools": [],
            "tool_choice": {"type": "auto"}, "generation": {}, "response_format": {"type": "text"},
            "context_epoch": 0, "renderer_version": 1},
        "input": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        "origin": {"kind": "fresh"}
    }));
    let answer = record(serde_json::json!({
        "type": "model_step_finished", "step_id": 1,
        "assistant_message": {"message_id": "m1", "content": [{"type": "text", "text": "answer"}]},
        "usage": {}
    }));
    let finished = record(serde_json::json!({
        "type": "run_finished", "reason": "cancelled", "usage": {}
    }));
    let mut wal = FileWal::open(&rig.wal_path)
        .unwrap()
        .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
    wal.append(1, &started, true).unwrap();
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(
        1,
        &WalRecord::Steered {
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: Some("steer-1".into()),
            request_identity_digest: None,
            delivery: Some(broca_protocol::Delivery::Steer),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "write up now".into(),
                }],
            )]
            .into(),
            hooks: None,
        },
        true,
    )
    .unwrap();
    wal.append(1, &answer, true).unwrap();
    wal.append(1, &finished, true).unwrap();
    drop(wal);
    let before = rig.wal_bytes();

    // This build reads the session: the steered message is in the transcript.
    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    assert_eq!(
        page["messages"].as_array().unwrap().last().unwrap()["message"]["content"][0]["text"],
        "write up now",
        "{page}"
    );

    // A build that predates the feature refuses the session on every path, and the refusal
    // names the feature it is missing.
    let _older = broca_wal::forget_feature_for_test(STEER_QUEUE_FEATURE);
    for (label, body) in [
        (
            "session.read",
            serde_json::json!({"method": "session.read", "params": {}}),
        ),
        (
            "session.send",
            serde_json::json!({"method": "session.send", "params": {
                "prompt": "next", "send_id": "turn-2",
                "model": {"provider": "fake", "model": "fake"}
            }}),
        ),
    ] {
        let reply = rig.request(7, body).await;
        assert_eq!(reply.header.ty, FrameType::Error, "{label}");
        let error: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], "wal_newer_writer", "{label}: {error}");
        assert_eq!(
            error["detail"],
            serde_json::json!({"kind": "newer_writer", "missing": ["steer-queue/v1"]}),
            "{label}: {error}"
        );
    }
    assert_eq!(rig.wal_bytes(), before, "a refusal writes nothing");
    rig.finish().await;
}
