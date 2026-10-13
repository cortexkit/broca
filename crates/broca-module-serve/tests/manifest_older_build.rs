//! A build that predates `plan-manifest/v1` refuses a session whose `RunStarted` froze a plan
//! manifest, through the real serve path, as a newer writer naming the feature. It never reads
//! the record with the manifest ignored: that would render the session without the tools and
//! system text it was admitted with.
//!
//! It plays the older build with `broca_wal::forget_feature_for_test`, which makes this
//! process's WAL decoder treat `plan-manifest/v1` as unknown, exactly as a build released before
//! the name existed would. That setting is process-wide, so this is the only test in its binary.

mod delivery_rig;

use broca_wal::{FileWal, Wal, WalRecord, PLAN_MANIFEST_FEATURE};
use delivery_rig::*;
use subc_protocol::FrameType;

#[tokio::test]
async fn a_build_without_plan_manifest_refuses_a_manifest_session_as_newer_writer() {
    let mut rig = Rig::start("manifest-older-build").await;
    let fixture: WalRecord = serde_json::from_str(
        &include_str!("../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "fixture"),
    )
    .unwrap();
    let WalRecord::RunStarted {
        manifest: frozen, ..
    } = fixture
    else {
        unreachable!()
    };
    assert!(frozen.is_some(), "the fixture carries a manifest");
    // A finished episode whose `RunStarted` carries a frozen plan manifest, written to the WAL
    // before any send, so no lineage actor holds the session or caches its state.
    let record = |value: serde_json::Value| -> WalRecord { serde_json::from_value(value).unwrap() };
    let mut started = record(serde_json::json!({
        "type": "run_started", "run_id": "run-1",
        "session": serde_json::to_value(&rig.session).unwrap(),
        "config": {"model": {"provider_module_id": "fake", "model_id": "fake"}, "tools": [],
            "tool_choice": {"type": "auto"}, "generation": {}, "response_format": {"type": "text"},
            "context_epoch": 0, "renderer_version": 1},
        "input": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
        "origin": {"kind": "fresh"}
    }));
    if let WalRecord::RunStarted { manifest, .. } = &mut started {
        *manifest = frozen;
    }
    let answer = record(serde_json::json!({
        "type": "model_step_finished", "step_id": 1,
        "assistant_message": {"message_id": "m1", "content": [{"type": "text", "text": "answer"}]},
        "usage": {}
    }));
    let finished = record(serde_json::json!({
        "type": "run_finished", "reason": "completed", "usage": {}
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
    wal.append(1, &answer, true).unwrap();
    wal.append(1, &finished, true).unwrap();
    drop(wal);
    let before = rig.wal_bytes();
    let needle = br#"{"requires":["plan-manifest/v1"],"record":{"type":"run_started""#;
    assert!(before.windows(needle.len()).any(|window| window == needle));

    // The build under test, which knows `plan-manifest/v1`, reads the session.
    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    assert_eq!(
        page["messages"].as_array().unwrap().last().unwrap()["message"]["content"][0]["text"],
        "answer",
        "{page}"
    );

    // A build that predates the feature refuses the session on every path, and the refusal
    // names the feature it is missing.
    let _older = broca_wal::forget_feature_for_test(PLAN_MANIFEST_FEATURE);
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
            serde_json::json!({"kind": "newer_writer", "missing": ["plan-manifest/v1"]}),
            "{label}: {error}"
        );
    }
    assert_eq!(rig.wal_bytes(), before, "a refusal writes nothing");
    rig.finish().await;
}
