//! What `session.send` and `session.send_status` answer a head's sender while its sends queue
//! behind a running run, through the real serve path.

use super::app_readers::{principal, response, send_wire, RunGate};
use super::*;

/// `session.send_status` for `send_id` once it reads `state`.
async fn status_once(rig: &mut Rig, send_id: &str, state: &str) -> Value {
    for _ in 0..2000 {
        let status = rig
            .result(1, "session.send_status", json!({"send_id": send_id}))
            .await;
        if status["state"] == state {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("send {send_id} never read {state}");
}

/// A head's second send queues behind its running first. `session.send` answers `active` for
/// the first and `pending` for the second, and `session.send_status` follows each send from
/// `pending` through `active` to `finished`, under its own run id.
#[tokio::test]
async fn a_heads_queued_send_reads_pending_then_active_then_finished() {
    let gate = RunGate::new();
    let mut rig = Rig::start_gated(
        "head-send-status",
        vec![
            text_step("a", "first answer"),
            text_step("b", "second answer"),
        ],
        ReadyProvider::default(),
        None,
        Some(gate.clone()),
        false,
    )
    .await;
    rig.bind_session(1, principal("prefrontal-core")).await;
    let first = response(&send_wire(&mut rig, 1, "inbox-a", None, Some(json!("queue"))).await);
    assert_eq!(first["state"], "active", "{first}");
    let run_a = first["run_id"].as_str().unwrap().to_owned();
    broca_test_support::wait_for(
        "the first run's model request is held",
        gate.entered.acquire(),
        || "the model was never asked".into(),
    )
    .await
    .unwrap()
    .forget();
    // The second run's model request is held too, so its `active` state can be read.
    gate.hold_next();
    let active_a = status_once(&mut rig, "inbox-a", "active").await;
    assert_eq!(active_a["run_id"], run_a.as_str(), "{active_a}");

    let second = response(&send_wire(&mut rig, 1, "inbox-b", None, Some(json!("queue"))).await);
    assert_eq!(second["state"], "pending", "{second}");
    let submission = second["submission_id"].as_str().unwrap().to_owned();
    assert!(!submission.is_empty());
    let pending_b = status_once(&mut rig, "inbox-b", "pending").await;
    assert_eq!(
        pending_b["submission_id"],
        submission.as_str(),
        "{pending_b}"
    );

    gate.release.add_permits(1);
    let finished_a = status_once(&mut rig, "inbox-a", "finished").await;
    assert_eq!(finished_a["reason"], "completed", "{finished_a}");
    assert_eq!(finished_a["run_id"], run_a.as_str(), "{finished_a}");

    broca_test_support::wait_for(
        "the second run's model request is held",
        gate.entered.acquire(),
        || "the queued send never started its run".into(),
    )
    .await
    .unwrap()
    .forget();
    let active_b = status_once(&mut rig, "inbox-b", "active").await;
    let run_b = active_b["run_id"].as_str().unwrap().to_owned();
    assert_ne!(run_b, run_a);
    gate.release.add_permits(1);
    let finished_b = status_once(&mut rig, "inbox-b", "finished").await;
    assert_eq!(finished_b["reason"], "completed", "{finished_b}");
    assert_eq!(finished_b["run_id"], run_b.as_str(), "{finished_b}");
}
