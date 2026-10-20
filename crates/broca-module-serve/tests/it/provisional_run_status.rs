//! `run.status` on the run id of a queued send, through the real serve path and core loop.
//!
//! A queued send's reply names only its submission id, `sub-<run id>`. Callers derive the run id
//! from it and poll `run.status` and `session.read` on that id to learn what became of the send.
//! A send using the `delivery_order` group may never become a run of its own: it can be delivered
//! into the run already going (as a steer, say), replaced, expire or be withdrawn. Broca answers the
//! id for every one of those, from the WAL alone, so the answer survives a restart. This is a
//! broca extension; the llm-runner role contract answers an id naming no run `unknown`.
//!
//! The same holds for a re-send of the queued send's `send_id`: once the WAL records where the
//! send was delivered, the re-send's `delivered` receipt says so, before and after a restart.

use crate::delivery_rig::*;

use std::time::Duration;

use broca_wal::WalRecord;
use subc_protocol::Frame;

/// Short, so an idle session's actor closes quickly and a restart reopens a closed session.
const IDLE_TTL: Duration = Duration::from_millis(200);

/// The ordered list every send here uses unless it says otherwise: deliver into the running
/// turn at its next step, or start a run when the session is idle.
fn steer_or_idle_start() -> serde_json::Value {
    serde_json::json!({"delivery": ["steer", "idle_start"]})
}

fn run_id_of(frame: &Frame) -> String {
    result(frame)["run_id"].as_str().unwrap().to_owned()
}

/// The run id a queued send's submission id was minted from.
fn provisional_run_id(frame: &Frame) -> String {
    let reply = result(frame);
    assert_eq!(reply["state"], "pending", "{reply}");
    reply["submission_id"]
        .as_str()
        .unwrap()
        .strip_prefix("sub-")
        .unwrap()
        .to_owned()
}

async fn run_status(rig: &mut Rig, run_id: &str) -> Frame {
    rig.request(
        7,
        serde_json::json!({"method": "run.status", "params": {"run_id": run_id}}),
    )
    .await
}

fn send_delivered(records: &[WalRecord]) -> Vec<(String, String)> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::SendDelivered {
                submission_id,
                run_id,
                ..
            } => Some((submission_id.0.clone(), run_id.0.clone())),
            _ => None,
        })
        .collect()
}

fn started_runs(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect()
}

fn step_receipt(run_id: &str) -> serde_json::Value {
    serde_json::json!({"as": "step", "ref": run_id})
}

/// An ordered send queued behind a running run and delivered into it as a steer never becomes a
/// run of its own. Its run id answers `pending` while it waits, then the run it joined (with a
/// `step` receipt naming that run), on the live session and after a restart alike. `run.cancel`
/// on the id cancels nothing: the run it joined goes on. A re-send of its `send_id` answers the
/// joined run with the same receipt, after the restart too.
#[tokio::test]
async fn a_send_delivered_as_a_steer_answers_the_run_it_joined_across_a_restart() {
    let mut rig = Rig::start_with_idle_ttl("provisional-steer", IDLE_TTL).await;
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Call("call-2"),
        Step::Text("done"),
    ]);
    rig.script.hold_tool(true);
    let joined = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;

    let queued = rig
        .send_with("note", "also this", None, steer_or_idle_start())
        .await;
    let provisional = provisional_run_id(&queued);
    let pending = run_status(&mut rig, &provisional).await;
    assert_golden(
        &pending,
        "run_status.provisional_pending.json",
        &provisional,
    );
    let resent = rig
        .send_with("note", "also this", None, steer_or_idle_start())
        .await;
    assert_eq!(result(&resent), result(&queued), "still queued");

    // The tool result's step boundary delivers the send into the running turn. Its records are
    // durable once the next tool call's dispatch intent is, and that call is held, so the run
    // the send joined is still going.
    rig.script.hold_model(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    rig.script.hold_tool(true);
    rig.script.hold_model(false);
    rig.wait_entered().await;
    let records = rig.records();
    assert_eq!(
        send_delivered(&records),
        [(format!("sub-{provisional}"), joined.clone())],
        "delivered once, into the running turn"
    );
    // The delivery is answered once it is durable; until then the send is still pending.
    let live = broca_test_support::event_timeout(
        "the durable delivery is answered",
        async {
            loop {
                let live = result(&run_status(&mut rig, &provisional).await);
                if live["state"] != "pending" {
                    return live;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || "run.status still answered pending".into(),
    )
    .await
    .expect("the durable delivery is answered");
    assert_eq!(live["state"], "active", "{live}");
    assert_eq!(live["delivered"], step_receipt(&joined), "{live}");
    let active_status = rig
        .request(
            7,
            serde_json::json!({"method":"send.status", "params":{"send_id":"note"}}),
        )
        .await;
    let active_resent = rig
        .send_with("note", "also this", None, steer_or_idle_start())
        .await;
    assert_eq!(active_status.body, active_resent.body);
    let active_reply = result(&active_status);
    assert_eq!(active_reply["state"], "active");
    assert_eq!(active_reply["run_id"], joined);
    assert_eq!(active_reply["delivered"], step_receipt(&joined));
    pin_resend(&active_status, "send_status.active_step.json", &joined);

    // Cancelling the send's run id cancels nothing: the send is part of another run now.
    assert_eq!(rig.cancel(&provisional).await, "not_active");
    let still = result(&run_status(&mut rig, &joined).await);
    assert_eq!(still["state"], "active", "the joined run goes on: {still}");

    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    // The terminal record is durable before the live actor stops reporting the run as
    // active, so wait for the joined run's status to settle before taking it as the
    // expected answer; the restart below reads the settled state from the log.
    let joined_status = broca_test_support::event_timeout(
        "the joined run's status settles to its terminal",
        async {
            loop {
                let status = run_status(&mut rig, &joined).await;
                if result(&status)["state"] != "active" {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || "run.status on the joined run still answered active".into(),
    )
    .await
    .expect("the joined run's status settles");
    let ended_live = run_status(&mut rig, &provisional).await;
    let mut expected = result(&joined_status);
    expected["delivered"] = step_receipt(&joined);
    assert_eq!(
        result(&ended_live),
        expected,
        "the joined run's whole status"
    );

    rig.wait_closed().await;
    rig = rig.restart().await;
    let records = rig.records();
    assert_eq!(
        started_runs(&records),
        std::slice::from_ref(&joined),
        "no run of its own"
    );

    // Closed session: nothing has reopened the actor yet.
    let restarted = run_status(&mut rig, &provisional).await;
    assert_golden(
        &restarted,
        "run_status.provisional_delivered_steer.json",
        &joined,
    );
    assert_eq!(result(&restarted), expected);

    let status_frame = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "note"}}),
        )
        .await;
    pin_resend(&status_frame, "send_status.step.json", &joined);
    // The re-send after the restart answers the run the send joined, with the
    // `step` receipt, never `pending`.
    let resent_frame = rig
        .send_with("note", "also this", None, steer_or_idle_start())
        .await;
    assert_eq!(
        resent_frame.body, status_frame.body,
        "send.status is the same bytes as a re-send"
    );
    pin_resend(
        &resent_frame,
        "send_reply.ordered_resend_step.json",
        &joined,
    );
    let resent = result(&resent_frame);
    assert_eq!(resent["state"], "finished", "{resent}");
    assert_eq!(resent["run_id"], joined.as_str(), "{resent}");
    assert_eq!(resent["delivered"], step_receipt(&joined), "{resent}");

    // The re-send reopened the session's actor: the live path answers the same bytes.
    let reopened = run_status(&mut rig, &provisional).await;
    assert_eq!(reopened.body, restarted.body);
    assert_eq!(rig.cancel(&provisional).await, "not_active");
    assert_eq!(
        started_runs(&rig.records()),
        [joined],
        "nothing ran the send again"
    );
    rig.finish().await;
}

/// An ordered send queued behind a run whose last step is under way is not delivered into that
/// run (no further step boundary comes); it starts its own run once the session is idle, under
/// the run id its submission was minted from. That id then answers its own run's status with no
/// receipt, and a re-send of its `send_id` carries the `turn` receipt naming the run, after a
/// restart too.
#[tokio::test]
async fn an_ordered_send_that_starts_its_own_run_answers_that_run() {
    let mut rig = Rig::start_with_idle_ttl("provisional-started", IDLE_TTL).await;
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    rig.script.hold_model(true);
    let first = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    let queued = rig
        .send_with("later", "and then", None, steer_or_idle_start())
        .await;
    let provisional = provisional_run_id(&queued);
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    let records = rig.records();
    assert_eq!(started_runs(&records), [first, provisional.clone()]);
    assert!(send_delivered(&records).is_empty(), "{records:?}");

    // As above: the terminal record lands before the live actor stops answering `active`.
    let status = broca_test_support::event_timeout(
        "the started run's status settles to its terminal",
        async {
            loop {
                let status = run_status(&mut rig, &provisional).await;
                if result(&status)["state"] != "active" {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || "run.status on the started run still answered active".into(),
    )
    .await
    .expect("the started run's status settles");
    assert_golden(&status, "run_status.provisional_started.json", &provisional);
    let turn = serde_json::json!({"as": "turn", "ref": provisional});
    let resent = result(
        &rig.send_with("later", "and then", None, steer_or_idle_start())
            .await,
    );
    assert_eq!(resent["state"], "finished", "{resent}");
    assert_eq!(resent["run_id"], provisional.as_str(), "{resent}");
    assert_eq!(resent["delivered"], turn, "{resent}");

    rig.wait_closed().await;
    rig = rig.restart().await;
    assert_eq!(
        run_status(&mut rig, &provisional).await.body,
        status.body,
        "the same answer after a restart"
    );
    let status_frame = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "later"}}),
        )
        .await;
    pin_resend(&status_frame, "send_status.turn.json", &provisional);
    let resent_frame = rig
        .send_with("later", "and then", None, steer_or_idle_start())
        .await;
    assert_eq!(
        resent_frame.body, status_frame.body,
        "send.status is the same bytes as a re-send"
    );
    pin_resend(
        &resent_frame,
        "send_reply.ordered_resend_turn.json",
        &provisional,
    );
    let resent = result(&resent_frame);
    assert_eq!(resent["run_id"], provisional.as_str(), "{resent}");
    assert_eq!(resent["delivered"], turn, "{resent}");
    rig.finish().await;
}

/// Pins a re-send or send-status reply from the real serve path, with
/// `@RUN_ID@` standing for the run the receipt names. `BROCA_UPDATE_GOLDEN` writes them.
fn pin_resend(frame: &Frame, file: &str, run_id: &str) {
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        let body = String::from_utf8(frame.body.to_vec()).expect("UTF-8 reply");
        let body = if run_id.is_empty() {
            body
        } else {
            body.replace(run_id, "@RUN_ID@")
        };
        std::fs::write(golden_path(file), body).expect("write the re-send golden");
    }
    assert_golden(frame, file, run_id);
}

/// A queued send that ends undelivered answers its terminal send state for good: `retracted`
/// once withdrawn, `superseded` once a newer send under its dedupe key replaced it, `expired`
/// once its expiry passed. The answers are the same after a restart.
#[tokio::test]
async fn an_undelivered_send_answers_its_terminal_state_across_a_restart() {
    let mut rig = Rig::start_with_idle_ttl("provisional-ended", IDLE_TTL).await;
    rig.script.hold_model(true);
    rig.send("turn-1", "task", None).await;
    rig.wait_entered().await;

    let idle_start = serde_json::json!({"delivery": ["idle_start"]});
    let withdrawn = provisional_run_id(
        &rig.send_with("withdrawn", "never mind", None, idle_start.clone())
            .await,
    );
    let retract = rig
        .request(
            7,
            serde_json::json!({"method": "session.retract", "params": {"submission_id": format!("sub-{withdrawn}")}}),
        )
        .await;
    assert_eq!(result(&retract), "retracted");

    let keyed = serde_json::json!({"delivery": ["steer"], "dedupe_key": "ctx"});
    let replaced = provisional_run_id(&rig.send_with("old", "old", None, keyed.clone()).await);
    let replacement = provisional_run_id(&rig.send_with("new", "new", None, keyed).await);

    // Already past its expiry when it arrives: ended durably at once, and answered so.
    let expiring = serde_json::json!({"delivery": ["steer"], "expires_at_ms": 1});
    let expired_reply = result(&rig.send_with("late", "late", None, expiring).await);
    assert_eq!(expired_reply["state"], "expired", "{expired_reply}");
    let expired = expired_reply["submission_id"]
        .as_str()
        .unwrap()
        .strip_prefix("sub-")
        .unwrap()
        .to_owned();

    let expected = [
        (withdrawn.clone(), "retracted"),
        (replaced.clone(), "superseded"),
        (expired.clone(), "expired"),
        (replacement.clone(), "pending"),
    ];
    for (run_id, state) in &expected {
        let status = run_status(&mut rig, run_id).await;
        assert_eq!(result(&status), serde_json::json!({"state": state}));
    }

    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_closed().await;
    rig = rig.restart().await;
    for (run_id, state) in &expected {
        let status = run_status(&mut rig, run_id).await;
        assert_eq!(
            result(&status),
            serde_json::json!({"state": state}),
            "after the restart"
        );
    }
    rig.finish().await;
}

#[tokio::test]
async fn send_status_writes_nothing_and_never_opens_a_closed_actor() {
    let mut rig = Rig::start_with_idle_ttl("send-status-read-only", IDLE_TTL).await;
    let before = rig.wal_bytes();
    let reply = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "absent"}}),
        )
        .await;
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(
        rig.wal_bytes(),
        before,
        "even an unknown send must not create a WAL"
    );
    rig.script.push(&[Step::Text("done")]);
    rig.send("first", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_closed().await;
    rig = rig.restart().await;
    let before = rig.wal_bytes();
    let reply = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "first"}}),
        )
        .await;
    assert_eq!(result(&reply)["state"], "finished");
    assert_eq!(rig.wal_bytes(), before);
    assert_eq!(
        rig.registry().slot_counts().open,
        0,
        "polling a closed session must not reopen it"
    );
    assert_eq!(started_runs(&rig.records()).len(), 1);
    rig.finish().await;
}

#[tokio::test]
async fn send_status_unknown_send_is_terminal_and_strict() {
    let mut rig = Rig::start_with_idle_ttl("send-status-unknown", IDLE_TTL).await;
    rig.send("known", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_closed().await;
    rig = rig.restart().await;
    let reply = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "absent"}}),
        )
        .await;
    pin_resend(&reply, "send_status.unknown_send.json", "");
    let error: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(error["code"], "unknown_send");
    let reply = rig.request(7, serde_json::json!({"method": "send.status", "params": {"send_id": "known", "typo": true}})).await;
    let error: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "typo");
    rig.finish().await;
}

#[tokio::test]
async fn send_status_pending_answers_resend_bytes_after_restart() {
    let mut rig = Rig::start_with_idle_ttl("send-status-pending", IDLE_TTL).await;
    rig.send("first", "hi", None).await;
    rig.wait_for_finished(1).await;
    let extra = serde_json::json!({"delivery": ["steer"]});
    let pending = rig.send_with("waiting", "wait", None, extra.clone()).await;
    let provisional = provisional_run_id(&pending);
    rig.wait_closed().await;
    rig = rig.restart().await;
    let reply = rig
        .request(
            7,
            serde_json::json!({"method": "send.status", "params": {"send_id": "waiting"}}),
        )
        .await;
    pin_resend(&reply, "send_status.pending.json", &provisional);
    assert_eq!(reply.body, pending.body);
    assert_eq!(rig.registry().slot_counts().open, 0);
    let resend = rig.send_with("waiting", "wait", None, extra).await;
    assert_eq!(reply.body, resend.body);
    rig.finish().await;
}
