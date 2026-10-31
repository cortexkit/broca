//! `session.send` delivery modes through the real serve path: the reply bytes of the default
//! `queue` mode, pinned before anything else changed, and the behaviour a send's `delivery`
//! selects.
//!
//! Every run goes through the real core loop over a real file WAL, so each record the tests
//! read back was written by core or the lineage actor exactly as in production. The model is a
//! scripted provider and the one tool is an in-test tool plane that can hold a call open, so a
//! test decides exactly when a run is inside a model step or inside a tool call.

use crate::delivery_rig;

use delivery_rig::*;
use std::time::Duration;

use broca_wal::WalRecord;
use subc_protocol::{Frame, FrameType};

/// The reply bytes of the default mode, pinned. A send while the session is idle is `active`; a
/// send while a run is active is durably queued and answered `pending`; a same-key send with a
/// different prompt is refused `send_id_reuse` naming `input`; and a retry of a finished send is
/// answered `finished`. A send spelling `"delivery": "queue"` gets exactly the same bytes as one
/// that omits the field, because queue is the default.
#[tokio::test]
async fn queue_mode_reply_bytes_are_pinned() {
    for delivery in [None, Some("queue")] {
        let tag = delivery.unwrap_or("absent");
        let mut rig = Rig::start(&format!("pin-{tag}")).await;
        // The first turn calls the tool once before answering, so the pinned `finished` covers
        // a turn with a tool batch in it.
        rig.script
            .push(&[Step::Call("call-1"), Step::Text("answer")]);
        rig.script.hold_model(true);
        let active = rig.send("turn-1", "hi", delivery).await;
        let run_1 = result(&active)["run_id"].as_str().unwrap().to_owned();
        assert_golden_admitting(&active, "send_reply.active.json", &run_1);
        rig.wait_entered().await;

        let pending = rig.send("turn-2", "next", delivery).await;
        let submission = result(&pending)["submission_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let run_2 = submission.strip_prefix("sub-").unwrap().to_owned();
        assert_golden(&pending, "send_reply.pending.json", &run_2);

        let before = rig.wal_bytes();
        let reuse = rig.send("turn-2", "changed", delivery).await;
        assert_eq!(reuse.header.ty, FrameType::Error, "{tag}");
        assert_golden(&reuse, "send_id_reuse.input.json", "");
        assert_eq!(
            rig.wal_bytes(),
            before,
            "{tag}: a refused send writes nothing"
        );

        rig.script.hold_model(false);
        rig.wait_for_finished(2).await;
        let finished = rig.send("turn-1", "hi", delivery).await;
        assert_golden_admitting(&finished, "send_reply.finished.json", &run_1);
        rig.finish().await;
    }
}

/// Each episode's end in WAL order: the `RunFinished` reason, or `paused`.
fn endings(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunFinished { reason, .. } => Some(
                serde_json::to_value(reason)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ),
            WalRecord::RunPaused { .. } => Some("paused".to_owned()),
            _ => None,
        })
        .collect()
}

/// Each episode's run id in WAL order.
fn starts(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect()
}

fn run_id_of(frame: &Frame) -> String {
    result(frame)["run_id"].as_str().unwrap().to_owned()
}

fn pending_run_id(frame: &Frame) -> String {
    result(frame)["submission_id"]
        .as_str()
        .unwrap()
        .strip_prefix("sub-")
        .unwrap()
        .to_owned()
}

fn pair(role: &str, text: &str) -> (String, String) {
    (role.to_owned(), text.to_owned())
}

/// The last text of a prompt the model was called with.
fn last_text(prompt: &[broca_protocol::content::Message]) -> String {
    match prompt.last().and_then(|message| message.content.first()) {
        Some(broca_protocol::content::ContentBlock::Text { text }) => text.clone(),
        other => format!("{other:?}"),
    }
}

/// A steer sent while a tool call runs joins that run: the reply is `active` naming the running
/// run (pinned bytes), sent after the `steered` record is durable. The message is placed after
/// the call's result, never between the call and its result, and the next model step renders
/// it as the last user message. The tool ran exactly once. A retry of the same steer answers
/// `active` for that run while it lives and its `finished` once it has ended, never a new turn,
/// and the same key under another mode is refused naming `delivery`. Once the next step has
/// rendered it, the retry also carries the delivery receipt `step` naming the run.
#[tokio::test]
async fn a_steer_during_a_tool_call_joins_the_running_turn_after_the_result() {
    let mut rig = Rig::start("steer-tool").await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("written up")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;

    let steered = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_golden(&steered, "send_reply.steer_joined.json", &run_1);
    let records = rig.records();
    assert!(
        records
            .iter()
            .any(|record| matches!(record, WalRecord::Steered { .. })),
        "the steer is durable before its reply"
    );
    let retry = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_golden(&retry, "send_reply.steer_joined.json", &run_1);

    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(endings(&records), ["completed"]);
    assert_eq!(
        starts(&records),
        std::slice::from_ref(&run_1),
        "no new turn"
    );
    assert_eq!(*rig.script.dispatched.lock().unwrap(), ["call-1"]);
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert_eq!(prompts.len(), 2);
    assert_eq!(last_text(&prompts[1]), "write up now");
    rig.wait_idle().await;
    assert_eq!(
        rig.transcript().await,
        [
            pair("user", "task"),
            pair("assistant", "tool_call"),
            pair("tool", "tool_result"),
            pair("user", "write up now"),
            pair("assistant", "written up"),
        ]
    );

    let after = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_golden(&after, "send_reply.steer_retry_step.json", &run_1);
    let before = rig.wal_bytes();
    let other_mode = rig.send("steer-1", "write up now", None).await;
    assert_golden(&other_mode, "send_id_reuse.delivery.json", "");
    assert_eq!(rig.wal_bytes(), before, "a refused send writes nothing");
    rig.finish().await;
}

/// A steer with no run active is a plain send: it starts a turn, with the reply bytes pinned. A
/// later send under the same key and another mode is refused naming `delivery`.
#[tokio::test]
async fn a_steer_with_no_active_run_starts_a_turn() {
    let mut rig = Rig::start("steer-idle").await;
    let started = rig.send("turn-1", "hello", Some("steer")).await;
    let run_1 = run_id_of(&started);
    assert_golden_admitting(&started, "send_reply.steer_idle.json", &run_1);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(starts(&records), [run_1]);
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::Steered { .. })));
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    let reuse = rig.send("turn-1", "hello", Some("interrupt")).await;
    assert_golden(&reuse, "send_id_reuse.delivery.json", "");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[tokio::test]
async fn joining_steer_settings_conflict_refuses_before_any_wal_write() {
    let mut rig = Rig::start("steer-settings-conflict").await;
    rig.script.hold_model(true);
    rig.send_with(
        "turn-1",
        "task",
        None,
        serde_json::json!({
            "model": {"provider": "fake", "model": "fake", "variant": "high"},
            "generation": {
                "max_output_tokens": 100, "temperature": 0.5, "top_p": 0.9,
                "stop_sequences": ["stop"]
            }
        }),
    )
    .await;
    rig.wait_entered().await;

    let cases = [
        (
            "model",
            serde_json::json!({"model": {"provider": "fake", "model": "other", "variant": "low"}, "generation": {"temperature": 0.1}}),
        ),
        (
            "model",
            serde_json::json!({"model": {"provider": "other", "model": "fake"}}),
        ),
        (
            "model.variant",
            serde_json::json!({"model": {"provider": "fake", "model": "fake", "variant": "low"}}),
        ),
        (
            "generation.max_output_tokens",
            serde_json::json!({"generation": {"max_output_tokens": 101, "temperature": 0.1}}),
        ),
        (
            "stop_when.max_steps",
            serde_json::json!({"stop_when": {"max_steps": 2}}),
        ),
        (
            "generation.temperature",
            serde_json::json!({"generation": {"temperature": 0.1, "top_p": 0.1}}),
        ),
        (
            "generation.top_p",
            serde_json::json!({"generation": {"top_p": 0.1, "stop_sequences": []}}),
        ),
        (
            "generation.stop_sequences",
            serde_json::json!({"generation": {"stop_sequences": ["other"]}}),
        ),
        (
            "generation.stop_sequences",
            serde_json::json!({"generation": {"stop_sequences": []}}),
        ),
    ];
    for (index, (field, extra)) in cases.into_iter().enumerate() {
        let before = rig.wal_bytes();
        let refused = rig
            .send_with(&format!("steer-{index}"), "change", Some("steer"), extra)
            .await;
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "invalid_params", "{field}: {body}");
        assert_eq!(body["detail"]["field"], field);
        assert_eq!(rig.wal_bytes(), before, "{field}: refusal writes nothing");
        if index == 0 {
            assert_golden(&refused, "send_reply.steer_settings_conflict.json", "");
        }
    }
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(1).await;
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::Steered { .. })));
    rig.finish().await;
}

#[tokio::test]
async fn joining_steer_same_settings_and_omissions_are_accepted() {
    let mut rig = Rig::start("steer-settings-same").await;
    rig.script.hold_model(true);
    let settings = serde_json::json!({
        "model": {"provider": "fake", "model": "fake", "variant": "high"},
        "generation": {
            "max_output_tokens": 100, "temperature": 0.5, "top_p": 0.9,
            "stop_sequences": ["stop"]
        }
    });
    let run = run_id_of(
        &rig.send_with("turn-1", "task", None, settings.clone())
            .await,
    );
    rig.wait_entered().await;
    for (index, extra) in [
        serde_json::json!({}),
        settings,
        serde_json::json!({"generation": {"temperature": 0.5}}),
        serde_json::json!({"model": null}),
    ]
    .into_iter()
    .enumerate()
    {
        let accepted = rig
            .send_with(&format!("steer-{index}"), "same", Some("steer"), extra)
            .await;
        assert_golden(&accepted, "send_reply.steer_joined.json", &run);
    }
    assert_eq!(
        rig.records()
            .iter()
            .filter(|record| matches!(record, WalRecord::Steered { .. }))
            .count(),
        4
    );
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(starts(&records), [run]);
    rig.finish().await;
}

/// A joining steer's auth pin is compared with the credential the running run froze. Naming
/// another method or another account is refused `invalid_params` naming `auth` and writes
/// nothing; naming the frozen method, alone or with the frozen account, joins the run.
#[tokio::test]
async fn joining_steer_auth_pin_is_refused_unless_it_names_the_frozen_credential() {
    let mut rig = Rig::start("steer-auth-pin").await;
    *rig.script.auth_selection.lock().unwrap() = Some(broca_wal::AuthSelection {
        billing_vault_facts: None,
        auth_method: None,
        credential_id: "oauth:anthropic:work".into(),
        account_ref: None,
        resolved_credential_id: None,
        vault_request: None,
        account_decision: None,
    });
    rig.script.hold_model(true);
    let run = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    let frozen = rig
        .records()
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted { config, .. } => config.auth_selection.clone(),
            _ => None,
        })
        .expect("the run froze its auth selection");
    assert_eq!(frozen.credential_id, "oauth:anthropic:work");
    assert_eq!(
        result(&run_status(&mut rig, &run).await)["auth_selection"],
        "oauth:anthropic:work"
    );

    for (index, pin) in [
        serde_json::json!({"method": "apikey"}),
        serde_json::json!({"method": "apikey", "account": "work"}),
        serde_json::json!({"method": "oauth", "account": "home"}),
    ]
    .into_iter()
    .enumerate()
    {
        let before = rig.wal_bytes();
        let refused = rig
            .send_with(
                &format!("refused-{index}"),
                "switch credential",
                Some("steer"),
                serde_json::json!({"auth": pin}),
            )
            .await;
        assert_eq!(refused.header.ty, FrameType::Error, "{pin}");
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "invalid_params", "{pin}: {body}");
        assert_eq!(body["detail"]["field"], "auth", "{pin}: {body}");
        assert_eq!(rig.wal_bytes(), before, "{pin}: refusal writes nothing");
        if index == 0 {
            crate::scope_carrier::assert_reply_golden(
                &refused,
                "steer_settings_refused.auth.json",
                None,
                "187d7c600b0df20b63ef5aa8939623d2522ee6381882dc8ad79d2294054d4f6f",
            );
        }
    }

    for (index, pin) in [
        serde_json::json!({"method": "oauth"}),
        serde_json::json!({"method": "oauth", "account": "work"}),
    ]
    .into_iter()
    .enumerate()
    {
        let accepted = rig
            .send_with(
                &format!("accepted-{index}"),
                "same credential",
                Some("steer"),
                serde_json::json!({"auth": pin}),
            )
            .await;
        assert_golden(&accepted, "send_reply.steer_joined.json", &run);
    }
    assert_eq!(
        rig.records()
            .iter()
            .filter(|record| matches!(record, WalRecord::Steered { .. }))
            .count(),
        2,
        "only the two matching steers were recorded"
    );
    rig.script
        .auth_selection
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .credential_id = "apikey:replacement".into();
    assert_eq!(
        result(&run_status(&mut rig, &run).await)["auth_selection"],
        "oauth:anthropic:work"
    );
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(starts(&records), std::slice::from_ref(&run));
    rig.wait_idle().await;
    assert_eq!(
        result(&run_status(&mut rig, &run).await)["auth_selection"],
        "oauth:anthropic:work"
    );
    rig.finish().await;
}

#[tokio::test]
async fn idle_steer_interrupt_and_queue_honour_their_named_settings() {
    for delivery in ["steer", "interrupt", "queue"] {
        let mut rig = Rig::start(&format!("steer-settings-new-{delivery}")).await;
        rig.script.hold_model(delivery != "steer");
        rig.send("turn-1", "task", None).await;
        if delivery == "steer" {
            rig.wait_for_finished(1).await;
            rig.wait_idle().await;
        } else {
            rig.wait_entered().await;
        }
        let accepted = rig
            .send_with(
                "turn-2",
                "new settings",
                Some(delivery),
                serde_json::json!({
                    "model": {"provider": "fake", "model": "other", "variant": "low"},
                    "generation": {
                        "max_output_tokens": 101, "temperature": 0.1, "top_p": 0.2,
                        "stop_sequences": ["end"]
                    }
                }),
            )
            .await;
        let run = if delivery == "steer" {
            run_id_of(&accepted)
        } else {
            pending_run_id(&accepted)
        };
        rig.script.hold_model(false);
        let records = rig.wait_for_finished(2).await;
        let config = records
            .iter()
            .find_map(|record| match record {
                WalRecord::RunStarted { run_id, config, .. } if run_id.0 == run => Some(config),
                _ => None,
            })
            .expect("the new run froze its requested settings");
        assert_eq!(config.model.provider_module_id, "fake");
        assert_eq!(config.model.model_id, "other");
        assert_eq!(config.variant.as_deref(), Some("low"));
        assert_eq!(config.generation.max_output_tokens, Some(101));
        assert_eq!(config.generation.temperature, Some(0.1));
        assert_eq!(config.generation.top_p, Some(0.2));
        assert_eq!(config.generation.stop_sequences, ["end"]);
        rig.finish().await;
    }
}

#[tokio::test]
async fn accepted_steer_settings_retry_replays_after_the_active_run_changes() {
    let mut rig = Rig::start("steer-settings-retry").await;
    let settings = serde_json::json!({
        "model": {"provider": "fake", "model": "fake", "variant": "high"},
        "generation": {"temperature": 0.5}
    });
    rig.script.hold_model(true);
    let run = run_id_of(
        &rig.send_with("turn-1", "task", None, settings.clone())
            .await,
    );
    rig.wait_entered().await;
    let first = rig
        .send_with("steer-1", "same", Some("steer"), settings.clone())
        .await;
    assert_golden(&first, "send_reply.steer_joined.json", &run);
    let before = rig.wal_bytes();
    let retry = rig
        .send_with("steer-1", "same", Some("steer"), settings.clone())
        .await;
    assert_eq!(retry.body, first.body);
    assert_eq!(rig.wal_bytes(), before);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let after = rig
        .send_with("steer-1", "same", Some("steer"), settings.clone())
        .await;
    assert_golden(&after, "send_reply.steer_retry_step.json", &run);

    rig.script.hold_model(true);
    rig.send_with(
        "turn-2",
        "different run",
        None,
        serde_json::json!({
            "model": {"provider": "fake", "model": "other", "variant": "low"},
            "generation": {"temperature": 0.1}
        }),
    )
    .await;
    rig.wait_entered().await;
    let before = rig.wal_bytes();
    let retry = rig
        .send_with("steer-1", "same", Some("steer"), settings)
        .await;
    assert_eq!(
        retry.body, after.body,
        "replay describes the original joined run"
    );
    assert_eq!(
        rig.wal_bytes(),
        before,
        "replay neither revalidates nor writes"
    );
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.finish().await;
}

/// A paused session is not an idle one: a steer into it is refused `run_paused` naming the
/// paused run (pinned bytes), and the WAL is byte-identical afterwards.
#[tokio::test]
async fn a_steer_into_a_paused_run_is_refused_and_writes_nothing() {
    let mut rig = Rig::start("steer-paused").await;
    *rig.script.pause_next_run.lock().unwrap() = true;
    let run_1 = run_id_of(&rig.send("turn-1", "hello", None).await);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    let floor = std::fs::read_to_string(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME)).ok();
    let refused = rig.send("steer-1", "more", Some("steer")).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_golden(&refused, "run_paused.json", &run_1);
    assert_eq!(rig.wal_bytes(), before, "a refused steer writes nothing");
    assert_eq!(
        std::fs::read_to_string(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME)).ok(),
        floor
    );
    rig.finish().await;
}

#[tokio::test]
async fn loop_finished_with_a_late_steer_renders_one_extra_step() {
    final_answer_with_late_steer(
        "loop",
        serde_json::json!([{"type": "loop_finished"}]),
        false,
    )
    .await;
}

#[tokio::test]
async fn step_limit_with_a_late_steer_reports_undelivered_send_id() {
    // Put LoopFinished first to prove a later satisfied limit still prevents extension.
    final_answer_with_late_steer(
        "limit",
        serde_json::json!([
            {"type": "loop_finished"}, {"type": "step_count", "n": 1}
        ]),
        true,
    )
    .await;
}

#[tokio::test]
async fn empty_stop_when_with_a_late_steer_renders_one_extra_step() {
    final_answer_with_late_steer("empty", serde_json::json!([]), false).await;
}

async fn final_answer_with_late_steer(tag: &str, stop_when: serde_json::Value, limited: bool) {
    let mut rig =
        Rig::start_with_idle_ttl(&format!("steer-final-{tag}"), Duration::from_millis(100)).await;
    rig.script
        .push(&[Step::Text("answer"), Step::Text("after steer")]);
    rig.script.hold_model(true);
    let run_1 = run_id_of(
        &rig.send_with(
            "turn-1",
            "task",
            None,
            serde_json::json!({"stop_when": stop_when}),
        )
        .await,
    );
    rig.wait_entered().await;
    let steered = rig.send("steer-1", "one more thing", Some("steer")).await;
    assert_eq!(run_id_of(&steered), run_1);
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let prompts = rig.script.prompts.lock().unwrap().clone();
    // LoopFinished is first in the stop list, so the run reports completed even
    // when the later step-count condition forbids another step.
    assert_eq!(endings(&records), ["completed"]);
    if limited {
        assert_eq!(prompts.len(), 1, "a steer never buys a step past a limit");
        assert_eq!(
            rig.transcript().await,
            [
                pair("user", "task"),
                pair("assistant", "answer"),
                pair("user", "one more thing")
            ]
        );
    } else {
        assert_eq!(prompts.len(), 2, "exactly one extra step");
        assert_eq!(last_text(&prompts[1]), "one more thing");
        assert_eq!(
            rig.transcript().await,
            [
                pair("user", "task"),
                pair("assistant", "answer"),
                pair("user", "one more thing"),
                pair("assistant", "after steer")
            ]
        );
    }
    assert_eq!(starts(&records), std::slice::from_ref(&run_1));
    let status = rig
        .request(
            7,
            serde_json::json!({"method": "run.status", "params": {"run_id": run_1}}),
        )
        .await;
    if limited {
        assert_eq!(
            result(&status)["undelivered_steers"],
            serde_json::json!(["steer-1"])
        );
        assert_golden(&status, "run_status.undelivered_steer.json", &run_1);
    } else {
        assert_eq!(result(&status)["undelivered_steers"], serde_json::json!([]));
        assert_golden(
            &status,
            "run_status.completed_no_undelivered_steers.json",
            &run_1,
        );
    }
    // A new registry and store must reconstruct both status and prompt from durable WAL.
    rig.wait_closed().await;
    rig = rig.restart().await;
    let restarted = rig
        .request(
            7,
            serde_json::json!({"method": "run.status", "params": {"run_id": run_1}}),
        )
        .await;
    assert_eq!(result(&restarted), result(&status));
    if !limited {
        rig.script.push(&[Step::Text("next answer")]);
        rig.send("turn-2", "next", None).await;
        rig.wait_for_finished(2).await;
        let resumed = rig.script.prompts.lock().unwrap().clone();
        assert_eq!(&resumed[0][..prompts[1].len()], prompts[1].as_slice());
        assert_eq!(last_text(&resumed[0]), "next");
    }
    rig.finish().await;
}

/// A cancel with a steer pending ends the run `Cancelled` and starts no new turn; the steer is
/// in the transcript once, after the last message of the cancelled run.
#[tokio::test]
async fn a_cancel_with_a_steer_pending_starts_no_new_turn() {
    let mut rig = Rig::start("steer-cancel").await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("never")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    rig.send("steer-1", "hurry", Some("steer")).await;
    assert_eq!(rig.cancel(&run_1).await, "ack");
    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let records_later = rig.records();
    assert_eq!(endings(&records), ["cancelled"]);
    assert_eq!(starts(&records_later), [run_1], "no new turn");
    assert_eq!(rig.script.prompts.lock().unwrap().len(), 1);
    assert_eq!(
        rig.transcript().await,
        [
            pair("user", "task"),
            pair("assistant", "tool_call"),
            pair("tool", "tool_result"),
            pair("user", "hurry"),
        ]
    );
    rig.finish().await;
}

/// An interrupt during a model stream answers `pending` (pinned bytes) once its `Queued` is
/// durable, and the order in the WAL is the guarantee: the `Queued{delivery: interrupt}`
/// precedes the interrupted run's `Cancelled`, which precedes the new run. The aborted step
/// leaves no assistant message, and the new message runs exactly once.
#[tokio::test]
async fn an_interrupt_during_a_model_stream_cancels_then_starts_the_message() {
    let mut rig = Rig::start("interrupt-model").await;
    rig.script.hold_model(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    let pending = rig
        .send("stop-1", "stop and do this", Some("interrupt"))
        .await;
    let run_2 = pending_run_id(&pending);
    assert_golden(&pending, "send_reply.interrupt_pending.json", &run_2);
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(endings(&records), ["cancelled", "completed"]);
    assert_eq!(starts(&records), [run_1, run_2]);
    let queued_at = records
        .iter()
        .position(|record| {
            matches!(
                record,
                WalRecord::Queued {
                    delivery: Some(broca_protocol::Delivery::Interrupt),
                    ..
                }
            )
        })
        .expect("the interrupt's Queued");
    let cancelled_at = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunFinished { .. }))
        .unwrap();
    assert!(
        queued_at < cancelled_at,
        "the message is durable before the cancel"
    );
    rig.wait_idle().await;
    assert_eq!(
        rig.transcript().await,
        [
            pair("user", "task"),
            pair("user", "stop and do this"),
            pair("assistant", "done"),
        ]
    );
    rig.finish().await;
}

/// An interrupt during a tool call waits for the call (it is never killed or re-sent), then the
/// run ends `Cancelled` and the message starts as the next turn.
#[tokio::test]
async fn an_interrupt_during_a_tool_call_waits_for_the_call_then_cancels() {
    let mut rig = Rig::start("interrupt-tool").await;
    rig.script.push(&[Step::Call("call-1")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    let run_2 = pending_run_id(&rig.send("stop-1", "stop", Some("interrupt")).await);
    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(endings(&records), ["cancelled", "completed"]);
    assert_eq!(starts(&records), [run_1, run_2]);
    assert_eq!(*rig.script.dispatched.lock().unwrap(), ["call-1"]);
    rig.finish().await;
}

/// An interrupt jumps the queue: with plain sends P1 and P2 queued behind a running turn, an
/// interrupt I runs next, then P1, then P2, each exactly once.
#[tokio::test]
async fn an_interrupt_runs_ahead_of_plain_queued_sends() {
    let mut rig = Rig::start("interrupt-order").await;
    rig.script.hold_model(true);
    let run_0 = run_id_of(&rig.send("turn-0", "task", None).await);
    rig.wait_entered().await;
    let p1 = pending_run_id(&rig.send("p1", "first plain", None).await);
    let p2 = pending_run_id(&rig.send("p2", "second plain", None).await);
    let i = pending_run_id(&rig.send("i", "interrupt", Some("interrupt")).await);
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(4).await;
    assert_eq!(starts(&records), [run_0, i, p1, p2]);
    assert_eq!(endings(&records)[0], "cancelled");
    rig.finish().await;
}

/// An interrupt into a paused session makes its message durable first, then ends the paused
/// run through the existing cancel path (`Cancelled`, the paused run's only terminal), then
/// starts the message. The paused run had no dispatched call without a result to abandon.
#[tokio::test]
async fn an_interrupt_ends_a_paused_run_and_starts_the_message() {
    let mut rig = Rig::start("interrupt-paused").await;
    *rig.script.pause_next_run.lock().unwrap() = true;
    let run_1 = run_id_of(&rig.send("turn-1", "hello", None).await);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let before = rig.records();
    let intents = before
        .iter()
        .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
        .count();
    let results = before
        .iter()
        .filter(|record| matches!(record, WalRecord::ToolResult { .. }))
        .count();
    assert_eq!(intents, results, "nothing in flight is abandoned");
    let pending = rig
        .send("stop-1", "do this instead", Some("interrupt"))
        .await;
    let run_2 = pending_run_id(&pending);
    let records = rig.wait_for_finished(3).await;
    assert_eq!(endings(&records), ["paused", "cancelled", "completed"]);
    assert_eq!(starts(&records), [run_1, run_2]);
    rig.finish().await;
}

/// A plain send with a new `send_id` into a session whose last run is paused for auth is
/// refused `run_paused` naming the paused run, with the same pinned bytes a steer gets, and the
/// WAL is byte-identical afterwards. Before this refusal existed the same send was answered
/// `active` naming the PAUSED run and its message was recorded nowhere.
#[tokio::test]
async fn a_plain_send_into_a_paused_session_is_refused_run_paused() {
    for delivery in [None, Some("queue")] {
        let tag = delivery.unwrap_or("absent");
        let mut rig = Rig::start(&format!("plain-paused-{tag}")).await;
        *rig.script.pause_next_run.lock().unwrap() = true;
        let run_1 = run_id_of(&rig.send("turn-1", "hello", None).await);
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        let before = rig.wal_bytes();
        let refused = rig.send("turn-2", "a brand new message", delivery).await;
        assert_eq!(refused.header.ty, FrameType::Error, "{tag}");
        assert_golden(&refused, "run_paused.json", &run_1);
        rig.wait_idle().await;
        assert_eq!(
            rig.wal_bytes(),
            before,
            "{tag}: a refused send writes nothing"
        );
        rig.finish().await;
    }
}

/// The paused run's own retry (same `send_id`, same payload) is not a new message: it keeps
/// the existing resume path and is answered `active` naming that run, never `run_paused`, and
/// starts no episode. (An interrupt into a paused session is covered by
/// `an_interrupt_ends_a_paused_run_and_starts_the_message`.)
#[tokio::test]
async fn the_paused_runs_own_retry_still_resumes_it() {
    let mut rig = Rig::start("paused-own-retry").await;
    *rig.script.pause_next_run.lock().unwrap() = true;
    let run_1 = run_id_of(&rig.send("turn-1", "hello", None).await);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let retry = rig.send("turn-1", "hello", None).await;
    assert_golden_admitting(&retry, "send_reply.active.json", &run_1);
    rig.wait_idle().await;
    assert_eq!(starts(&rig.records()), [run_1]);
    rig.finish().await;
}

#[tokio::test]
async fn conversation_key_refusal_bytes_are_pinned() {
    let mut rig = Rig::start("conversation-key-refusal").await;
    rig.script.hold_model(true);
    let invalid = rig
        .send_with(
            "turn",
            "hi",
            None,
            serde_json::json!({"conversation_key": ""}),
        )
        .await;
    assert_golden(&invalid, "invalid_params.conversation_key.json", "");
    assert!(rig.wal_bytes().is_empty());
    let active = rig
        .send_with(
            "turn",
            "hi",
            None,
            serde_json::json!({"conversation_key": "consult:a"}),
        )
        .await;
    assert_eq!(result(&active)["state"], "active");
    rig.wait_entered().await;
    let before = rig.wal_bytes();
    let reuse = rig
        .send_with(
            "turn",
            "hi",
            None,
            serde_json::json!({"conversation_key": "consult:b"}),
        )
        .await;
    assert_golden(&reuse, "send_id_reuse.conversation_key.json", "");
    assert_eq!(rig.wal_bytes(), before);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.finish().await;
}

/// A retry's `delivered` receipt, decoded with the role's own `SendReply` the way an owner
/// decodes it.
fn receipt(frame: &Frame) -> Option<cortexkit_role_llm_runner::send::Delivered> {
    serde_json::from_value::<cortexkit_role_llm_runner::send::SendReply>(result(frame))
        .unwrap_or_else(|error| panic!("{}: {error}", String::from_utf8_lossy(&frame.body)))
        .delivered
}

/// Re-send `send_id` until its reply carries a receipt. Every reply before that carries none:
/// broca never answers `pending` or `unknown`, only absent until a durable step has rendered
/// the steer.
async fn retry_until_receipted(rig: &mut Rig, send_id: &str, prompt: &str) -> Frame {
    let path = rig.dir.clone();
    broca_test_support::wait_for(
        "durable steer delivery receipt",
        async {
            loop {
                let reply = rig.send(send_id, prompt, Some("steer")).await;
                if receipt(&reply).is_some() {
                    return reply;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || format!("send_id={send_id}, state_root={}", path.display()),
    )
    .await
}

/// A steer's retry while the run it joined is still running: no receipt while the run is
/// inside the tool call the steer arrived during, and `step` naming that run once the next step
/// has started (the model is held inside it, so the run is still `active`).
#[tokio::test]
async fn a_steer_retry_answers_step_once_the_joined_runs_next_step_renders_it() {
    let mut rig = Rig::start("steer-receipt-step").await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("written up")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    rig.send("steer-1", "write up now", Some("steer")).await;

    let before = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_golden(&before, "send_reply.steer_joined.json", &run_1);
    assert_eq!(receipt(&before), None, "not rendered yet");

    rig.script.hold_model(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    let rendered = retry_until_receipted(&mut rig, "steer-1", "write up now").await;
    assert_golden(&rendered, "send_reply.steer_retry_step_active.json", &run_1);
    let again = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_eq!(
        receipt(&again),
        receipt(&rendered),
        "a receipt never changes"
    );

    rig.script.hold_model(false);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(starts(&records), std::slice::from_ref(&run_1));
    rig.wait_idle().await;
    let finished = rig.send("steer-1", "write up now", Some("steer")).await;
    assert_golden(&finished, "send_reply.steer_retry_step.json", &run_1);
    assert_eq!(receipt(&finished), receipt(&rendered));
    rig.finish().await;
}

/// A steer whose run is cancelled before any step renders it: its retry carries no receipt
/// while it waits in the transcript, and once the next send's episode has started a step (its
/// prefix carries the steer) the retry answers `turn`, naming that new run, while still naming
/// the cancelled run it joined as the run it answers for.
#[tokio::test]
async fn a_steer_retry_answers_turn_when_the_next_episode_renders_it() {
    let mut rig = Rig::start("steer-receipt-turn").await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("never")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(&rig.send("turn-1", "task", None).await);
    rig.wait_entered().await;
    rig.send("steer-1", "hurry", Some("steer")).await;
    assert_eq!(rig.cancel(&run_1).await, "ack");
    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(1).await;
    assert_eq!(endings(&records), ["cancelled"]);
    rig.wait_idle().await;

    let unrendered = rig.send("steer-1", "hurry", Some("steer")).await;
    assert_golden(&unrendered, "send_reply.steer_retry_cancelled.json", &run_1);
    assert_eq!(receipt(&unrendered), None);

    let run_2 = run_id_of(&rig.send("turn-2", "next", None).await);
    assert_ne!(run_2, run_1);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(starts(&records), [run_1.clone(), run_2.clone()]);
    rig.wait_idle().await;
    // The new episode's first model call carried the steer in its prefix.
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert!(prompts[1]
        .iter()
        .any(|message| last_text(std::slice::from_ref(message)) == "hurry"));

    let delivered = rig.send("steer-1", "hurry", Some("steer")).await;
    let pinned = std::fs::read_to_string(golden_path("send_reply.steer_retry_turn.json"))
        .unwrap()
        .replace("@NEXT_RUN_ID@", &run_2)
        .replace("@RUN_ID@", &run_1);
    assert_eq!(String::from_utf8_lossy(&delivered.body), pinned);
    let again = rig.send("steer-1", "hurry", Some("steer")).await;
    assert_eq!(again.body, delivered.body, "a receipt never changes");
    rig.finish().await;
}

#[tokio::test]
async fn cache_none_with_automatic_warming_refuses_before_any_write() {
    let mut rig = Rig::start("cache-none-auto").await;
    for provider in [
        "anthropic",
        "openai",
        "google",
        "bedrock",
        "gateway",
        "fake",
    ] {
        let refused = rig
            .send_with(
                "refused",
                "hello",
                None,
                serde_json::json!({
                    "model": {"provider": provider, "model": "any"},
                    "cache": {"mode": "none"}, "keep_warm": "auto"
                }),
            )
            .await;
        assert_golden(&refused, "invalid_params.cache_none_keep_warm.json", "");
        assert!(
            rig.wal_bytes().is_empty(),
            "{provider}: refusal must not write WAL"
        );
        assert!(
            rig.script.prompts.lock().unwrap().is_empty(),
            "no provider call"
        );
    }
    rig.finish().await;
}

async fn invalid_retention_writes_nothing(tag: &str, delivery: Option<&str>) {
    let mut rig = Rig::start(tag).await;
    let values = [
        serde_json::json!(0),
        serde_json::json!(2592001),
        serde_json::json!(u64::MAX),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("forever"),
        serde_json::json!(false),
        serde_json::json!({}),
        serde_json::json!([]),
    ];
    for retention in &values {
        let refused = rig
            .send_with(
                "refused",
                "hi",
                delivery,
                serde_json::json!({"retention": retention}),
            )
            .await;
        assert_eq!(
            refused.header.ty,
            FrameType::Error,
            "{delivery:?} {retention}"
        );
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "invalid_params", "{delivery:?} {retention}");
        assert_eq!(
            body["detail"]["field"], "retention",
            "{delivery:?} {retention}"
        );
        assert!(
            rig.wal_bytes().is_empty(),
            "a refused first send writes nothing"
        );
    }
    assert!(
        rig.script.prompts.lock().unwrap().is_empty(),
        "no provider call"
    );

    // A send without retention admits a non-expiring session. Invalid policies and later
    // attempts to add retention must not queue, steer or interrupt its accepted run.
    // Holding the model response prevents the run from appending records during these checks.
    rig.script.hold_model(true);
    let accepted = rig.send("accepted", "hi", None).await;
    assert_eq!(accepted.header.ty, FrameType::Response);
    rig.wait_entered().await;
    let before = rig.wal_bytes();
    assert!(
        !before.is_empty(),
        "the accepted run must reach its real WAL"
    );
    for retention in values
        .iter()
        .chain([serde_json::json!(1), serde_json::json!(3600)].iter())
    {
        let refused = rig
            .send_with(
                "refused",
                "more",
                delivery,
                serde_json::json!({"retention": retention}),
            )
            .await;
        assert_eq!(
            refused.header.ty,
            FrameType::Error,
            "{delivery:?} {retention}"
        );
        let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(body["code"], "invalid_params", "{delivery:?} {retention}");
        assert_eq!(
            body["detail"]["field"], "retention",
            "{delivery:?} {retention}"
        );
        assert_eq!(
            rig.wal_bytes(),
            before,
            "a refused later send writes nothing"
        );
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

#[tokio::test]
async fn invalid_retention_on_default_delivery_is_refused_before_any_wal_write() {
    invalid_retention_writes_nothing("retention-default", None).await;
}

#[tokio::test]
async fn invalid_retention_on_queue_delivery_is_refused_before_any_wal_write() {
    invalid_retention_writes_nothing("retention-queue", Some("queue")).await;
}

#[tokio::test]
async fn invalid_retention_on_steer_delivery_is_refused_before_any_wal_write() {
    invalid_retention_writes_nothing("retention-steer", Some("steer")).await;
}

#[tokio::test]
async fn invalid_retention_on_interrupt_delivery_is_refused_before_any_wal_write() {
    invalid_retention_writes_nothing("retention-interrupt", Some("interrupt")).await;
}

async fn restart_paused_rig(tag: &str) -> (Rig, String) {
    let mut rig = Rig::start(tag).await;
    rig.script.push(&[Step::Call("restart-call")]);
    rig.script.hold_tool(true);
    let run_id = run_id_of(
        &rig.send_with(
            "restart-turn",
            "task",
            None,
            serde_json::json!({"on_restart": "resume"}),
        )
        .await,
    );
    rig.wait_entered().await;
    rig = rig.restart_at_tool_boundary().await;
    assert!(matches!(
        rig.records().last(),
        Some(WalRecord::RunPaused {
            reason: broca_protocol::PauseReason::Restart,
            ..
        })
    ));
    assert_eq!(starts(&rig.records()), std::slice::from_ref(&run_id));
    (rig, run_id)
}

fn assert_restart_paused_golden(frame: &Frame, file: &str, run_id: &str, sha256: &str) {
    use sha2::{Digest, Sha256};
    assert_golden(frame, file, run_id);
    assert_eq!(
        format!(
            "{:x}",
            Sha256::digest(std::fs::read(golden_path(file)).unwrap())
        ),
        sha256,
        "golden file {file} changed"
    );
}

/// A restart-paused steer is durable before its paused reply, and renders only on boot resume.
#[tokio::test]
async fn restart_paused_steer_reply_bytes_are_pinned_and_render_on_resume() {
    let (mut rig, run_id) = restart_paused_rig("restart-steer-golden").await;
    let steer = rig
        .send("restart-steer", "restart steer", Some("steer"))
        .await;
    assert_restart_paused_golden(
        &steer,
        "send_reply.paused_restart.steer.json",
        &run_id,
        "d83d96f442423a7f32c1071514b331252b9d4fb167e05253a2ada82e99612de5",
    );
    let records = rig.records();
    assert!(records.iter().any(|record| matches!(
        record,
        WalRecord::Steered { send_id: Some(send_id), delivery: Some(broca_protocol::Delivery::Steer), input, .. }
            if send_id == "restart-steer" && input.iter().any(|message| last_text(std::slice::from_ref(message)) == "restart steer")
    )), "the exact steer is durable before its reply");
    assert_eq!(
        starts(&records),
        std::slice::from_ref(&run_id),
        "the steer starts no run"
    );

    rig.script.push(&[Step::Text("resumed")]);
    assert_eq!(
        rig.registry()
            .resume_restart_pauses(
                std::sync::Arc::new(broca_core::FileWalStore::new(rig.dir.join("wal"))),
                vec![(run_id.clone(), rig.session.clone())],
            )
            .await,
        (1, 0)
    );
    let records = rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    assert_eq!(endings(&records), ["paused", "completed"]);
    assert_eq!(starts(&records), std::slice::from_ref(&run_id));
    assert_eq!(
        last_text(&rig.script.prompts.lock().unwrap()[0]),
        "restart steer"
    );
    assert_eq!(
        rig.transcript().await,
        [
            pair("user", "task"),
            pair("assistant", "tool_call"),
            pair("tool", "tool_result"),
            pair("user", "restart steer"),
            pair("assistant", "resumed"),
        ]
    );
    rig.finish().await;
}

/// Retrying the paused run's own key leaves it paused, without a baseline or a delivery receipt.
#[tokio::test]
async fn restart_paused_resend_reply_bytes_are_pinned_and_start_no_run() {
    let (mut rig, run_id) = restart_paused_rig("restart-resend-golden").await;
    let before = rig.wal_bytes();
    let resend = rig
        .send_with(
            "restart-turn",
            "task",
            None,
            serde_json::json!({"on_restart": "resume"}),
        )
        .await;
    assert_restart_paused_golden(
        &resend,
        "send_reply.paused_restart.resend.json",
        &run_id,
        "d83d96f442423a7f32c1071514b331252b9d4fb167e05253a2ada82e99612de5",
    );
    assert_eq!(rig.wal_bytes(), before, "the resend writes nothing");
    assert_eq!(
        starts(&rig.records()),
        std::slice::from_ref(&run_id),
        "the resend starts no run"
    );
    rig.finish().await;
}

/// Each record a send wrote, by kind, with its optional title.
fn titles(records: &[WalRecord]) -> Vec<(&'static str, Option<String>)> {
    records
        .iter()
        .filter_map(|record| {
            let kind = match record {
                WalRecord::RunStarted { .. } => "run_started",
                WalRecord::Queued { .. } => "queued",
                WalRecord::Steered { .. } => "steered",
                _ => return None,
            };
            Some((kind, record.title().map(str::to_owned)))
        })
        .collect()
}

/// A reply's body as text, so a mismatch reads as the two replies rather than two byte lists.
fn body_text(frame: &Frame) -> String {
    String::from_utf8_lossy(&frame.body).into_owned()
}

/// `run.status` for `run_id`.
async fn run_status(rig: &mut Rig, run_id: &str) -> Frame {
    rig.request(
        7,
        serde_json::json!({"method": "run.status", "params": {"run_id": run_id}}),
    )
    .await
}

/// The `title` member of the session's `session.head`, absent as `None`.
async fn head_title(rig: &mut Rig) -> Option<String> {
    let head = result(
        &rig.request(
            7,
            serde_json::json!({"method": "session.head", "params": {}}),
        )
        .await,
    );
    assert_ne!(head["title"], serde_json::json!(""), "never an empty title");
    head.get("title")
        .map(|title| title.as_str().unwrap().to_owned())
}

/// The title the run index row of `run_id` holds, read from the rig's store file.
fn indexed_title(rig: &Rig, run_id: &str) -> Option<String> {
    use broca_core::RunIndex;
    broca_store::SqliteStore::open(rig.dir.join("store.db"))
        .unwrap()
        .lookup_run(run_id)
        .unwrap()
        .expect("the run has an index row")
        .title
}

/// A titled send, through the real serve path: the title is recorded on the `RunStarted` that
/// opens its run and in the run's index row, and `run.status` (while the run is live and once
/// it has completed, with the completed reply compared with its golden fixture) and `session.head` report it. A
/// later queued send with a new title is recorded on its `Queued` record, `session.head`
/// switches to it at once, and the run that drains it records it too. Both survive a restart
/// onto a new registry and store. A send without a title leaves every one of these absent.
#[tokio::test]
async fn a_titled_send_is_recorded_on_its_run_and_reported_by_status_and_head() {
    let mut rig = Rig::start_with_idle_ttl("title-run", Duration::from_millis(100)).await;
    assert_eq!(head_title(&mut rig).await, None);
    rig.script.hold_model(true);
    let run_1 = run_id_of(
        &rig.send_with(
            "turn-1",
            "hi",
            None,
            serde_json::json!({"title": "Fix the login bug"}),
        )
        .await,
    );
    rig.wait_entered().await;
    let live = result(&run_status(&mut rig, &run_1).await);
    assert_eq!(live["state"], "active");
    assert_eq!(live["title"], "Fix the login bug");
    assert_eq!(
        head_title(&mut rig).await.as_deref(),
        Some("Fix the login bug")
    );
    assert_eq!(
        indexed_title(&rig, &run_1).as_deref(),
        Some("Fix the login bug")
    );

    let run_2 = pending_run_id(
        &rig.send_with(
            "turn-2",
            "next",
            None,
            serde_json::json!({"title": "Write the release notes"}),
        )
        .await,
    );
    assert_eq!(
        titles(&rig.records()),
        [
            ("run_started", Some("Fix the login bug".to_owned())),
            ("queued", Some("Write the release notes".to_owned())),
        ]
    );
    assert_eq!(
        head_title(&mut rig).await.as_deref(),
        Some("Write the release notes"),
        "the newest title wins as soon as it is durable"
    );

    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(starts(&records), [run_1.clone(), run_2.clone()]);
    assert_eq!(
        titles(&records),
        [
            ("run_started", Some("Fix the login bug".to_owned())),
            ("queued", Some("Write the release notes".to_owned())),
            ("run_started", Some("Write the release notes".to_owned())),
        ]
    );
    rig.wait_idle().await;
    assert_golden(
        &run_status(&mut rig, &run_1).await,
        "run_status.completed_title.json",
        &run_1,
    );
    assert_eq!(
        result(&run_status(&mut rig, &run_2).await)["title"],
        "Write the release notes"
    );
    assert_eq!(
        indexed_title(&rig, &run_2).as_deref(),
        Some("Write the release notes")
    );
    assert_eq!(
        head_title(&mut rig).await.as_deref(),
        Some("Write the release notes")
    );

    // A later send without a title changes nothing: the head keeps the newest recorded title
    // and the untitled run reports none.
    let run_3 = run_id_of(&rig.send("turn-3", "plain", None).await);
    rig.wait_for_finished(3).await;
    rig.wait_idle().await;
    assert!(result(&run_status(&mut rig, &run_3).await)
        .get("title")
        .is_none());
    assert_eq!(indexed_title(&rig, &run_3), None);
    assert_eq!(
        head_title(&mut rig).await.as_deref(),
        Some("Write the release notes")
    );
    // The store's projection holds it now, past every titled record: the head no longer needs
    // the WAL tail for it.
    let projected = || {
        use broca_core::MaterializedStore;
        broca_store::SqliteStore::open(rig.dir.join("store.db"))
            .unwrap()
            .read_page_with_watermark(&rig.session, None, 1, broca_core::context::ReadView::Raw)
            .unwrap()
    };
    broca_test_support::wait_for(
        "title projected through WAL head",
        async {
            loop {
                if projected().watermark_seq as usize >= rig.records().len() - 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || {
            format!(
                "watermark={}, records={}",
                projected().watermark_seq,
                rig.records().len()
            )
        },
    )
    .await;
    let page = projected();
    assert!(
        page.watermark_seq as usize >= rig.records().len() - 1,
        "projected"
    );
    assert_eq!(page.last_title.as_deref(), Some("Write the release notes"));

    rig.wait_closed().await;
    rig = rig.restart().await;
    assert_eq!(
        result(&run_status(&mut rig, &run_1).await)["title"],
        "Fix the login bug"
    );
    assert_eq!(
        head_title(&mut rig).await.as_deref(),
        Some("Write the release notes")
    );
    rig.finish().await;
}

/// The title the `run_started` record of `run_id` carries, read from the session's WAL.
fn started_title(records: &[WalRecord], run_id: &str) -> Option<String> {
    records
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                run_id: started,
                title,
                ..
            } if started.0 == run_id => Some(title.clone()),
            _ => None,
        })
        .expect("the run has a run_started record")
}

/// An ordered send's title reaches the index row of the run it starts, on both ordered start
/// paths. An `["idle_start"]` send on a fresh, idle session starts its run while it is being
/// admitted; the same send made while another run is active waits in the queue and starts its
/// run once that run ends. Each run's index row holds exactly the title its `run_started`
/// records. (Direct sends and plain queued sends are covered by the titled-send test above.)
#[tokio::test]
async fn an_ordered_sends_title_reaches_the_index_row_of_the_run_it_starts() {
    let mut rig = Rig::start("title-ordered").await;
    let idle_start = |title: &str| serde_json::json!({"delivery": ["idle_start"], "title": title});
    rig.script.hold_model(true);
    let immediate = run_id_of(
        &rig.send_with("ordered-1", "hi", None, idle_start("Ordered on idle"))
            .await,
    );
    rig.wait_entered().await;
    assert_eq!(
        started_title(&rig.records(), &immediate).as_deref(),
        Some("Ordered on idle")
    );
    assert_eq!(
        indexed_title(&rig, &immediate).as_deref(),
        Some("Ordered on idle"),
        "the run an idle_start send starts at once indexes its title"
    );

    let drained = pending_run_id(
        &rig.send_with(
            "ordered-2",
            "next",
            None,
            idle_start("Ordered from the queue"),
        )
        .await,
    );
    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    assert_eq!(starts(&records), [immediate.clone(), drained.clone()]);
    assert_eq!(
        started_title(&records, &drained).as_deref(),
        Some("Ordered from the queue")
    );
    rig.wait_idle().await;
    assert_eq!(
        indexed_title(&rig, &drained).as_deref(),
        Some("Ordered from the queue"),
        "the run a queued idle_start send starts indexes its title"
    );
    assert_eq!(
        indexed_title(&rig, &immediate).as_deref(),
        Some("Ordered on idle"),
        "finishing the run keeps its title"
    );
    rig.finish().await;
}

/// A steer's title is stored on its `Steered` record and becomes the session's newest title;
/// a retry of the steer with another title is answered as the first one was, writes nothing,
/// and keeps the first title.
#[tokio::test]
async fn a_steers_title_is_stored_on_its_steered_record() {
    let mut rig = Rig::start("title-steer").await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("written up")]);
    rig.script.hold_tool(true);
    let run_1 = run_id_of(
        &rig.send_with("turn-1", "task", None, serde_json::json!({"title": "Task"}))
            .await,
    );
    rig.wait_entered().await;
    let steered = rig
        .send_with(
            "steer-1",
            "write up now",
            Some("steer"),
            serde_json::json!({"title": "Write it up"}),
        )
        .await;
    assert_golden(&steered, "send_reply.steer_joined.json", &run_1);
    assert_eq!(
        titles(&rig.records()),
        [
            ("run_started", Some("Task".to_owned())),
            ("steered", Some("Write it up".to_owned())),
        ]
    );
    assert_eq!(head_title(&mut rig).await.as_deref(), Some("Write it up"));
    let before = rig.wal_bytes();
    let retry = rig
        .send_with(
            "steer-1",
            "write up now",
            Some("steer"),
            serde_json::json!({"title": "Another title"}),
        )
        .await;
    assert_eq!(
        body_text(&retry),
        body_text(&steered),
        "the recorded answer"
    );
    assert_eq!(rig.wal_bytes(), before, "a retry writes nothing");
    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    assert_eq!(head_title(&mut rig).await.as_deref(), Some("Write it up"));
    // The steer joined the run: the run's own title is still its start's.
    assert_eq!(result(&run_status(&mut rig, &run_1).await)["title"], "Task");
    rig.finish().await;
}

/// Every title the contract refuses is refused as `invalid_params` naming `title` (the first
/// refusal compared with its golden fixture), on every delivery mode, before anything is written: the WAL stays
/// empty on an unwritten session and byte-identical on one with a run in flight. The longest
/// accepted title, 200 two-byte characters, is admitted whole.
#[tokio::test]
async fn an_invalid_title_is_refused_before_any_wal_write() {
    let invalid = [
        serde_json::json!(""),
        serde_json::json!("   "),
        serde_json::json!("line one\nline two"),
        serde_json::json!("tab\there"),
        serde_json::json!("bell\u{7}"),
        serde_json::json!("x".repeat(201)),
        serde_json::json!("é".repeat(201)),
        serde_json::Value::Null,
        serde_json::json!(7),
    ];
    let mut rig = Rig::start("title-invalid").await;
    let refused = rig
        .send_with("turn", "hi", None, serde_json::json!({"title": ""}))
        .await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_golden(&refused, "invalid_params.title.json", "");
    for delivery in [None, Some("queue"), Some("steer"), Some("interrupt")] {
        for title in &invalid {
            let refused = rig
                .send_with("turn", "hi", delivery, serde_json::json!({"title": title}))
                .await;
            assert_eq!(refused.header.ty, FrameType::Error, "{delivery:?} {title}");
            let body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
            assert_eq!(body["code"], "invalid_params", "{delivery:?} {title}");
            assert_eq!(body["detail"]["field"], "title", "{delivery:?} {title}");
            assert!(
                rig.wal_bytes().is_empty(),
                "{delivery:?} {title}: nothing written"
            );
        }
    }
    assert!(
        rig.script.prompts.lock().unwrap().is_empty(),
        "no provider call"
    );

    rig.script.hold_model(true);
    let longest = "é".repeat(200);
    let run_1 = run_id_of(
        &rig.send_with("turn", "hi", None, serde_json::json!({"title": longest}))
            .await,
    );
    rig.wait_entered().await;
    assert_eq!(
        titles(&rig.records()),
        [("run_started", Some(longest.clone()))]
    );
    let before = rig.wal_bytes();
    for delivery in [None, Some("steer"), Some("interrupt")] {
        for title in &invalid {
            let refused = rig
                .send_with(
                    "other",
                    "more",
                    delivery,
                    serde_json::json!({"title": title}),
                )
                .await;
            assert_eq!(refused.header.ty, FrameType::Error, "{delivery:?} {title}");
            assert_eq!(
                rig.wal_bytes(),
                before,
                "{delivery:?} {title}: nothing written"
            );
        }
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    assert_eq!(
        result(&run_status(&mut rig, &run_1).await)["title"],
        longest
    );
    rig.finish().await;
}

/// A title is not part of a send's identity. A keyed resend with an edited title is answered
/// with the recorded answer (`active` while its run lives, `pending` while it is queued,
/// `finished` once it has ended), is never refused `send_id_reuse`, writes nothing, and the
/// first title stays on the record, in the run index, in `run.status` and in `session.head`.
#[tokio::test]
async fn a_keyed_resend_with_an_edited_title_gets_the_recorded_answer_and_keeps_the_first_title() {
    let mut rig = Rig::start("title-resend").await;
    rig.script.hold_model(true);
    let first = rig
        .send_with("turn-1", "hi", None, serde_json::json!({"title": "First"}))
        .await;
    let run_1 = run_id_of(&first);
    rig.wait_entered().await;
    let before = rig.wal_bytes();
    let live_retry = rig
        .send_with("turn-1", "hi", None, serde_json::json!({"title": "Edited"}))
        .await;
    assert_eq!(
        body_text(&live_retry),
        body_text(&first),
        "the recorded answer, not send_id_reuse"
    );
    assert_eq!(rig.wal_bytes(), before, "a retry writes nothing");

    let queued = rig
        .send_with(
            "turn-2",
            "next",
            None,
            serde_json::json!({"title": "Queued first"}),
        )
        .await;
    let run_2 = pending_run_id(&queued);
    let before = rig.wal_bytes();
    let queued_retry = rig
        .send_with(
            "turn-2",
            "next",
            None,
            serde_json::json!({"title": "Queued edited"}),
        )
        .await;
    assert_eq!(body_text(&queued_retry), body_text(&queued));
    assert_eq!(rig.wal_bytes(), before);
    let untitled_retry = rig.send("turn-2", "next", None).await;
    assert_eq!(
        body_text(&untitled_retry),
        body_text(&queued),
        "dropping the title is no change either"
    );

    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    let finished = rig
        .send_with(
            "turn-1",
            "hi",
            None,
            serde_json::json!({"title": "Edited again"}),
        )
        .await;
    assert_golden_admitting(&finished, "send_reply.finished.json", &run_1);
    assert_eq!(rig.wal_bytes(), before);
    assert_eq!(
        titles(&rig.records()),
        [
            ("run_started", Some("First".to_owned())),
            ("queued", Some("Queued first".to_owned())),
            ("run_started", Some("Queued first".to_owned())),
        ]
    );
    assert_eq!(indexed_title(&rig, &run_1).as_deref(), Some("First"));
    assert_eq!(indexed_title(&rig, &run_2).as_deref(), Some("Queued first"));
    assert_eq!(
        result(&run_status(&mut rig, &run_1).await)["title"],
        "First"
    );
    assert_eq!(head_title(&mut rig).await.as_deref(), Some("Queued first"));
    rig.finish().await;
}

/// The run index is written only at admission, so a row written before titles existed has no
/// title. `run.status` then answers from the run's `RunStarted` in the WAL. Simulated by
/// clearing the column in the store file; `session.head` is rebuilt the same way, from the WAL,
/// when the store is deleted outright.
#[tokio::test]
async fn status_and_head_fall_back_to_the_wal_when_the_store_has_no_title() {
    let mut rig = Rig::start_with_idle_ttl("title-fallback", Duration::from_millis(100)).await;
    let run_1 = run_id_of(
        &rig.send_with(
            "turn-1",
            "hi",
            None,
            serde_json::json!({"title": "From the WAL"}),
        )
        .await,
    );
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;
    let store = rig.dir.join("store.db");
    rusqlite::Connection::open(&store)
        .unwrap()
        .execute("UPDATE run_index SET title = NULL", [])
        .unwrap();
    rig = rig.restart().await;
    assert_eq!(indexed_title(&rig, &run_1), None);
    assert_eq!(
        result(&run_status(&mut rig, &run_1).await)["title"],
        "From the WAL"
    );
    rig.wait_closed().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(rig.dir.join(format!("store.db{suffix}")));
    }
    rig = rig.restart().await;
    assert_eq!(head_title(&mut rig).await.as_deref(), Some("From the WAL"));
    rig.finish().await;
}

#[tokio::test]
async fn invalid_run_limits_are_refused_by_field_without_writes() {
    let mut rig = Rig::start("invalid-run-limits").await;
    for field in ["stop_when.max_steps", "generation.max_output_tokens"] {
        let (group, member) = field.split_once('.').unwrap();
        for (index, value) in [
            serde_json::json!(0),
            serde_json::json!(1.5),
            serde_json::json!(-1),
            serde_json::Value::Null,
            serde_json::json!("2"),
            serde_json::json!(1e30),
        ]
        .into_iter()
        .enumerate()
        {
            let before = rig.wal_bytes();
            let mut extra = serde_json::json!({});
            extra[group] = serde_json::json!({member: value});
            let frame = rig
                .send_with(&format!("{group}-{index}"), "invalid", None, extra)
                .await;
            let error: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(error["code"], "invalid_params", "{field}: {error}");
            assert_eq!(error["detail"]["field"], field, "{error}");
            assert_eq!(rig.wal_bytes(), before);
        }
    }
    rig.finish().await;
}
