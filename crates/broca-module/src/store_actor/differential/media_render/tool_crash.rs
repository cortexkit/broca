//! A crash while a planned session's tool call runs, recovered in the session store.
//!
//! The process dies with the call dispatched and no result recorded. A new process seals the
//! run as the crash's orphan, answering the call as never run, and the session's next run sends
//! a request holding that answer, pinned. On a session whose plan names a compaction provider
//! that request numbers the parts of every message, so it shows how the recovered tool turn is
//! numbered.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use broca_protocol::content::{AssistantMessage, ContentBlock};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::{FinishReason, MessageId, Usage};
use broca_session_store::keys::Cipher;
use serde_json::json;

use super::super::controls::{compacting_plan, Compactor};
use super::{planned, registry, send_planned, text, text_step, tool_hold, Rendered, Sent};
use crate::actor::SendOutcome;

cipher_tests!(a_crash_while_a_planned_tool_call_runs_is_recovered_alike);

/// A step that calls the `wait` tool, whose call waits on the hold `hold`.
fn wait_step(id: &str, hold: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: broca_protocol::ToolCallId("c1".into()),
                    tool_name: "wait".into(),
                    input: json!({ "hold": hold }).into(),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A plan naming the compaction provider, whose runs may call the `wait` tool.
fn waiting_plan() -> Arc<broca_records::record::PlanManifest> {
    let mut plan = (*compacting_plan()).clone();
    plan.tools = Arc::new(vec![serde_json::from_value(json!({
        "name": "wait",
        "description": "waits",
        "input_schema": {"type": "object"},
    }))
    .unwrap()]);
    Arc::new(plan)
}

/// Copies the state root `from` to `to` as a crash leaves it: every file but the leases, which
/// name a process that no longer runs.
fn copy_state(from: &std::path::Path, to: &std::path::Path) {
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        if std::fs::metadata(from).unwrap().is_dir() {
            std::fs::create_dir_all(to).unwrap();
            for entry in std::fs::read_dir(from).unwrap() {
                let entry = entry.unwrap();
                copy(&entry.path(), &to.join(entry.file_name()));
            }
        } else {
            std::fs::copy(from, to).unwrap();
        }
    }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "leases" {
            copy(&entry.path(), &to.join(entry.file_name()));
        }
    }
}

/// A planned session's run calls a tool, and the process crashes while the call runs. On both
/// storages the new process seals the run as the crash's orphan without calling the model, and
/// the next run sends the same request.
async fn a_crash_while_a_planned_tool_call_runs_is_recovered_alike(cipher: Cipher) {
    let plan = waiting_plan();
    // Both sides' calls name the same hold, so their requests carry the same input.
    let name = format!("planned-crash-{cipher:?}");
    let hold = tool_hold(&name);
    let mut sides = Vec::new();
    for store in [true] {
        let reached = hold.reached.load(Ordering::SeqCst);
        let mut side = Rendered::with_compactor(
            store,
            vec![wait_step("a1", &name), text_step("a2", "done")],
            false,
            Some(Arc::new(Compactor::default())),
            cipher,
        )
        .await;
        let reply = send_planned(&side, "r1", planned(text("first")), &plan).await;
        assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
        broca_test_support::wait_until("the tool call runs", || async {
            if hold.reached.load(Ordering::SeqCst) > reached {
                Ok(())
            } else {
                Err("not yet".to_owned())
            }
        })
        .await;
        // The crash: the state root as it lies while the call runs.
        let crashed = side.side.dir.with_extension("crashed");
        let _ = std::fs::remove_dir_all(&crashed);
        copy_state(&side.side.dir, &crashed);
        hold.released.notify_one();
        side.side.idle().await;
        assert!(side.side.registry.stop_and_seal().await);

        // A new process starts on what the crash left, and recovers the run it finds
        // unfinished, as a starting process does.
        std::fs::remove_dir_all(&side.side.dir).unwrap();
        copy_state(&crashed, &side.side.dir);
        std::fs::remove_dir_all(&crashed).unwrap();
        side.sent = Sent::default();
        side.side.registry = registry(
            &side.side.dir,
            None,
            vec![text_step("b1", "after")],
            None,
            &side.sent,
            Some(Arc::new(Compactor::default())),
            cipher,
        )
        .await;
        let sweep = side
            .side
            .registry
            .recover_store_crash_orphans()
            .await
            .unwrap();
        assert_eq!((sweep.recovered, sweep.failed.len()), (1, 0), "{sweep:?}");
        assert_eq!(
            sweep.opened_for,
            [("run_interrupted", 1)].into_iter().collect(),
            "the sweep says it opened the session for its run cut with a call out: {sweep:?}"
        );
        side.side.idle().await;
        assert!(
            side.sent().is_empty(),
            "sealing the orphan must not call the model"
        );
        let reply = send_planned(&side, "r2", planned(text("after the crash")), &plan).await;
        assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
        side.side.idle().await;
        sides.push(side);
    }
    let store = sides.pop().unwrap();
    let new = store.sent();
    assert_eq!(new.len(), 1, "the next run sent its request");
    let ended = {
        let runtime = store.side.registry.store_runtime().unwrap();
        let view = runtime
            .current_view(&store.side.key)
            .await
            .unwrap()
            .unwrap();
        view.operations.run.as_ref().map(|run| {
            format!(
                "run {}: {:?}, {:?}",
                run.run_id, run.finished, run.end_error
            )
        })
    };
    assert!(new[0].is_ok(), "the store's latest {ended:?}: {new:?}");
    super::sent_pinned(&new, "the next run after the crash");
    assert!(store.side.registry.stop_and_seal().await);
}
