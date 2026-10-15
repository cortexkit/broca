//! Hook admission, durable outputs and replay over real ck-subc role routes.
use crate::delivery_rig::{result, Rig, Step};
use crate::step_transform_rig::*;
use broca_protocol::content::{ContentBlock, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ToolCallId;
use broca_wal::WalRecord;
use cortexkit_role_step_transform_provider::answer::{HookAnswer, Operation};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct TimedHook {
    inner: broca_subc::SubcHookPlane,
    elapsed: Arc<Mutex<Vec<Duration>>>,
}
#[async_trait::async_trait]
impl broca_core::HookPlane for TimedHook {
    async fn hook(
        &self,
        provider: &str,
        bind: &broca_protocol::BindIdentity,
        call: broca_core::hooks::HookCall,
    ) -> Result<HookAnswer, broca_core::HookUnavailable> {
        struct Elapsed<'a>(tokio::time::Instant, &'a Mutex<Vec<Duration>>);
        impl Drop for Elapsed<'_> {
            fn drop(&mut self) {
                self.1.lock().unwrap().push(self.0.elapsed());
            }
        }
        // The future is dropped by core's deadline. Measuring its lifetime includes route
        // discovery and open, but excludes admission and the next model step.
        let _elapsed = Elapsed(tokio::time::Instant::now(), &self.elapsed);
        self.inner.hook(provider, bind, call).await
    }
}

fn answer(value: HookAnswer) -> Value {
    serde_json::to_value(value).unwrap()
}
fn call(id: &str, input: Value) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "work".into(),
        input,
        thought_signature: Some("tool-signature".into()),
    })
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}
async fn start(tag: &str, subs: Vec<Value>) -> (Rig, Daemon, Arc<State>, Value) {
    let rig = Rig::start_as(tag, "hook-caller").await;
    let state = State::new(subs);
    let daemon = Daemon::start(&rig.dir, state.clone()).await;
    daemon.install(&rig).await;
    let plan = plan(&state.subscriptions, false);
    (rig, daemon, state, plan)
}
async fn finish(rig: Rig, daemon: Daemon) {
    daemon.finish().await;
    rig.finish().await;
}
fn values(records: &[WalRecord]) -> Vec<Value> {
    records
        .iter()
        .map(|r| serde_json::to_value(r).unwrap())
        .collect()
}
fn only<'a>(records: &'a [Value], kind: &str) -> &'a Value {
    let matches: Vec<_> = records.iter().filter(|r| r["type"] == kind).collect();
    assert_eq!(matches.len(), 1, "{kind}: {records:?}");
    matches[0]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_assistant_routes_text_only_and_publishes_final_values() {
    let (mut rig, daemon, state, plan) = start(
        "post-assistant-live",
        vec![subscription("post_assistant", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|call| {
        if call["step_id"] == "1" {
            append("-appended")
        } else {
            answer(HookAnswer::Pass)
        }
    });
    let signed = ContentBlock::Reasoning {
        text: "thinking".into(),
        signature: Some("thinking-signature".into()),
    };
    let tool = call("c", json!({"value":"model"}));
    rig.script.push(&[
        Step::Stream(vec![
            signed.clone(),
            text("first"),
            tool.clone(),
            text("second"),
        ]),
        Step::Text("done"),
    ]);
    // Keep the provider waiting until the display subscription is open.
    rig.script.hold_model(true);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    // Release the held model only once the subscription is attached: the display lane is lossy.
    let script = rig.script.clone();
    let frames = rig
        .subscribe_collect_after_attach(
            8,
            json!({"from": "start"}),
            Duration::from_millis(500),
            move || script.hold_model(false),
        )
        .await;
    let records = rig.wait_for_finished(1).await;
    let message = records
        .iter()
        .find_map(|r| match r {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => Some(assistant_message),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        message.content,
        vec![signed, text("first-appended"), tool, text("second")]
    );
    let calls = state.hooks();
    assert_eq!(calls[0]["blocks"], json!(["first", "second"]));
    assert_eq!(calls[0]["harness"], "hook-caller");
    assert_ne!(
        calls[0]["harness"],
        state.binds.lock().unwrap()[0]["harness"]
    );
    assert_eq!(calls[0]["preset"], "worker");
    assert_eq!(calls[0]["params"], json!({"verbatim":[null,{"x":1}]}));
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert!(prompts[1].iter().any(|m| m.content == message.content));
    let page = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert!(page.to_string().contains("first-appended"));
    let bodies: Vec<Value> = frames
        .iter()
        .filter_map(|f| serde_json::from_slice(&f.body).ok())
        .collect();
    let encoded = serde_json::to_string(&bodies).unwrap();
    assert!(
        encoded.contains("first-appended"),
        "control subscriber must read final text: {encoded}"
    );
    let displays: Vec<_> = bodies
        .iter()
        .filter(|v| v.to_string().contains("display"))
        .collect();
    let encoded_display = serde_json::to_string(&displays).unwrap();
    let reset = encoded_display
        .find("display_reset")
        .expect("changed streamed message must reset display");
    assert!(encoded_display[..reset].contains("first"));
    assert!(encoded_display[reset..].contains("first-appended"));
    assert_eq!(encoded_display.matches("display_reset").count(), 1);
    assert!(
        records
            .iter()
            .rfind(|r| matches!(r, WalRecord::ModelStepFinished { .. }))
            .is_some_and(|r| serde_json::to_value(r).unwrap().get("hooks").is_none()),
        "all-pass step carries no hooks"
    );
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_tool_mutation_is_dispatched_but_original_is_rendered() {
    let (mut rig, daemon, state, plan) = start(
        "mutate-live",
        vec![
            subscription("pre_tool", Some("mutate"), "pass", 1000),
            subscription("pre_tool", Some("validate"), "refuse", 1000),
        ],
    )
    .await;
    state.answer_with(|c| {
        if c["phase"] == "mutate" {
            answer(HookAnswer::Mutate {
                input: json!({"value":"changed"}),
                note: Some("rewrite".into()),
            })
        } else {
            assert_eq!(c["input"], json!({"value":"changed"}));
            answer(HookAnswer::Pass)
        }
    });
    rig.script.push(&[
        Step::Stream(vec![call("c", json!({"value":"model"}))]),
        Step::Text("done"),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = values(&rig.wait_for_finished(1).await);
    let intent = only(&records, "tool_dispatch_intent");
    assert_eq!(intent["args"], json!({"value":"changed"}));
    assert_eq!(intent["hooks"]["original"], json!({"value":"model"}));
    assert_eq!(intent["hooks"]["ops"][0]["phase"], "mutate");
    assert_eq!(
        *rig.script.dispatched_inputs.lock().unwrap(),
        vec![json!({"value":"changed"})]
    );
    let prompts = rig.script.prompts.lock().unwrap().clone();
    assert!(prompts[1].iter().flat_map(|m| &m.content).any(|b| matches!(b,ContentBlock::ToolCall(c) if c.input == json!({"value":"model"}) && c.thought_signature.as_deref()==Some("tool-signature"))));
    assert_eq!(
        state.counts(),
        [
            ("pre_tool:mutate".into(), 1),
            ("pre_tool:validate".into(), 1)
        ]
        .into_iter()
        .collect()
    );
    for c in state.hooks() {
        assert!(c.get("call_key").is_none());
    }
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_mutation_has_one_mutate_no_validate_and_hookless_replay() {
    let (mut rig, daemon, state, plan) = start(
        "invalid-mutate-live",
        vec![
            subscription("pre_tool", Some("mutate"), "pass", 1000),
            subscription("pre_tool", Some("validate"), "pass", 1000),
        ],
    )
    .await;
    *state.schema.lock().unwrap() =
        json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]});
    state.answer_with(|_| {
        answer(HookAnswer::Mutate {
            input: json!({"value":"x".repeat(1024*1024+1)}),
            note: None,
        })
    });
    rig.script.push(&[
        Step::Stream(vec![call("c", json!({"value":"model"}))]),
        Step::Text("done"),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = values(&rig.wait_for_finished(1).await);
    let terminal = only(&records, "tool_no_dispatch");
    assert_eq!(terminal["reason"]["kind"], "validation_failed");
    assert_eq!(terminal["hooks"]["original"], json!({"value":"model"}));
    assert_eq!(
        terminal["hooks"]["final"]["value"].as_str().unwrap().len(),
        1024 * 1024 + 1
    );
    assert_eq!(terminal["hooks"]["ops"][0]["op"], "mutate");
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(
        state.counts(),
        [("pre_tool:mutate".into(), 1)].into_iter().collect()
    );
    state.clear();
    let before = rig.wal_bytes();
    result(&send(&mut rig, "first", Some(&plan), None).await);
    assert!(state.hooks().is_empty());
    assert_eq!(rig.wal_bytes(), before);
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn approval_denies_mutated_input_before_validate() {
    let (mut rig, daemon, state, plan) = start(
        "mutate-approval-live",
        vec![subscription("pre_tool", Some("mutate"), "pass", 1000)],
    )
    .await;
    *rig.script.deny_input.lock().unwrap() = Some(json!({"value":"denied"}));
    state.answer_with(|_| {
        answer(HookAnswer::Mutate {
            input: json!({"value":"denied"}),
            note: None,
        })
    });
    rig.script.push(&[
        Step::Stream(vec![call("c", json!({"value":"allowed"}))]),
        Step::Text("done"),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = values(&rig.wait_for_finished(1).await);
    let terminal = only(&records, "tool_no_dispatch");
    assert_eq!(terminal["reason"]["kind"], "denied");
    assert_eq!(terminal["hooks"]["final"], json!({"value":"denied"}));
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(state.hooks().len(), 1);
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validate_sees_approved_narrowing_and_denial_does_not_cancel_siblings() {
    let (mut rig, daemon, state, plan) = start(
        "validate-live",
        vec![subscription("pre_tool", Some("validate"), "refuse", 1000)],
    )
    .await;
    *rig.script.narrow_input.lock().unwrap() = Some(json!({"value":"narrow"}));
    state.answer_with(|c| {
        assert_eq!(c["input"], json!({"value":"narrow"}));
        if c["tool_call_id"] == "deny" {
            answer(HookAnswer::Deny {
                text: "provider says no".into(),
            })
        } else {
            answer(HookAnswer::Pass)
        }
    });
    rig.script.push(&[
        Step::Stream(vec![
            call("deny", json!({"value":"wide"})),
            call("allow", json!({"value":"wide"})),
        ]),
        Step::Text("done"),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = values(&rig.wait_for_finished(1).await);
    let denied: Vec<_> = records
        .iter()
        .filter(|r| r["type"] == "tool_result" && r["result"]["tool_call_id"] == "deny")
        .collect();
    assert_eq!(denied.len(), 1);
    assert_eq!(denied[0]["reason"], "pre_tool_denied");
    assert!(denied[0].to_string().contains("provider says no"));
    assert!(records
        .iter()
        .all(|r| !(r["type"] == "tool_dispatch_intent" && r["tool_call_id"] == "deny")));
    assert_eq!(*rig.script.dispatched.lock().unwrap(), vec!["allow"]);
    assert_eq!(state.hooks().len(), 2);
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_replace_requires_frozen_grant() {
    for grants in [false, true] {
        let mut sub = subscription("post_tool", None, "refuse", 1000);
        sub["ops"] = json!(["replace"]);
        let (mut rig, daemon, state, _) = start(
            if grants {
                "replace-granted-live"
            } else {
                "replace-refused-live"
            },
            vec![sub],
        )
        .await;
        let plan = plan(&state.subscriptions, grants);
        state.answer_with(|c| {
            assert!(c["call_key"].as_str().is_some());
            answer(HookAnswer::Ops {
                ops: vec![Operation::Replace {
                    block: 0,
                    value: "replacement".into(),
                    note: None,
                }],
            })
        });
        rig.script.push(&[Step::Call("c"), Step::Text("done")]);
        result(&send(&mut rig, "first", Some(&plan), None).await);
        let records = values(&rig.wait_for_finished(1).await);
        let result = only(&records, "tool_result");
        if grants {
            assert_eq!(result["result"]["output"]["text"], "replacement");
            assert_eq!(
                result["hooks"]["original"],
                json!({"tool_call_id":"c","is_error":false,"output":{"kind":"text","text":"worked"}})
            );
        } else {
            assert_eq!(result["reason"], "post_tool_unavailable");
            assert!(result.to_string().contains(MODULE));
            assert!(result.to_string().contains("executed"));
        }
        assert_eq!(state.hooks().len(), 1);
        finish(rig, daemon).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_caps_before_hooks_without_truncating_applied_ops() {
    for too_large in [false, true] {
        let (mut rig, daemon, state, plan) = start(
            if too_large {
                "cap-excess-live"
            } else {
                "cap-headroom-live"
            },
            vec![subscription("post_tool", None, "refuse", 1000)],
        )
        .await;
        rig.script.tool_results.lock().unwrap().insert(
            "c".into(),
            broca_core::DispatchResult::Completed(ToolResult {
                tool_call_id: ToolCallId("c".into()),
                output: ToolOutput::Text {
                    text: "x".repeat(1024 * 1024 + 100),
                },
                is_error: false,
            }),
        );
        let suffix = if too_large {
            "y".repeat(16385)
        } else {
            "suffix".into()
        };
        let expected_suffix = suffix.clone();
        state.answer_with(move |c| {
            assert!(c["blocks"][0].as_str().unwrap().len() <= 1024 * 1024 - 16384);
            append(&suffix)
        });
        rig.script.push(&[Step::Call("c"), Step::Text("done")]);
        result(&send(&mut rig, "first", Some(&plan), None).await);
        let records = values(&rig.wait_for_finished(1).await);
        let result = only(&records, "tool_result");
        if too_large {
            assert_eq!(result["reason"], "post_tool_unavailable");
        } else {
            let rendered = result["result"]["output"]["text"].as_str().unwrap();
            assert!(rendered.ends_with(&expected_suffix));
            assert!(rendered.len() <= 1024 * 1024);
            let original = result["hooks"]["original"]["output"]["text"]
                .as_str()
                .unwrap();
            assert_eq!(format!("{original}{expected_suffix}"), rendered);
            assert_eq!(result["hooks"]["ops"][0]["op"], "append");
        }
        finish(rig, daemon).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_hook_budget_includes_route_open_and_hanging_provider() {
    for (hook, phase) in [
        ("pre_user", None),
        ("post_assistant", None),
        ("pre_tool", Some("mutate")),
        ("pre_tool", Some("validate")),
        ("post_tool", None),
    ] {
        // Long enough that the request always reaches the hanging provider, even after a slow
        // route open on a loaded runner, so the provider-reached assertion is not a race; the
        // upper bound below still proves the budget covers the route open.
        let budget = 2000;
        let (mut rig, daemon, state, plan) = start(
            // A bare phase name: Debug output of the Option would put quotes in the path,
            // which Windows refuses.
            &format!("budget-{hook}-{}", phase.unwrap_or("none")),
            vec![subscription(
                hook,
                phase,
                if hook == "post_assistant" {
                    "pass"
                } else {
                    "refuse"
                },
                budget,
            )],
        )
        .await;
        let key = phase.map_or(hook.to_owned(), |p| format!("{hook}:{p}"));
        state.never.lock().unwrap().insert(key);
        rig.script.push(&[
            if hook == "post_tool" || hook == "pre_tool" {
                Step::Call("c")
            } else {
                Step::Text("done")
            },
            Step::Text("done"),
        ]);
        let elapsed_calls = Arc::new(Mutex::new(Vec::new()));
        let conn = Arc::new(
            broca_subc::SubcConnection::connect(&daemon.connection_file)
                .await
                .unwrap(),
        );
        rig.set_hook_plane(Arc::new(TimedHook {
            inner: broca_subc::SubcHookPlane::new(conn),
            elapsed: elapsed_calls.clone(),
        }));
        result(&send(&mut rig, "first", Some(&plan), None).await);
        let records = values(&rig.wait_for_finished(1).await);
        let timings = elapsed_calls.lock().unwrap().clone();
        assert_eq!(timings.len(), 1);
        let elapsed = timings[0];
        assert!(
            records
                .iter()
                .any(|r| r.to_string().contains("unavailable")),
            "{hook} must end unavailable"
        );
        assert_eq!(
            state.hooks().len(),
            1,
            "{hook}: the real provider was reached"
        );
        assert!(
            elapsed >= Duration::from_millis(budget),
            "{hook} returned before the hanging hook budget"
        );
        assert!(
            elapsed <= Duration::from_millis(budget.min(broca_core::STEP_TRANSFORM_HOOK_CAP) + 250),
            "{hook}: {elapsed:?}"
        );
        finish(rig, daemon).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_user_steer_queue_and_keyed_retries_transform_once() {
    let (mut rig, daemon, state, plan) = start(
        "pre-user-delivery-live",
        vec![subscription("pre_user", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|_| append("-once"));
    rig.script.hold_model(true);
    rig.script
        .push(&[Step::Text("done"), Step::Text("queued done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    for (id, delivery) in [("steer", "steer"), ("queued", "queue")] {
        result(&send(&mut rig, id, Some(&plan), Some(delivery)).await);
        let bytes = rig.wal_bytes();
        result(&send(&mut rig, id, Some(&plan), Some(delivery)).await);
        assert_eq!(rig.wal_bytes(), bytes);
    }
    assert_eq!(state.hooks().len(), 3);
    rig.script.hold_model(false);
    let records = values(&rig.wait_for_finished(2).await);
    let queued = only(&records, "queued");
    assert_eq!(
        queued["hooks"]["final"][0]["content"][0]["text"],
        "input-once"
    );
    let drained = records
        .iter()
        .rfind(|r| r["type"] == "run_started")
        .unwrap();
    assert_eq!(drained["input"], queued["hooks"]["final"]);
    assert_eq!(drained["hooks"]["original"], queued["request"]["input"]);
    assert_eq!(
        state.hooks().len(),
        3,
        "draining cannot re-hook the queued input"
    );
    for (id, delivery) in [
        ("first", None),
        ("steer", Some("steer")),
        ("queued", Some("queue")),
    ] {
        result(&send(&mut rig, id, Some(&plan), delivery).await);
    }
    assert_eq!(
        state.hooks().len(),
        3,
        "drained keyed retry uses original identity"
    );
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_user_refused_interrupt_does_not_write_or_stop_running_run() {
    let (mut rig, daemon, state, plan) = start(
        "refused-interrupt-live",
        vec![subscription("pre_user", None, "refuse", 100)],
    )
    .await;
    rig.script.hold_model(true);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    state.never.lock().unwrap().insert("pre_user".into());
    for delivery in ["steer", "queue", "interrupt"] {
        let before = rig.wal_bytes();
        let frame = send(&mut rig, delivery, Some(&plan), Some(delivery)).await;
        let error: Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(frame.header.ty, subc_protocol::FrameType::Error);
        assert_eq!(error["code"], "pre_user_unavailable");
        assert_eq!(error["detail"]["provider"], MODULE);
        assert_eq!(rig.wal_bytes(), before);
        let status = result(
            &rig.request(7, json!({"method":"run.status","params":{}}))
                .await,
        );
        assert_ne!(status["state"], "idle");
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_user_steer_falling_back_to_queue_keeps_one_suffix() {
    let (mut rig, daemon, state, plan) = start(
        "steer-fallback-live",
        vec![subscription("pre_user", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|_| append("-once"));
    rig.script.hold_model(true);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    state.entered.notified().await;
    state.held.lock().unwrap().insert("pre_user".into());
    let shared = state.clone();
    let script = rig.script.clone();
    let path = rig.wal_path.clone();
    let release = tokio::spawn(async move {
        shared.entered.notified().await;
        script.hold_model(false);
        broca_test_support::event_timeout(
            "pre_user_steer_falling_back_to_queue_keeps_one_suffix",
            async {
                loop {
                    let records =
                        broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
                    if records
                        .iter()
                        .any(|f| matches!(f.record, WalRecord::RunFinished { .. }))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            },
            || format!("WAL bytes={:?}", std::fs::metadata(&path).map(|m| m.len())),
        )
        .await
        .unwrap();
        shared.held.lock().unwrap().clear();
        shared.release.notify_waiters();
    });
    result(&send(&mut rig, "fallback", Some(&plan), Some("steer")).await);
    release.await.unwrap();
    let records = values(&rig.wait_for_finished(2).await);
    let queued = only(&records, "queued");
    assert_eq!(
        queued["hooks"]["final"][0]["content"][0]["text"],
        "input-once"
    );
    assert_eq!(state.hooks().len(), 2);
    result(&send(&mut rig, "fallback", Some(&plan), Some("steer")).await);
    assert_eq!(state.hooks().len(), 2);
    finish(rig, daemon).await;
}

async fn excluded_dispatch(label: &str, result_variant: broca_core::DispatchResult) {
    let (mut rig, daemon, state, plan) =
        start(label, vec![subscription("post_tool", None, "pass", 1000)]).await;
    rig.script
        .tool_results
        .lock()
        .unwrap()
        .insert("c".into(), result_variant);
    rig.script.push(&[Step::Call("c"), Step::Text("done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_for_finished(1).await;
    assert!(state.hooks().is_empty());
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_not_sent() {
    excluded_dispatch(
        "exclude-not-sent-live",
        broca_core::DispatchResult::NotSent {
            reason: "not sent".into(),
        },
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_outcome_unknown() {
    excluded_dispatch(
        "exclude-unknown-live",
        broca_core::DispatchResult::SentButOutcomeUnknown {
            reason: "lost".into(),
        },
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_route_drained_synthetic() {
    excluded_dispatch(
        "exclude-drained-live",
        broca_core::DispatchResult::ScopeDrained {
            reason: "scope closed".into(),
            sent: false,
        },
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_tool_no_dispatch() {
    let (mut rig, daemon, state, plan) = start(
        "exclude-no-dispatch-live",
        vec![subscription("post_tool", None, "pass", 1000)],
    )
    .await;
    let ContentBlock::ToolCall(mut c) = call("c", json!({})) else {
        unreachable!()
    };
    c.tool_name = "absent".into();
    rig.script.push(&[
        Step::Stream(vec![ContentBlock::ToolCall(c)]),
        Step::Text("done"),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    assert!(rig
        .wait_for_finished(1)
        .await
        .iter()
        .any(|r| matches!(r, WalRecord::ToolNoDispatch { .. })));
    assert!(state.hooks().is_empty());
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_duplicate_synthetic() {
    let (mut rig, daemon, state, plan) = start(
        "exclude-duplicate-live",
        vec![subscription("post_tool", None, "pass", 1000)],
    )
    .await;
    rig.script
        .push(&[Step::Calls(&["c", "c"]), Step::Text("done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_for_finished(1).await;
    assert_eq!(
        state.hooks().len(),
        1,
        "only the real call is a subject, not its duplicate"
    );
    assert_eq!(rig.script.dispatched.lock().unwrap().len(), 1);
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_pre_tool_denied() {
    let (mut rig, daemon, state, plan) = start(
        "exclude-deny-live",
        vec![
            subscription("pre_tool", Some("validate"), "refuse", 1000),
            subscription("post_tool", None, "pass", 1000),
        ],
    )
    .await;
    state.answer_with(|_| answer(HookAnswer::Deny { text: "no".into() }));
    rig.script.push(&[Step::Call("c"), Step::Text("done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_for_finished(1).await;
    assert_eq!(
        state.counts(),
        [("pre_tool:validate".into(), 1)].into_iter().collect()
    );
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_pre_tool_unavailable() {
    let (mut rig, daemon, state, plan) = start(
        "exclude-unavailable-live",
        vec![
            subscription("pre_tool", Some("validate"), "refuse", 100),
            subscription("post_tool", None, "pass", 1000),
        ],
    )
    .await;
    state
        .never
        .lock()
        .unwrap()
        .insert("pre_tool:validate".into());
    rig.script.push(&[Step::Call("c"), Step::Text("done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = values(&rig.wait_for_finished(1).await);
    // The call ends unavailable and is never dispatched, so post_tool has no subject.
    assert_eq!(
        only(&records, "tool_result")["reason"],
        "pre_tool_unavailable"
    );
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    let counts = state.counts();
    assert!(
        !counts.keys().any(|hook| hook.starts_with("post_tool")),
        "post_tool must not see a call pre_tool left unavailable: {counts:?}"
    );
    // The 100 ms budget includes opening the hook route, so on a slow runner it can expire
    // before the request reaches the provider. Either way the hook is asked at most once.
    assert!(
        counts.get("pre_tool:validate").copied().unwrap_or(0) <= 1,
        "{counts:?}"
    );
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_structured_json_between_caps() {
    let output = ToolOutput::Json {
        value: json!({"large":"x".repeat(1024*1024-8192)}),
    };
    let original = output.clone();
    let (mut rig, daemon, state, plan) = start(
        "exclude-json-live",
        vec![subscription("post_tool", None, "pass", 1000)],
    )
    .await;
    rig.script.tool_results.lock().unwrap().insert(
        "c".into(),
        broca_core::DispatchResult::Completed(ToolResult {
            tool_call_id: ToolCallId("c".into()),
            output,
            is_error: false,
        }),
    );
    rig.script.push(&[Step::Call("c"), Step::Text("done")]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    let records = rig.wait_for_finished(1).await;
    let recorded = records
        .iter()
        .find_map(|r| match r {
            WalRecord::ToolResult { result, .. } => Some(&result.output),
            _ => None,
        })
        .unwrap();
    assert_eq!(recorded, &original);
    assert!(state.hooks().is_empty());
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_during_pre_tool_writes_no_intent_for_any_call() {
    let (mut rig, daemon, state, plan) = start(
        "cancel-pre-tool-live",
        vec![
            subscription("pre_tool", Some("mutate"), "pass", 30000),
            subscription("post_tool", None, "pass", 1000),
        ],
    )
    .await;
    state.never.lock().unwrap().insert("pre_tool:mutate".into());
    rig.script.push(&[Step::Calls(&["c", "sibling"])]);
    let admitted = result(&send(&mut rig, "first", Some(&plan), None).await);
    state.entered.notified().await;
    rig.cancel(admitted["run_id"].as_str().unwrap()).await;
    let records = rig.wait_for_finished(1).await;
    assert!(records
        .iter()
        .all(|r| !matches!(r, WalRecord::ToolDispatchIntent { .. })));
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    assert_eq!(*rig.script.batch_finishes.lock().unwrap(), 1);
    assert_eq!(
        state.counts(),
        [("pre_tool:mutate".into(), 1)].into_iter().collect()
    );
    finish(rig, daemon).await;
}

async fn seal_at_hook(hook: &str, policy: &str, parallel: bool) {
    let (mut rig, daemon, state, plan) = start(
        &format!("seal-{hook}-{policy}-{parallel}"),
        vec![subscription(hook, None, policy, 30000)],
    )
    .await;
    *rig.script.ignore_graceful_stop.lock().unwrap() = true;
    state.never.lock().unwrap().insert(hook.into());
    rig.script.push(&[if hook == "post_assistant" {
        Step::Text("billed")
    } else if parallel {
        Step::Calls(&["c", "sibling"])
    } else {
        Step::Call("c")
    }]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    state.entered.notified().await;
    assert!(rig.registry().stop_and_seal().await);
    let records = values(&rig.records());
    assert_eq!(
        records.last().unwrap()["type"],
        "run_finished",
        "seal never permits writes after the terminal"
    );
    if hook == "post_assistant" {
        let step = only(&records, "model_step_finished");
        assert_eq!(step["assistant_message"]["content"][0]["text"], "billed");
        assert_eq!(step["hooks"]["unavailable"], json!([MODULE]));
    } else {
        let results: Vec<_> = records
            .iter()
            .filter(|r| r["type"] == "tool_result")
            .collect();
        assert_eq!(results.len(), if parallel { 2 } else { 1 });
        for r in results {
            if policy == "refuse" {
                assert_eq!(r["reason"], "post_tool_unavailable");
                assert!(r.to_string().contains(MODULE));
            } else {
                assert_eq!(r["result"]["output"]["text"], "worked");
                assert_eq!(r["hooks"]["unavailable"], json!([MODULE]));
            }
        }
    }
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_seal_during_post_assistant_records_one_billed_fallback() {
    seal_at_hook("post_assistant", "pass", false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_seal_during_post_tool_pass_records_one_fallback() {
    seal_at_hook("post_tool", "pass", false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_seal_during_post_tool_refuse_withholds_executed_output() {
    seal_at_hook("post_tool", "refuse", false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_seal_parallel_post_tool_writes_two_fallbacks_once() {
    seal_at_hook("post_tool", "pass", true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_restart_steer_and_retry_call_pre_user_once() {
    let (mut rig, daemon, state, plan) = start(
        "paused-steer-live",
        vec![subscription("pre_user", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|_| append("-once"));
    rig.script.hold_tool(true);
    rig.script.push(&[Step::Call("c")]);
    result(&rig.request(7, json!({"method":"session.send","params":{"prompt":"input","send_id":"first","model":{"provider":"fake","model":"fake"},"plan":plan,"on_restart":"resume"}})).await);
    rig.wait_entered().await;
    let mut rig = rig.restart_at_tool_boundary().await;
    daemon.install(&rig).await;
    *state.refuse_declare.lock().unwrap() = true;
    let receipt = result(&send(&mut rig, "steer", Some(&plan), Some("steer")).await);
    assert_eq!(receipt["state"], "paused");
    let records = values(&rig.records());
    let steered = only(&records, "steered");
    assert_eq!(steered["input"][0]["content"][0]["text"], "input-once");
    assert_eq!(state.hooks().len(), 2);
    result(&send(&mut rig, "steer", Some(&plan), Some("steer")).await);
    assert_eq!(state.hooks().len(), 2);
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_restart_drains_frozen_final_without_declare_or_pre_user() {
    let (mut rig, daemon, state, plan) = start(
        "queued-restart-live",
        vec![subscription("pre_user", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|_| append("-once"));
    rig.script.hold_model(true);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    result(&send(&mut rig, "queued", Some(&plan), Some("queue")).await);
    assert_eq!(state.hooks().len(), 2);
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    daemon.install(&rig).await;
    state.clear();
    *state.refuse_declare.lock().unwrap() = true;
    rig.registry().actor_for(&rig.session).await.unwrap();
    let records = values(&rig.wait_for_finished(2).await);
    let queued = only(&records, "queued");
    let started = records
        .iter()
        .rfind(|r| r["type"] == "run_started")
        .unwrap();
    assert_eq!(started["input"], queued["hooks"]["final"]);
    assert_eq!(started["hooks"]["original"], queued["request"]["input"]);
    assert!(state.hooks().is_empty());
    assert_eq!(state.declares(), 0);
    result(&send(&mut rig, "queued", Some(&plan), Some("queue")).await);
    assert!(state.hooks().is_empty());
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_tool_excludes_interrupted_batch_closures() {
    let (mut rig, daemon, state, plan) = start(
        "exclude-interrupted-live",
        vec![
            subscription("pre_tool", Some("mutate"), "pass", 30000),
            subscription("post_tool", None, "pass", 1000),
        ],
    )
    .await;
    state.never.lock().unwrap().insert("pre_tool:mutate".into());
    rig.script.push(&[Step::Call("c")]);
    let admitted = result(&send(&mut rig, "first", Some(&plan), None).await);
    state.entered.notified().await;
    rig.cancel(admitted["run_id"].as_str().unwrap()).await;
    let records = rig.wait_for_finished(1).await;
    assert!(records.iter().any(|r| matches!(
        r,
        WalRecord::ToolResult {
            outcome: broca_wal::DispatchOutcome::Interrupted,
            ..
        }
    )));
    assert_eq!(
        state.counts(),
        [("pre_tool:mutate".into(), 1)].into_iter().collect()
    );
    finish(rig, daemon).await;
}

async fn cancel_pass_then_refuse(seal: bool) {
    let (mut rig, daemon, state, mut plan) = start(
        if seal {
            "seal-pass-refuse-live"
        } else {
            "stop-pass-refuse-live"
        },
        vec![subscription("post_tool", None, "pass", 30000)],
    )
    .await;
    *rig.script.ignore_graceful_stop.lock().unwrap() = seal;
    let later = "later-refuse";
    let second = State::new(vec![subscription("post_tool", None, "refuse", 30000)]);
    let serving = register_as(&daemon.connection_file, later, second.clone()).await;
    let item = json!({"provider":later,"preset":"worker","params":{},"subscriptions":second.subscriptions});
    plan["step_transform_items"]
        .as_array_mut()
        .unwrap()
        .push(item.clone());
    plan["composition"]["step_transform_items"]
        .as_array_mut()
        .unwrap()
        .push(item);
    plan["composition"]["providers"]
        .as_array_mut()
        .unwrap()
        .push(json!({"provider":later,"tools":[]}));
    plan["composition"]["providers"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|p| p["provider"].as_str().unwrap().to_owned());
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    state.never.lock().unwrap().insert("post_tool".into());
    rig.script.push(&[Step::Call("c")]);
    let admitted = result(&send(&mut rig, "first", Some(&plan), None).await);
    state.entered.notified().await;
    if seal {
        assert!(rig.registry().stop_and_seal().await);
    } else {
        rig.cancel(admitted["run_id"].as_str().unwrap()).await;
        rig.wait_for_finished(1).await;
    }
    let records = values(&rig.records());
    let result = only(&records, "tool_result");
    assert_eq!(result["reason"], "post_tool_unavailable");
    assert!(result.to_string().contains(later));
    assert!(result.to_string().contains("executed"));
    assert_eq!(state.hooks().len(), 1);
    assert!(
        second.hooks().is_empty(),
        "cancellation never calls an unanswered later provider"
    );
    assert_eq!(records.last().unwrap()["type"], "run_finished");
    serving.abort();
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_during_post_tool_pass_then_refuse_withholds_naming_later_provider() {
    cancel_pass_then_refuse(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actor_seal_during_post_tool_pass_then_refuse_withholds_naming_later_provider() {
    cancel_pass_then_refuse(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_during_post_assistant_keeps_billed_step_and_marks_unavailable() {
    let (mut rig, daemon, state, plan) = start(
        "stop-post-assistant-live",
        vec![subscription("post_assistant", None, "pass", 30000)],
    )
    .await;
    state.never.lock().unwrap().insert("post_assistant".into());
    rig.script.push(&[Step::Text("billed")]);
    let admitted = result(&send(&mut rig, "first", Some(&plan), None).await);
    state.entered.notified().await;
    rig.cancel(admitted["run_id"].as_str().unwrap()).await;
    let records = values(&rig.wait_for_finished(1).await);
    let step = only(&records, "model_step_finished");
    assert_eq!(step["assistant_message"]["content"][0]["text"], "billed");
    assert_eq!(step["hooks"]["unavailable"], json!([MODULE]));
    assert_eq!(state.hooks().len(), 1);
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_cancel_during_pre_user_discards_send_without_interrupting_run() {
    let (mut rig, daemon, state, plan) = start(
        "caller-cancel-pre-user-live",
        vec![subscription("pre_user", None, "refuse", 30000)],
    )
    .await;
    rig.script.hold_model(true);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    state.entered.notified().await;
    state.never.lock().unwrap().insert("pre_user".into());
    let before = rig.wal_bytes();
    let registry = rig.registry().clone();
    let key = rig.session.clone();
    let request=serde_json::from_value(json!({"model":{"provider_module_id":"fake","model_id":"fake"},"input":[{"role":"user","content":[{"type":"text","text":"cancelled"}]}]})).unwrap();
    let pending = tokio::spawn(async move {
        registry
            .route_submission_with_delivery(
                &key,
                broca_protocol::RunId("cancelled-send".into()),
                request,
                broca_module::RunKind::FreshOrResume,
                broca_module::actor::SendKey::SendId("cancelled-send".into()),
                false,
                broca_protocol::Delivery::Interrupt,
            )
            .await
    });
    state.entered.notified().await;
    pending.abort();
    let _ = pending.await;
    let status = tokio::time::timeout(
        Duration::from_secs(5),
        rig.registry().route_status(&rig.session),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        status,
        broca_module::actor::LineageStatus::Active { .. }
    ));
    assert_eq!(rig.wal_bytes(), before);
    assert_eq!(state.hooks().len(), 2);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    finish(rig, daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_pass_post_assistant_emits_no_display_reset() {
    let (mut rig, daemon, state, plan) = start(
        "all-pass-display-live",
        vec![subscription("post_assistant", None, "pass", 1000)],
    )
    .await;
    rig.script.hold_model(true);
    rig.script.push(&[Step::Stream(vec![text("unchanged")])]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    // Release the held model only once the subscription is attached: the display lane is lossy.
    let script = rig.script.clone();
    let frames = rig
        .subscribe_collect_after_attach(
            8,
            json!({"from": "start"}),
            Duration::from_millis(500),
            move || script.hold_model(false),
        )
        .await;
    let records = values(&rig.wait_for_finished(1).await);
    assert!(only(&records, "model_step_finished").get("hooks").is_none());
    let stream = frames
        .iter()
        .map(|f| String::from_utf8_lossy(&f.body).to_string())
        .collect::<String>();
    assert!(stream.contains("unchanged"));
    assert!(!stream.contains("display_reset"));
    assert_eq!(state.hooks().len(), 1);
    finish(rig, daemon).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_step_empty_message_ids_keep_reset_on_the_display_lane() {
    let (mut rig, daemon, state, plan) = start(
        "empty-id-display-live",
        vec![subscription("post_assistant", None, "pass", 1000)],
    )
    .await;
    state.answer_with(|c| {
        if c["step_id"] == "1" {
            append("-post")
        } else {
            answer(HookAnswer::Pass)
        }
    });
    rig.script.hold_model(true);
    rig.script.push(&[
        Step::StreamEmpty(vec![text("gemini-step-one"), call("c", json!({}))]),
        Step::StreamEmpty(vec![text("gemini-step-two")]),
    ]);
    result(&send(&mut rig, "first", Some(&plan), None).await);
    rig.wait_entered().await;
    // Release the held model only once the subscription is attached: the display lane is lossy.
    let script = rig.script.clone();
    let frames = rig
        .subscribe_collect_after_attach(
            8,
            json!({"from": "start"}),
            Duration::from_millis(500),
            move || script.hold_model(false),
        )
        .await;
    rig.wait_for_finished(1).await;
    let displays = frames
        .iter()
        .filter_map(|f| serde_json::from_slice::<Value>(&f.body).ok())
        .filter(|v| v.to_string().contains("display"))
        .collect::<Vec<_>>();
    let lane = serde_json::to_string(&displays).unwrap();
    let original = lane.find("gemini-step-one").unwrap();
    let reset = lane.find("display_reset").unwrap();
    let final_text = lane.find("gemini-step-one-post").unwrap();
    let next = lane.find("gemini-step-two").unwrap();
    assert!(
        original < reset && reset < final_text && final_text < next,
        "{lane}"
    );
    assert_eq!(lane.matches("display_reset").count(), 1);
    assert!(lane.contains("\"message_id\":\"\""));
    finish(rig, daemon).await;
}
