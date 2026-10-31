//! The `llm-runner/v1` role surface through the real serve path: `session.read`,
//! `session.subscribe`, `session.head`, `run.result`, `role.describe` and their refusals.
//!
//! Every run goes through the real core loop over a real file WAL (the delivery rig), so each
//! record a test reads back was written by core or the lineage actor exactly as in production.

use broca_module::ServeHandler;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::RunId;

use crate::delivery_rig;
use delivery_rig::*;
use std::time::Duration;

use subc_protocol::{Frame, FrameType};

/// `body` with the run id and lineage id it names replaced by placeholders, so the rest of its
/// bytes can be pinned: both are minted per run from a per-process scratch directory.
fn normalized(body: &[u8], run_id: &str, lineage_id: &str) -> String {
    String::from_utf8_lossy(body)
        .replace(run_id, "@RUN_ID@")
        .replace(lineage_id, "@LINEAGE_ID@")
}

/// The pinned text of `file`, without its trailing newline.
fn pinned(file: &str) -> String {
    std::fs::read_to_string(golden_path(file))
        .unwrap()
        .trim_end_matches('\n')
        .to_owned()
}

/// A finished session of one run that called the `work` tool once before answering.
async fn one_tool_turn(tag: &str) -> (Rig, String) {
    let mut rig = Rig::start(tag).await;
    rig.script
        .push(&[Step::Call("call-1"), Step::Text("answer")]);
    let active = rig.send("turn-1", "hi", None).await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    (rig, run_id)
}

/// A `session.read` and a `session.subscribe` from the start, each with no params, of a session
/// whose one run called a tool. Existing member and cursor/unit bytes remain pinned separately
/// from the default read-entry times, dispatch attribution, live metadata and message boundaries.
#[tokio::test]
async fn read_and_subscribe_bytes_without_new_params_are_pinned() {
    let (mut rig, run_id) = one_tool_turn("pin-read").await;
    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    let lineage_id = page["lineage_id"].as_str().unwrap().to_owned();
    // Times are intentional default envelope additions. Require each one, then remove only
    // its envelope spelling to compare the older member bytes; new shape goldens pin the additions.
    let mut body = normalized(&read.body, &run_id, &lineage_id);
    for entry in page["messages"].as_array().unwrap() {
        let at = entry["written_at"]
            .as_u64()
            .expect("new messages have recorded times");
        assert!(at > 0);
        let ordinal = entry["ordinal"].as_u64().unwrap();
        let timed = format!("\"written_at\":{at},\"ordinal\":{ordinal}");
        assert_eq!(body.matches(&timed).count(), 1);
        body = body.replace(&timed, &format!("\"ordinal\":{ordinal}"));
    }
    let attribution = format!(
        r#","tool_calls":[{{"tool_call_id":"call-1","call_key":"@LINEAGE_ID@:5","dispatched_to":"{WORK_MODULE}","indeterminate":false}}]"#
    );
    assert_eq!(body.matches(&attribution).count(), 1, "{body}");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(
            golden_path("session_read.tool_turn.json"),
            format!("{}\n", body.replace(&attribution, "")),
        )
        .unwrap();
    }
    assert_eq!(
        body.replace(&attribution, ""),
        pinned("session_read.tool_turn.json")
    );
    assert!(
        !pinned("session_read.tool_turn.json").contains("tool_calls"),
        "the pinned bytes predate the member"
    );

    let frames: Vec<Frame> = rig
        .subscribe_collect(
            9,
            serde_json::json!({"from": "start"}),
            Duration::from_millis(500),
        )
        .await;
    assert!(
        frames.iter().all(|f| f.header.ty == FrameType::StreamData),
        "a subscription from the start streams: {frames:?}"
    );
    let events: Vec<String> = frames
        .iter()
        .map(|f| normalized(&f.body, &run_id, &lineage_id))
        .collect();
    let legacy: Vec<_> = events
        .iter()
        .filter_map(|body| {
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            if value["kind"] != "control" || value["unit"]["type"] == "message_boundary" {
                return None;
            }
            let split = body
                .rfind(",\"seq\":")
                .expect("live members follow the unchanged payload");
            Some(format!("{}}}", &body[..split]))
        })
        .collect();
    assert_eq!(
        legacy.join("\n"),
        pinned("session_subscribe.tool_turn.legacy.jsonl")
    );
    for (index, body) in events.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["seq"], index as u64 + 1);
        cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&value["event"]).unwrap();
    }
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        let sample = format!("{}\n", events.join("\n"));
        std::fs::write(golden_path("session_subscribe.tool_turn.jsonl"), &sample).unwrap();
        println!("GOLDEN_BEGIN session_subscribe.tool_turn.jsonl\n{sample}GOLDEN_END session_subscribe.tool_turn.jsonl");
    }
    assert_eq!(
        events.join("\n"),
        tool_turn_sample_with_recorded_times(
            &pinned("session_subscribe.tool_turn.jsonl"),
            &rig.records(),
        )
    );
    rig.finish().await;
}

#[tokio::test]
async fn tagged_open_tool_turn_read_bytes_are_pinned() {
    let (mut rig, _) = crate::plan_admission::plan_rig("pin-tagged-open-read").await;
    rig.script.push(&[
        Step::Usage {
            content: vec![
                ContentBlock::Text {
                    text: "checking".into(),
                },
                ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-1".into()),
                    tool_name: "work".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                }),
            ],
            usage: Default::default(),
        },
        Step::Call("call-open"),
        Step::Text("answer"),
    ]);
    rig.script.hold_tool(true);
    let composition = serde_json::json!({"providers": [{"provider": WORK_MODULE, "tools": [
        {"name": "work", "capabilities": []}
    ]}], "step_transform_items": []});
    let plan = serde_json::json!({
        "tool_items": [{"provider": WORK_MODULE, "preset": "head", "params": {}}],
        "system_text_items": [], "step_transform_items": [], "render_tags": true,
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    });
    let active = rig.request(7, serde_json::json!({"method": "session.send", "params": {
        "send_id": "tagged-turn", "prompt": "hi", "model": {"provider": "fake", "model": "fake"},
        "plan": plan,
    }})).await;
    let run_id = result(&active)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;

    // After call-1 commits, pause the next model request, then hold call-open's execution.
    // session.read can then see the assistant's open call before its batch commits, so its
    // part rows must exist without depending on commit-time numbering.
    rig.script.hold_model(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    rig.script.hold_tool(true);
    rig.script.hold_model(false);
    rig.wait_entered().await;
    let records = rig.records();
    let config = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .unwrap();
    assert!(config.render_tags);
    assert!(config.computed_tags.is_some());
    assert!(records.iter().any(|record| matches!(record,
        broca_wal::WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "call-1")));
    assert!(!records.iter().any(|record| matches!(record,
        broca_wal::WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "call-open")));

    let read = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let page = result(&read);
    let lineage_id = page["lineage_id"].as_str().unwrap();
    assert_eq!(page["messages"].as_array().unwrap().len(), 4);
    assert_eq!(
        page["messages"][0]["tags"],
        serde_json::json!([{"part": "m0#0", "tag": "§1§"}])
    );
    assert_eq!(
        page["messages"][1]["tags"],
        serde_json::json!([{"part": "m1#0", "tag": "§2§"}, {"part": "m1#1"}])
    );
    assert_eq!(
        page["messages"][2]["tags"],
        serde_json::json!([{"part": "m2#0", "tag": "§3§"}])
    );
    assert_eq!(
        page["messages"][3]["tags"],
        serde_json::json!([{"part": "m3#0"}])
    );
    assert_eq!(
        page["messages"][3]["tool_calls"][0]["tool_call_id"],
        "call-open"
    );
    let mut body = normalized(&read.body, &run_id, lineage_id);
    // Entry times are pinned by session_read.written_at.json. This golden pins the tag rows, so
    // remove each recorded time from the envelope (the user message must carry one) and compare
    // the rest.
    for entry in page["messages"].as_array().unwrap() {
        let ordinal = entry["ordinal"].as_u64().unwrap();
        match entry["written_at"].as_u64() {
            Some(at) => {
                let timed = format!("\"written_at\":{at},\"ordinal\":{ordinal}");
                assert_eq!(body.matches(&timed).count(), 1);
                body = body.replace(&timed, &format!("\"ordinal\":{ordinal}"));
            }
            None => assert_ne!(ordinal, 0, "the user message has a recorded time"),
        }
    }
    let file = "session_read.tagged_open_tool_turn.json";
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(file), format!("{body}\n")).unwrap();
        println!("GOLDEN_BEGIN {file}\n{body}\nGOLDEN_END {file}");
    }
    assert_eq!(body, pinned(file));
    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

/// A read held between a tool turn's write and its batch commit (the tool provider holds the
/// call) exposes the turn's text. llm-runner §9.6 lets an exposed part change its number only
/// on a new lineage, so the number that read shows must be the one the finished run shows: an
/// unnumbered row there, numbered after commit, is a renumbering on the same lineage.
#[tokio::test]
async fn a_held_tool_turns_text_keeps_the_number_a_read_exposed() {
    let (mut rig, _) = crate::plan_admission::plan_rig("held-turn-text-number").await;
    rig.script.push(&[
        Step::Usage {
            content: vec![
                ContentBlock::Text {
                    text: "checking".into(),
                },
                ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-held".into()),
                    tool_name: "work".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                }),
            ],
            usage: Default::default(),
        },
        Step::Text("answer"),
    ]);
    rig.script.hold_tool(true);
    let composition = serde_json::json!({"providers": [{"provider": WORK_MODULE, "tools": [
        {"name": "work", "capabilities": []}
    ]}], "step_transform_items": []});
    let plan = serde_json::json!({
        "tool_items": [{"provider": WORK_MODULE, "preset": "head", "params": {}}],
        "system_text_items": [], "step_transform_items": [], "render_tags": true,
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    });
    rig.request(
        7,
        serde_json::json!({"method": "session.send", "params": {
            "send_id": "held-text", "prompt": "hi", "model": {"provider": "fake", "model": "fake"},
            "plan": plan,
        }}),
    )
    .await;
    // The tool provider now holds call-held: its dispatch intent is durable, its result is not.
    rig.wait_entered().await;
    assert!(!rig.records().iter().any(|record| matches!(record,
        broca_wal::WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "call-held")));

    let held = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let held = result(&held).clone();
    let expected = serde_json::json!([{"part": "m1#0", "tag": "§2§"}, {"part": "m1#1"}]);
    assert_eq!(
        held["messages"][1]["tags"], expected,
        "the held turn's text shows its final number: {held}"
    );

    rig.script.hold_tool(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let after = rig
        .request(
            7,
            serde_json::json!({"method": "session.read", "params": {}}),
        )
        .await;
    let after = result(&after).clone();
    assert_eq!(after["lineage_id"], held["lineage_id"]);
    assert_eq!(
        after["messages"][1]["tags"], held["messages"][1]["tags"],
        "a number a read exposed is unchanged on the same lineage"
    );
    rig.finish().await;
}
// ---- helpers ------------------------------------------------------------------------------

/// Keep the captured frame template, substituting only this run's two WAL observations.
/// The expected clocks come from durable records, never from the stream being checked.
fn tool_turn_sample_with_recorded_times(sample: &str, records: &[broca_wal::WalRecord]) -> String {
    sample
        .lines()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let (field, start) = match value["event"]["type"].as_str() {
                Some("tool_started") => ("started_at", true),
                Some("tool_finished") => ("finished_at", false),
                _ => return line.to_owned(),
            };
            let call = value["event"]["tool_call_id"].as_str().unwrap();
            let recorded = records
                .iter()
                .find_map(|record| match record {
                    broca_wal::WalRecord::ToolDispatchIntent {
                        tool_call_id,
                        dispatched_at_ms,
                        ..
                    } if start && tool_call_id.0 == call => Some(*dispatched_at_ms),
                    broca_wal::WalRecord::ToolResult {
                        result,
                        finished_at_ms,
                        ..
                    } if !start && result.tool_call_id.0 == call => Some(*finished_at_ms),
                    _ => None,
                })
                .flatten()
                .expect("the real tool turn recorded its observation");
            let captured = value["event"][field]
                .as_u64()
                .expect("the sample retains the real captured time");
            line.replace(
                &format!("\"{field}\":{captured}"),
                &format!("\"{field}\":{recorded}"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The role's error body of a refusal frame.
fn refusal(frame: &Frame) -> serde_json::Value {
    assert_eq!(
        frame.header.ty,
        FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

async fn call(rig: &mut Rig, method: &str, params: serde_json::Value) -> Frame {
    rig.request(7, serde_json::json!({"method": method, "params": params}))
        .await
}

async fn read(rig: &mut Rig, params: serde_json::Value) -> ReadPage {
    let reply = call(rig, "session.read", params).await;
    serde_json::from_value(result(&reply)).unwrap()
}

/// Send a refused request and prove it wrote nothing: the WAL holds the same bytes after it.
async fn refused_writing_nothing(
    rig: &mut Rig,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let before = rig.wal_bytes();
    let reply = call(rig, method, params.clone()).await;
    let body = refusal(&reply);
    assert_eq!(
        rig.wal_bytes().len(),
        before.len(),
        "{method} {params} was refused {body} but the WAL grew"
    );
    assert_eq!(rig.wal_bytes(), before);
    body
}

/// A finished session of three runs, each a prompt and a one-text answer: six messages.
async fn three_turns(tag: &str) -> Rig {
    let mut rig = Rig::start(tag).await;
    for (n, answer) in ["one", "two", "three"].into_iter().enumerate() {
        rig.script.push(&[Step::Text(answer)]);
        rig.send(&format!("turn-{n}"), &format!("prompt {n}"), None)
            .await;
        rig.wait_for_finished(n + 1).await;
        rig.wait_idle().await;
    }
    rig
}

fn ordinals(page: &ReadPage) -> Vec<u64> {
    page.messages.iter().map(|m| m.ordinal).collect()
}

#[tokio::test]
async fn reply_too_large_message_bytes_match_published_vector_through_serve() {
    let vectors = reply_too_large_vectors();
    let case = vectors["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "raw message exceeds the transport frame limit")
        .unwrap();
    let mut calibration = Rig::start("reply-too-large-calibration").await;
    import_reply_too_large_session(&mut calibration, 9, String::new()).await;
    let calibration_frame = calibration
        .request(
            9,
            serde_json::json!({"method":"session.read","params":case["request"]}),
        )
        .await;
    assert_eq!(calibration_frame.header.ty, FrameType::Response);
    let expected_frame_bytes = case["refusal"]["detail"]["bytes"].as_u64().unwrap() as usize;
    let calibration_frame_bytes = calibration_frame.body.len() + subc_protocol::HEADER_LEN;
    let final_text_bytes = expected_frame_bytes - calibration_frame_bytes;
    calibration.finish().await;

    let mut rig = Rig::start("reply-too-large-vector").await;
    import_reply_too_large_session(&mut rig, 9, "x".repeat(final_text_bytes)).await;
    let frame = rig
        .request(
            9,
            serde_json::json!({"method":"session.read","params":case["request"]}),
        )
        .await;
    assert_eq!(frame.header.ty, FrameType::Error);
    let refusal = &case["refusal"];
    let detail: cortexkit_role_llm_runner::errors::ReplyTooLargeDetail =
        serde_json::from_value(refusal["detail"].clone()).unwrap();
    assert_eq!(serde_json::to_value(&detail).unwrap(), refusal["detail"]);
    #[derive(serde::Serialize)]
    struct ExpectedError<'a> {
        code: &'a str,
        message: &'a str,
        detail: &'a cortexkit_role_llm_runner::errors::ReplyTooLargeDetail,
    }
    assert_eq!(
        frame.body,
        serde_json::to_vec(&ExpectedError {
            code: refusal["code"].as_str().unwrap(),
            message: refusal["message"].as_str().unwrap(),
            detail: &detail,
        })
        .unwrap()
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap(),
        *refusal
    );
    rig.finish().await;
}

use cortexkit_role_llm_runner::{
    describe::check_describe,
    read::{HeadMeta, ReadPage},
    run::RunResult,
};

const REPLY_TOO_LARGE_VECTOR_SESSION: &str = "ses_01J9Z8Y7X6W5V4U3T2S1R0Q9P8";

async fn import_reply_too_large_session(rig: &mut Rig, channel: u16, final_text: String) {
    let session = rig
        .bind_session(channel, REPLY_TOO_LARGE_VECTOR_SESSION)
        .await;
    let mut messages: Vec<_> = (0..7)
        .map(|ordinal| {
            let role = if ordinal % 2 == 0 {
                Role::User
            } else {
                Role::Assistant
            };
            Message::new(
                role,
                vec![ContentBlock::Text {
                    text: format!("history {ordinal}"),
                }],
            )
        })
        .collect();
    messages.push(Message::new(
        Role::Assistant,
        vec![ContentBlock::Text { text: final_text }],
    ));
    rig.registry()
        .import(broca_module::serve::SessionImportRequest {
            project_id: None,
            principal: None,
            session,
            run_id: RunId("reply-too-large-import".into()),
            messages,
            source_ref: "fixture:reply-too-large".into(),
        })
        .await
        .unwrap();
}

/// Read the published crate's copy of the refusal vectors.
fn reply_too_large_vectors() -> serde_json::Value {
    let contents = cortexkit_role_llm_runner::vectors::FILES
        .iter()
        .find_map(|(name, contents)| (*name == "reply-too-large.json").then_some(*contents))
        .expect("the role crate publishes reply-too-large.json");
    serde_json::from_str(contents).unwrap()
}

// ---- role.describe ------------------------------------------------------------------------

/// `role.describe` passes the role's own consumer check: the v1 major with its required ops,
/// every op of each declared group, the session-capability source and the read byte cap. It
/// declares the groups broca serves and none it does not.
#[tokio::test]
async fn role_describe_passes_the_roles_check_and_declares_what_broca_serves() {
    let mut rig = Rig::start("describe").await;
    let reply = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let raw = result(&reply);
    let describe = check_describe(&raw).unwrap_or_else(|p| panic!("{raw}: {p:?}"));
    for group in [
        "transcript_reads",
        "run_ops",
        "run_status",
        "held_calls",
        "run_usage",
        "run_cancel",
        "retract",
        "input_token_cap",
        "dispatch_attribution",
        "streaming",
        "live_events",
        "steer",
        "queue",
        "interrupt",
        "retention",
        "session_change",
    ] {
        assert!(describe.declares(group), "{group}");
    }
    for group in ["model_view", "compaction", "call-modes/v1"] {
        assert!(!describe.declares(group), "{group}");
    }
    assert_eq!(raw["session_capabilities_from"], "admission");
    assert_eq!(
        raw["retention"],
        serde_json::json!({"max_seconds":2592000,"delete_within_ms":3600000})
    );
    assert!(describe.majors[0].ops.iter().any(|op| op == "run.status"));
    assert_eq!(
        raw["max_bytes"],
        // Default and maximum are both the shared 3 MiB reply budget (READ_REPLY_MAX_BYTES).
        serde_json::json!({"default": 3145728, "maximum": 3145728})
    );
    assert_eq!(raw["majors"][0]["stability"], "alpha");
    // It describes the build, never the session, so a second answer is the same.
    let again = call(&mut rig, "role.describe", serde_json::Value::Null).await;
    assert_eq!(result(&again), raw);
    let body = refusal(&call(&mut rig, "role.describe", serde_json::json!({"x": 1})).await);
    assert_eq!(body["detail"]["field"], "x");
    rig.finish().await;
}

// ---- session.read ---------------------------------------------------------------------------

/// A session never written answers an empty page with no `lineage_id`, and `session.head` with
/// every member absent; neither writes anything.
#[tokio::test]
async fn a_never_written_session_has_no_lineage_page_or_head() {
    let mut rig = Rig::start("unwritten").await;
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(page, ReadPage::no_lineage());
    let head: HeadMeta = serde_json::from_value(result(
        &call(&mut rig, "session.head", serde_json::json!({})).await,
    ))
    .unwrap();
    // §4.3 would let it state `input`, but the conformance kit requires every member absent.
    assert_eq!(head, HeadMeta::no_lineage());
    assert!(
        rig.wal_bytes().is_empty(),
        "a read of an unwritten session wrote"
    );
    rig.finish().await;
}

/// The tail is the newest page, a range stops at its limit with a cursor, and `after_mid` reads
/// strictly after the message it names; every page carries the session's lineage.
#[tokio::test]
async fn tail_range_and_after_mid_reads() {
    let mut rig = three_turns("modes").await;
    let all = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(ordinals(&all), [0, 1, 2, 3, 4, 5]);
    let lineage = all
        .lineage_id
        .clone()
        .expect("a written session has a lineage");

    let tail = read(&mut rig, serde_json::json!({"limit": 2})).await;
    assert_eq!(ordinals(&tail), [4, 5], "the tail is the newest page");
    assert_eq!(tail.next_from_ordinal, None);

    let range = read(&mut rig, serde_json::json!({"from_ordinal": 0, "limit": 2})).await;
    assert_eq!(ordinals(&range), [0, 1]);
    assert_eq!(range.next_from_ordinal, Some(2));

    let after = read(
        &mut rig,
        serde_json::json!({"after_mid": "m1", "lineage_id": lineage}),
    )
    .await;
    assert_eq!(ordinals(&after), [2, 3, 4, 5]);
    let past_end = read(
        &mut rig,
        serde_json::json!({"after_mid": "m5", "lineage_id": lineage}),
    )
    .await;
    assert!(past_end.messages.is_empty());
    for page in [&tail, &range, &after, &past_end] {
        assert_eq!(page.lineage_id.as_deref(), Some(lineage.as_str()));
    }
    // Broca runs no hooks, so asking for originals adds nothing.
    let with_originals = read(&mut rig, serde_json::json!({"include_originals": true})).await;
    assert_eq!(with_originals, all);
    rig.finish().await;
}

/// Each refusal of a read is by name, writes nothing, and is never answered from the tail: a
/// lineage other than the session's, a mid the lineage does not hold, and every misuse of the
/// request, naming the member at fault.
#[tokio::test]
async fn read_refusals_are_named_and_write_nothing() {
    let mut rig = three_turns("refusals").await;
    let lineage = read(&mut rig, serde_json::json!({}))
        .await
        .lineage_id
        .unwrap();
    for params in [
        serde_json::json!({"lineage_id": "not-this-lineage"}),
        serde_json::json!({"from_ordinal": 0, "lineage_id": "not-this-lineage"}),
        serde_json::json!({"after_mid": "m1", "lineage_id": "not-this-lineage"}),
        // Naming another lineage is refused `lineage_changed` even when the mid is malformed.
        serde_json::json!({"after_mid": "bogus", "lineage_id": "not-this-lineage"}),
    ] {
        let body = refused_writing_nothing(&mut rig, "session.read", params).await;
        assert_eq!(body["code"], "lineage_changed", "{body}");
    }
    for mid in ["m6", "m99", "bogus", "m01", "m"] {
        let body = refused_writing_nothing(
            &mut rig,
            "session.read",
            serde_json::json!({"after_mid": mid, "lineage_id": lineage}),
        )
        .await;
        assert_eq!(body["code"], "unknown_mid", "{mid}: {body}");
    }
    for (params, field) in [
        (serde_json::json!({"after_mid": "m1"}), "lineage_id"),
        (
            serde_json::json!({"after_mid": "m1", "lineage_id": lineage, "from_ordinal": 0}),
            "from_ordinal",
        ),
        (serde_json::json!({"from_ordinl": 0}), "from_ordinl"),
        (serde_json::json!({"view": "model"}), "view"),
        (serde_json::json!({"max_bytes": "lots"}), "max_bytes"),
    ] {
        let body = refused_writing_nothing(&mut rig, "session.read", params.clone()).await;
        assert_eq!(body["code"], "invalid_params", "{params}: {body}");
        assert_eq!(body["detail"]["field"], field, "{params}: {body}");
    }
    let body =
        refused_writing_nothing(&mut rig, "session.head", serde_json::json!({"x": true})).await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("x"))
    );
    rig.finish().await;
}

/// A byte cap stops a range page early with a cursor to the next message, keeps a tail page at
/// the newest message, and lets a single message larger than the cap through alone and whole.
#[tokio::test]
async fn pages_are_capped_by_bytes() {
    let mut rig = Rig::start("bytes").await;
    let big = format!("big-{}", "x".repeat(16 * 1024));
    rig.script.push(&[Step::Text("small"), Step::Text("after")]);
    rig.send("turn-0", &big, None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.send("turn-1", "next", None).await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let all = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(ordinals(&all), [0, 1, 2, 3]);

    let one = read(
        &mut rig,
        serde_json::json!({"from_ordinal": 1, "limit": 100, "max_bytes": 1}),
    )
    .await;
    assert_eq!(one.messages, all.messages[1..2]);
    assert_eq!(one.next_from_ordinal, Some(2));

    let oversize = read(
        &mut rig,
        serde_json::json!({"from_ordinal": 0, "limit": 100, "max_bytes": 1024}),
    )
    .await;
    assert_eq!(
        oversize.messages,
        all.messages[..1],
        "the big message comes alone"
    );
    assert!(oversize.messages[0].message.to_string().contains(&big));
    assert_eq!(oversize.next_from_ordinal, Some(1));

    let tail = read(&mut rig, serde_json::json!({"max_bytes": 1})).await;
    assert_eq!(
        tail.messages,
        all.messages[3..],
        "a capped tail keeps the newest"
    );
    assert_eq!(tail.next_from_ordinal, None);
    rig.finish().await;
}

// ---- dispatch attribution --------------------------------------------------------------------

/// Two sibling calls of one step get distinct keys, a model id recurring in a later step gets a
/// distinct key again, and every call names the module it was dispatched to. A key is the
/// lineage and WAL sequence of the call's own dispatch intent.
#[tokio::test]
async fn every_call_has_its_own_key_and_module() {
    let mut rig = Rig::start("keys").await;
    rig.script.push(&[
        Step::Calls(&["call_0", "call_1"]),
        Step::Call("call_0"),
        Step::Text("done"),
    ]);
    rig.send("turn-0", "go", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let lineage = page.lineage_id.clone().unwrap();
    let entries: Vec<_> = page
        .messages
        .iter()
        .flat_map(|m| m.tool_calls.iter().cloned())
        .collect();
    let ids: Vec<&str> = entries.iter().map(|e| e.tool_call_id.as_str()).collect();
    assert_eq!(ids, ["call_0", "call_1", "call_0"]);
    let keys: Vec<String> = entries
        .iter()
        .map(|e| e.call_key.clone().unwrap())
        .collect();
    assert_eq!(
        keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3,
        "{keys:?}"
    );
    let intents: Vec<u64> =
        broca_wal::replay_file(&rig.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
            .unwrap()
            .into_iter()
            .filter(|f| matches!(f.record, broca_wal::WalRecord::ToolDispatchIntent { .. }))
            .map(|f| f.seq)
            .collect();
    let expected: Vec<String> = intents
        .iter()
        .map(|seq| format!("{lineage}:{seq}"))
        .collect();
    assert_eq!(keys, expected);
    for entry in &entries {
        assert_eq!(entry.dispatched_to.as_deref(), Some(WORK_MODULE));
        assert!(!entry.indeterminate);
    }
    // A page holding only part of the transcript reports the same keys for the calls it holds.
    let later = read(&mut rig, serde_json::json!({"from_ordinal": 3, "limit": 1})).await;
    assert_eq!(
        later.messages[0].tool_calls[0].call_key.as_ref(),
        Some(&keys[2])
    );
    rig.finish().await;
}

/// A call recorded before broca recorded the dispatch module keeps its key and reads with no
/// `dispatched_to`: the module is never guessed.
#[tokio::test]
async fn an_intent_without_a_module_reads_with_no_dispatched_to() {
    let mut rig = Rig::start("legacy-intent").await;
    // Write one finished episode the way a build that predates the dispatch module wrote it,
    // before any send, so no lineage actor holds the session.
    let record = |value: serde_json::Value| -> broca_wal::WalRecord {
        serde_json::from_value(value).unwrap()
    };
    let records = [
        record(serde_json::json!({
            "type": "run_started", "run_id": "run-1",
            "session": serde_json::to_value(&rig.session).unwrap(),
            "config": {"model": {"provider_module_id": "fake", "model_id": "fake"}, "tools": [],
                "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1},
            "input": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
            "origin": {"kind": "fresh"}
        })),
        record(serde_json::json!({"type": "step_started", "step_id": 1})),
        record(serde_json::json!({
            "type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "m1", "content": [{"type": "tool_call",
                "tool_call_id": "call-1", "tool_name": "work", "input": {}}]},
            "usage": {}, "finish_reason": "tool_calls"
        })),
        record(serde_json::json!({
            "type": "tool_batch_started", "batch_id": "b1", "step_id": 1,
            "planned_calls": ["call-1"]
        })),
        record(serde_json::json!({
            "type": "tool_dispatch_intent", "batch_id": "b1", "tool_call_id": "call-1",
            "tool_name": "work", "args": {}
        })),
        record(serde_json::json!({
            "type": "tool_result", "batch_id": "b1", "outcome": "completed",
            "result": {"tool_call_id": "call-1", "output": {"kind": "text", "text": "ok"},
                "is_error": false}
        })),
        record(serde_json::json!({"type": "tool_batch_finished", "batch_id": "b1"})),
        record(serde_json::json!({"type": "turn_finished", "step_id": 1})),
        record(serde_json::json!({"type": "step_started", "step_id": 2})),
        record(serde_json::json!({
            "type": "model_step_finished", "step_id": 2,
            "assistant_message": {"message_id": "m2", "content": [{"type": "text",
                "text": "answer"}]},
            "usage": {}
        })),
        record(serde_json::json!({"type": "run_finished", "reason": "completed", "usage": {}})),
    ];
    {
        use broca_wal::Wal;
        std::fs::create_dir_all(rig.wal_path.parent().unwrap()).unwrap();
        let mut wal = broca_wal::FileWal::open(&rig.wal_path).unwrap();
        for record in &records {
            assert!(record.required_features().is_empty());
            wal.append(1, record, true).unwrap();
        }
    }
    let intent_seq = broca_wal::replay_file(&rig.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
        .unwrap()
        .into_iter()
        .find(|f| matches!(f.record, broca_wal::WalRecord::ToolDispatchIntent { .. }))
        .unwrap()
        .seq;
    let page = read(&mut rig, serde_json::json!({})).await;
    let lineage = page.lineage_id.clone().unwrap();
    let entry = &page.messages[1].tool_calls[0];
    assert_eq!(entry.tool_call_id, "call-1");
    assert_eq!(entry.call_key, Some(format!("{lineage}:{intent_seq}")));
    assert_eq!(entry.dispatched_to, None);
    assert!(!entry.indeterminate);
    rig.finish().await;
}

// ---- session.head ---------------------------------------------------------------------------

/// `session.head` names the lineage, the newest ordinal, the control head and the last run,
/// with the time of the newest write, and no message body.
#[tokio::test]
async fn head_reports_the_session_without_bodies() {
    let mut rig = three_turns("head").await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let raw = result(&call(&mut rig, "session.head", serde_json::json!({})).await);
    let head: HeadMeta = serde_json::from_value(raw.clone()).unwrap();
    assert!(head.lineage_consistent(), "{raw}");
    assert_eq!(head.lineage_id, page.lineage_id);
    assert_eq!(head.last_ordinal, Some(5));
    assert_eq!(head.head, page.head);
    let last = head.last_run_state.unwrap();
    assert_eq!(last.state, "completed");
    assert!(head.updated_at.is_some_and(|at| at > 1_700_000_000_000));
    assert!(raw.get("messages").is_none() && !raw.to_string().contains("prompt 0"));
    // llm-runner 0.5.0 §4.3, §4.5: broca applies no view with a cut, so its one lineage
    // starts at ordinal 0 and the head names no cut.
    assert_eq!(head.first_ordinal, Some(0));
    assert_eq!(head.cut, None);
    // Always 0 within one WAL: broca has no revert and no recovery that rewrites a written
    // message. The one case it misstates (a session whose WAL was deleted and later written
    // again) is the "history_version floor" section of
    // .cortexkit/internal-docs/designs/lineage-and-history-0.5.md.
    assert_eq!(head.history_version, Some(0));
    // A send's prompt is text only, a known fact, so `no` rather than `unknown`.
    assert_eq!(
        head.input
            .as_ref()
            .and_then(|input| input.carries_attachments()),
        Some(false)
    );
    rig.finish().await;
}

// ---- run.result -----------------------------------------------------------------------------

/// A completed run answers its final message: the text parts joined exactly as emitted,
/// reasoning left out; a final message with no text answers `text: ""`. An unknown run is
/// refused `unknown_run` and nothing is written.
#[tokio::test]
async fn run_result_answers_the_final_text() {
    use broca_protocol::content::ContentBlock;
    let mut rig = Rig::start("run-result").await;
    let text = |t: &str| ContentBlock::Text { text: t.into() };
    let reasoning = |t: &str| ContentBlock::Reasoning {
        text: t.into(),
        signature: None,
    };
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Blocks(vec![
            text("{\"answer\":\""),
            reasoning("thinking"),
            text("yes"),
            text("\",\"n\":1}"),
        ]),
        Step::Blocks(vec![reasoning("only thinking")]),
    ]);
    let first = rig.send("turn-0", "go", None).await;
    let run_1 = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let second = rig.send("turn-1", "again", None).await;
    let run_2 = result(&second)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;

    let one: RunResult = serde_json::from_value(result(
        &call(&mut rig, "run.result", serde_json::json!({"run_id": run_1})).await,
    ))
    .unwrap();
    assert!(one.final_message_consistent());
    assert_eq!(one.state, "completed");
    let message = one.final_message.unwrap();
    assert_eq!(message.text, "{\"answer\":\"yes\",\"n\":1}");
    assert_eq!((message.ordinal, message.mid.as_str()), (3, "m3"));

    let two: RunResult = serde_json::from_value(result(
        &call(&mut rig, "run.result", serde_json::json!({"run_id": run_2})).await,
    ))
    .unwrap();
    assert_eq!(two.final_message.unwrap().text, "");

    let body = refused_writing_nothing(
        &mut rig,
        "run.result",
        serde_json::json!({"run_id": "nope"}),
    )
    .await;
    assert_eq!(body["code"], "unknown_run");
    for (params, field) in [
        (serde_json::json!({}), "run_id"),
        (serde_json::json!({"run_id": run_1, "extra": 1}), "extra"),
    ] {
        let body = refused_writing_nothing(&mut rig, "run.result", params).await;
        assert_eq!(
            (body["code"].as_str(), body["detail"]["field"].as_str()),
            (Some("invalid_params"), Some(field))
        );
    }
    rig.finish().await;
}

#[tokio::test]
async fn run_result_refuses_a_final_message_that_exceeds_the_frame_limit() {
    use broca_protocol::ContentBlock;

    let mut rig = Rig::start("run-result-frame-limit").await;
    rig.script.push(&[Step::Blocks(vec![ContentBlock::Text {
        text: "x".repeat(4 * 1024 * 1024),
    }])]);
    let sent = rig.send("large-answer", "answer", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;

    let body = refusal(
        &call(
            &mut rig,
            "run.result",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    );
    assert_eq!(body["code"], broca_wire::REPLY_TOO_LARGE);
    assert!(body["detail"]["bytes"].as_u64().unwrap() > broca_wire::SUBC_MAX_FRAME_BYTES);
    assert_eq!(body["detail"]["limit"], broca_wire::SUBC_MAX_FRAME_BYTES);
    assert_eq!(body["detail"]["subject"]["kind"], "message");
    assert_eq!(body["detail"]["subject"]["session"], rig.session.session);
    assert_eq!(body["detail"]["subject"]["ordinal"], 1);
    rig.finish().await;
}

/// Durable role reads still answer after a closed session is folded, without restoring its WAL.
#[tokio::test]
async fn run_result_answers_the_final_text_from_an_archived_session() {
    use broca_core::wal_fold::{fold, FoldOptions, FoldSettings};
    use broca_protocol::content::ContentBlock;
    use broca_wal::archive::ArchiveReader;

    let mut rig = Rig::start_with_idle_ttl("run-result-archived", Duration::from_millis(100)).await;
    let text = |t: &str| ContentBlock::Text { text: t.into() };
    rig.script.push(&[Step::Blocks(vec![
        text("{\"answer\":\""),
        ContentBlock::Reasoning {
            text: "thinking".into(),
            signature: None,
        },
        text("yes"),
        text("\",\"n\":1}"),
    ])]);
    let sent = rig.send("turn-0", "go", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;

    let options = FoldOptions {
        write: true,
        ..Default::default()
    };
    let report = fold(
        &rig.dir,
        FoldSettings {
            options: &options,
            cold_ms: 0,
        },
    )
    .unwrap();
    assert!(
        !rig.wal_path.exists(),
        "fold must remove the live WAL: {report:?}"
    );
    assert_eq!(report.removed, 1, "{report:?}");
    let archive = rig
        .dir
        .join("wal-archive")
        .join(report.journal.unwrap())
        .with_extension("ark");
    let member = ArchiveReader::open(archive, broca_wal::DEFAULT_MAX_READ_BYTES)
        .unwrap()
        .read_member(&broca_core::wal_writer::session_addr(&rig.session))
        .unwrap();
    assert!(
        member.is_some(),
        "the sealed container must hold this session"
    );

    let answer: RunResult = serde_json::from_value(result(
        &call(
            &mut rig,
            "run.result",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    ))
    .unwrap();
    assert_eq!(answer.state, "completed");
    assert!(answer.final_message_consistent());
    let final_message = answer.final_message.unwrap();
    assert_eq!(final_message.text, "{\"answer\":\"yes\",\"n\":1}");
    assert!(
        !rig.wal_path.exists(),
        "run.result must not restore the live WAL"
    );

    let page = read(&mut rig, serde_json::json!({})).await;
    let last = page.messages.last().unwrap();
    assert_eq!(last.ordinal, final_message.ordinal);
    assert_eq!(last.mid, final_message.mid);
    let message = serde_json::to_value(&last.message).unwrap();
    assert_eq!(message["role"], "assistant");
    let read_text: String = message["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|part| part["type"] == "text")
        .map(|part| part["text"].as_str().unwrap())
        .collect();
    assert_eq!(read_text, final_message.text);
    assert!(
        !rig.wal_path.exists(),
        "session.read must not restore the live WAL"
    );

    let status = result(
        &call(
            &mut rig,
            "run.status",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    );
    assert_eq!(status["state"], "completed");
    assert!(
        !rig.wal_path.exists(),
        "run.status must not restore the live WAL"
    );
    rig.finish().await;
}

/// A status-only read must not reopen a folded session's actor or WAL.
#[tokio::test]
async fn run_status_answers_from_an_archived_session_without_restoring_it() {
    let mut rig = Rig::start_with_idle_ttl("run-status-archived", Duration::from_millis(100)).await;
    rig.script.push(&[Step::Text("answer")]);
    let sent = rig.send("turn-0", "go", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;
    fold_session(&rig);
    let status = result(
        &call(
            &mut rig,
            "run.status",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    );
    assert_eq!(status["state"], "completed");
    assert!(
        !rig.wal_path.exists(),
        "run.status alone must not restore the live WAL"
    );
    rig.finish().await;
}

fn fold_session(rig: &Rig) {
    use broca_core::wal_fold::{fold, FoldOptions, FoldSettings};
    let options = FoldOptions {
        write: true,
        ..Default::default()
    };
    let report = fold(
        &rig.dir,
        FoldSettings {
            options: &options,
            cold_ms: 0,
        },
    )
    .unwrap();
    assert!(
        !rig.wal_path.exists(),
        "fold must remove the live WAL: {report:?}"
    );
    assert_eq!(report.removed, 1, "{report:?}");
}

/// An unavailable WAL cannot hide a terminal that the durable run index already knows.
#[tokio::test]
async fn run_status_falls_back_to_the_terminal_index_when_the_wal_is_absent() {
    let mut rig = Rig::start_with_idle_ttl("run-status-absent", Duration::from_millis(100)).await;
    rig.script.push(&[Step::Text("answer")]);
    let sent = rig.send("turn-0", "go", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;
    std::fs::remove_file(&rig.wal_path).unwrap();
    assert!(!rig.wal_path.exists());
    let status = result(
        &call(
            &mut rig,
            "run.status",
            serde_json::json!({"run_id": run_id}),
        )
        .await,
    );
    assert_eq!(status["state"], "completed");
    assert!(status.get("undelivered_steers").is_none());
    assert!(status.get("directives").is_none());
    rig.finish().await;
}

/// A durable pause retains its reason after folding, without recreating the live WAL.
#[tokio::test]
async fn paused_run_answers_from_an_archived_session_without_restoring_it() {
    let mut rig = Rig::start_with_idle_ttl("run-paused-archived", Duration::from_millis(100)).await;
    *rig.script.pause_next_run.lock().unwrap() = true;
    let sent = rig.send("turn-0", "go", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    let records = rig.wait_for_finished(1).await;
    let reason = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunPaused { reason, .. } => {
                Some(serde_json::to_value(reason).unwrap())
            }
            _ => None,
        })
        .unwrap();
    rig.wait_idle().await;
    rig.wait_closed().await;
    fold_session(&rig);
    for method in ["run.status", "run.result"] {
        let answer = result(&call(&mut rig, method, serde_json::json!({"run_id": run_id})).await);
        assert_eq!(answer["state"], "paused", "{method}: {answer}");
        assert_eq!(answer["reason"], reason, "{method}: {answer}");
        assert!(
            !rig.wal_path.exists(),
            "{method} must not restore the live WAL"
        );
    }
    rig.finish().await;
}

// ---- session.subscribe and session.send ------------------------------------------------------

fn seed_empty_lineage(rig: &Rig) {
    use broca_core::WalStore;
    // Opening a lineage can persist its header before any episode is admitted.
    let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
    drop(wal.open(&rig.session).unwrap());
    assert!(rig.wal_path.exists());
}

async fn assert_first_send_defaults(tag: &str, delivery: serde_json::Value) {
    for header_only in [false, true] {
        let mut rig = Rig::start(&format!("{tag}-{header_only}")).await;
        if header_only {
            seed_empty_lineage(&rig);
        }
        rig.script.push(&[Step::Text("revived")]);
        let reply = call(
            &mut rig,
            "session.send",
            serde_json::json!({"prompt": "revive", "send_id": "revive-1", "delivery": delivery,
                "model": {"provider": "fake", "model": "fake"}}),
        )
        .await;
        assert!(matches!(
            result(&reply)["state"].as_str(),
            Some("active" | "pending")
        ));
        let records = rig.wait_for_finished(1).await;
        let config = records
            .iter()
            .find_map(|record| match record {
                broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
                _ => None,
            })
            .expect("the first send started an episode");
        assert_eq!(
            config.model,
            broca_protocol::request::ModelHandle::new("fake", "fake")
        );
        assert_eq!(config.budget, None);
        assert_eq!(config.context_limit, None);
        assert_eq!(config.service_tier, None);
        rig.finish().await;
    }
}

#[tokio::test]
async fn a_fresh_idle_start_with_a_model_uses_defaults() {
    assert_first_send_defaults("fresh-idle-start", serde_json::json!(["idle_start"])).await;
}

#[tokio::test]
async fn a_fresh_steer_with_a_model_uses_defaults() {
    assert_first_send_defaults("fresh-steer", serde_json::json!("steer")).await;
}

#[tokio::test]
async fn an_idle_start_only_list_does_not_inherit_an_existing_episode() {
    let mut rig = Rig::start("idle-start-defaults").await;
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    let first = call(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "first", "send_id": "first",
            "model": {"provider": "fake", "model": "fake"},
            "generation": {"temperature": 0.25},
            "budget": {"nudges": [{"at_tool_calls": 5, "text": "finish soon"}]},
            "context_limit": 32000,
            "service_tier": "priority"
        }),
    )
    .await;
    assert_eq!(result(&first)["state"], "active");
    let initial = rig.wait_for_finished(1).await;
    let frozen = initial
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .unwrap();
    assert!(frozen.budget.is_some());
    assert_eq!(frozen.generation.temperature, Some(0.25));
    rig.wait_idle().await;
    let missing_model = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "next", "send_id": "no-model", "delivery": ["idle_start"]
        }),
    )
    .await;
    assert_eq!(missing_model["detail"]["field"], "model");
    let next = call(
        &mut rig,
        "session.send",
        serde_json::json!({
            "prompt": "next", "send_id": "next", "delivery": ["idle_start"],
            "model": {"provider": "fake", "model": "fake"}
        }),
    )
    .await;
    result(&next);
    let records = rig.wait_for_finished(2).await;
    let config = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .nth(1)
        .unwrap();
    assert_eq!(config.budget, None);
    assert_eq!(config.context_limit, None);
    assert_eq!(config.service_tier, None);
    assert_eq!(config.generation.temperature, None);
    rig.finish().await;
}

#[tokio::test]
async fn joining_delivery_lists_inherit_the_existing_episode() {
    for mode in ["steer", "tool_result", "turn_system"] {
        let mut rig = Rig::start(&format!("joining-list-{mode}")).await;
        rig.script
            .push(&[Step::Text("first"), Step::Text("second")]);
        let first = call(
            &mut rig,
            "session.send",
            serde_json::json!({
                "prompt": "first", "send_id": "first",
                "model": {"provider": "fake", "model": "fake"},
                "generation": {"temperature": 0.25}
            }),
        )
        .await;
        result(&first);
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        let next = call(
            &mut rig,
            "session.send",
            serde_json::json!({
                "prompt": "next", "send_id": "next", "delivery": [mode, "idle_start"]
            }),
        )
        .await;
        result(&next);
        let records = rig.wait_for_finished(2).await;
        let config = records
            .iter()
            .filter_map(|record| match record {
                broca_wal::WalRecord::RunStarted { config, .. } => Some(config),
                _ => None,
            })
            .nth(1)
            .unwrap();
        assert_eq!(config.generation.temperature, Some(0.25), "{mode}");
        assert_eq!(
            config.model,
            broca_protocol::request::ModelHandle::new("fake", "fake")
        );
        rig.finish().await;
    }
}

#[tokio::test]
async fn a_fresh_send_without_a_model_refuses_precisely() {
    for header_only in [false, true] {
        let mut rig = Rig::start(&format!("missing-model-{header_only}")).await;
        if header_only {
            seed_empty_lineage(&rig);
        }
        let body = refused_writing_nothing(
            &mut rig,
            "session.send",
            serde_json::json!({"prompt": "revive", "send_id": "missing", "delivery": "steer"}),
        )
        .await;
        assert!(
            body["message"]
                .as_str()
                .unwrap()
                .contains("model is required"),
            "{body}"
        );
        assert!(
            !body["message"]
                .as_str()
                .unwrap()
                .contains("runner parameters"),
            "{body}"
        );
        rig.finish().await;
    }
}

/// `session.subscribe` refuses an unknown member by name instead of attaching live.
#[tokio::test]
async fn subscribe_is_strict_on_unknown_members() {
    let (mut rig, _) = one_tool_turn("subscribe-strict").await;
    let frames = rig
        .subscribe_collect(
            9,
            serde_json::json!({"form": "start"}),
            Duration::from_millis(300),
        )
        .await;
    let body = refusal(&frames[0]);
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("form"))
    );
    let frames = rig
        .subscribe_collect(
            11,
            serde_json::json!({"from": "yesterday"}),
            Duration::from_millis(300),
        )
        .await;
    assert_eq!(refusal(&frames[0])["detail"]["field"], "from");
    rig.finish().await;
}

/// A steer into a session that has a model may omit `model` and runs on the session's frozen
/// one; any other send without a model is refused naming it, writing nothing.
#[tokio::test]
async fn a_steer_without_a_model_takes_the_sessions_frozen_model() {
    let (mut rig, _) = one_tool_turn("frozen-model").await;
    let body = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "queued", "send_id": "turn-q"}),
    )
    .await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("model"))
    );
    rig.script.push(&[Step::Text("steered answer")]);
    let reply = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "steer me", "send_id": "turn-s", "delivery": "steer"}),
    )
    .await;
    assert_eq!(result(&reply)["state"], "active");
    let records = rig.wait_for_finished(2).await;
    let models: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config.model.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(models.len(), 2);
    assert_eq!(models[0], models[1], "the steer ran on the frozen model");
    // A brand-new session has no frozen model, so a steer into it must name one.
    let mut fresh = Rig::start("frozen-model-fresh").await;
    let body = refused_writing_nothing(
        &mut fresh,
        "session.send",
        serde_json::json!({"prompt": "hi", "send_id": "s", "delivery": "steer"}),
    )
    .await;
    assert_eq!(body["detail"]["field"], "model");
    fresh.finish().await;
    rig.finish().await;
}

/// An unknown delivery mode is refused naming `delivery`, writing nothing.
#[tokio::test]
async fn an_unknown_delivery_is_refused_naming_it() {
    let (mut rig, _) = one_tool_turn("unknown-delivery").await;
    let body = refused_writing_nothing(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "x", "send_id": "s2", "delivery": "broadcast",
            "model": {"provider": "fake", "model": "fake"}}),
    )
    .await;
    assert_eq!(
        (body["code"].as_str(), body["detail"]["field"].as_str()),
        (Some("invalid_params"), Some("delivery"))
    );
    rig.finish().await;
}

// ---- session.baseline -----------------------------------------------------------------------

/// Before the first send the baseline is `not_yet`. The reply to the send that admits the
/// session carries the baseline, as does every retry of that send, and `session.baseline`
/// answers the same: no fetched items and no composition digest, since the session was admitted
/// without a fetch plan, the tool names it was admitted with, and both generations 0. A later
/// send's reply carries none.
#[tokio::test]
async fn the_admitting_send_and_session_baseline_answer_the_same_baseline() {
    use cortexkit_role_llm_runner::{baseline::BaselineReply, send::SendReply};
    let mut rig = Rig::start("baseline").await;
    let before: BaselineReply = serde_json::from_value(result(
        &call(&mut rig, "session.baseline", serde_json::json!({})).await,
    ))
    .unwrap();
    assert!(before.is_not_yet() && before.baseline.is_none());

    rig.script.push(&[Step::Text("one"), Step::Text("two")]);
    let first = rig.send("turn-1", "hi", None).await;
    let hint = serde_json::from_value::<SendReply>(result(&first))
        .unwrap()
        .baseline
        .expect("the admitting send carries the baseline");
    assert!(hint.plan_consistent());
    assert_eq!(hint.composition_digest, None);
    assert!(hint.items.is_empty());
    assert_eq!(hint.tool_names, ["work"]);
    assert_eq!((hint.accepted_generation, hint.applied_generation), (0, 0));
    assert_eq!(
        result(&first)["baseline"],
        serde_json::json!({"items": [], "tool_names": ["work"], "accepted_generation": 0,
            "applied_generation": 0, "rungs":{"tools":["on_prefix_rebuild","immediately"],"system_text":["on_prefix_rebuild","immediately"]}})
    );
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let retry = rig.send("turn-1", "hi", None).await;
    assert_eq!(result(&retry)["baseline"], result(&first)["baseline"]);
    let answered: BaselineReply = serde_json::from_value(result(
        &call(&mut rig, "session.baseline", serde_json::Value::Null).await,
    ))
    .unwrap();
    assert_eq!(answered.state, "ready");
    assert_eq!(answered.baseline.as_ref(), Some(&hint));

    let second = rig.send("turn-2", "again", None).await;
    assert!(result(&second).get("baseline").is_none());
    // The refusal below must write nothing, which it proves by comparing WAL bytes before and
    // after. Let the second run finish first, or its own records land between the two reads.
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let body =
        refused_writing_nothing(&mut rig, "session.baseline", serde_json::json!({"x": 1})).await;
    assert_eq!(body["detail"]["field"], "x");
    rig.finish().await;
}

/// `session.baseline` is answered only to the principal of the session's first send; anyone
/// else is refused `scope_owner_mismatch`, and nothing is written.
#[tokio::test]
async fn session_baseline_is_answered_only_to_the_owner() {
    let mut rig = Rig::start("baseline-owner").await;
    let owner = subc_protocol::Principal::Reserved {
        module_id: "baseline-owner".into(),
    };
    rig.bind_as(7, Some(owner)).await;
    rig.script.push(&[Step::Text("one")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let answered = call(&mut rig, "session.baseline", serde_json::json!({})).await;
    assert_eq!(result(&answered)["state"], "ready");

    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "stranger".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    let refused = rig
        .request(
            13,
            serde_json::json!({"method": "session.baseline", "params": {}}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "scope_owner_mismatch");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

/// An interrupt that omits the model, sent while a run is active, is recorded as a queued send
/// carrying the session's frozen model (and its variant), not the serve layer's empty
/// placeholder, beside `inherit_frozen`, the parameters it takes from the session at admission.
/// A build that predates `inherit_frozen` decodes the record without that set and runs it on
/// the recorded model, so the model must be the real one; this build runs it on the same model.
#[tokio::test]
async fn a_pending_interrupt_without_a_model_records_the_frozen_model() {
    let mut rig = Rig::start("pending-frozen-model").await;
    rig.script.hold_model(true);
    let first = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "task", "send_id": "turn-1",
            "model": {"provider": "fake", "model": "fake", "variant": "high"}}),
    )
    .await;
    assert_eq!(result(&first)["state"], "active");
    rig.wait_entered().await;
    let pending = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "stop", "send_id": "stop-1", "delivery": "interrupt"}),
    )
    .await;
    assert_eq!(result(&pending)["state"], "pending", "{}", result(&pending));
    // A retry of the pending interrupt is the same send. The interrupt cancels the held run, so
    // by the time the retry arrives it may already have started: the retry then answers active
    // with the run that the pending reply's submission named (`sub-<run_id>`), never a new run.
    let retry = call(
        &mut rig,
        "session.send",
        serde_json::json!({"prompt": "stop", "send_id": "stop-1", "delivery": "interrupt"}),
    )
    .await;
    let submission = result(&pending)["submission_id"]
        .as_str()
        .expect("a pending reply names its submission")
        .to_owned();
    match result(&retry)["state"].as_str() {
        Some("pending") => assert_eq!(result(&retry), result(&pending)),
        Some("active") => assert_eq!(
            result(&retry)["run_id"]
                .as_str()
                .map(|run| format!("sub-{run}")),
            Some(submission),
            "{}",
            result(&retry)
        ),
        _ => panic!("retry of a pending interrupt: {}", result(&retry)),
    }

    let frames = broca_wal::replay_file(&rig.wal_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let queued = frames
        .iter()
        .find_map(|framed| match &framed.record {
            broca_wal::WalRecord::Queued { request, .. } => Some(request.clone()),
            _ => None,
        })
        .expect("the interrupt is recorded as a queued send");
    assert_eq!(
        queued.model,
        broca_protocol::request::ModelHandle::new("fake", "fake")
    );
    assert_eq!(queued.variant.as_deref(), Some("high"));
    assert!(queued
        .inherit_frozen
        .contains(&broca_protocol::request::InheritedParam::Model));
    assert!(queued
        .inherit_frozen
        .contains(&broca_protocol::request::InheritedParam::Generation));

    rig.script.hold_model(false);
    let records = rig.wait_for_finished(2).await;
    let models: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => {
                Some((config.model.clone(), config.variant.clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(models.len(), 2);
    assert_eq!(
        models[0], models[1],
        "the interrupt ran on the frozen model"
    );
    rig.finish().await;
}

// ---- client_message_id ----------------------------------------------------------------------

/// A send's `client_message_id` (llm-runner 0.5.1 §9.11) through the real serve path. It is
/// stored only in the identity digest of the `RunStarted` record the send wrote, which then
/// requires `client-message-id/v1`; it is never in `session.read`. A retry with the same id is
/// answered as a retry; one with another id, or without the id, is refused `send_id_reuse`
/// naming `client_message_id` and writes nothing. A malformed id, and an id on a send without
/// a `send_id`, are refused `invalid_params` naming it and write nothing.
#[tokio::test]
async fn client_message_id_is_part_of_the_send_identity_and_never_read() {
    let mut rig = Rig::start("client-message-id").await;
    rig.script.push(&[Step::Text("answer")]);
    let with_id = serde_json::json!({"client_message_id": "msg-1"});
    let first = rig.send_with("turn-1", "hi", None, with_id.clone()).await;
    let run_id = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;

    let records = rig.records();
    let digest = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted {
                request_identity_digest,
                ..
            } => request_identity_digest.clone(),
            _ => None,
        })
        .expect("the keyed send recorded its identity");
    assert_eq!(
        broca_core::digest_client_message_id(&digest).as_deref(),
        Some("msg-1")
    );
    let started = records
        .iter()
        .find(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
        .unwrap();
    assert!(started
        .required_features()
        .contains(&broca_wal::framing::CLIENT_MESSAGE_ID_FEATURE));

    let read = call(&mut rig, "session.read", serde_json::json!({})).await;
    assert!(
        !String::from_utf8_lossy(&read.body).contains("msg-1"),
        "session.read carries the client_message_id: {}",
        String::from_utf8_lossy(&read.body)
    );

    let retry = rig.send_with("turn-1", "hi", None, with_id.clone()).await;
    assert_eq!(result(&retry)["run_id"], run_id.as_str());

    for (extra, code) in [
        (
            serde_json::json!({"client_message_id": "msg-2"}),
            "send_id_reuse",
        ),
        (serde_json::json!({}), "send_id_reuse"),
        (
            serde_json::json!({"client_message_id": "bad id"}),
            "invalid_params",
        ),
        (
            serde_json::json!({"client_message_id": "msg-1", "send_id": null}),
            "invalid_params",
        ),
    ] {
        let body =
            refused_writing_nothing(&mut rig, "session.send", keyed("turn-1", extra.clone())).await;
        assert_eq!(body["code"], code, "{extra}: {body}");
        assert_eq!(
            body["detail"]["field"], "client_message_id",
            "{extra}: {body}"
        );
    }

    // A send queued behind a running turn keeps its id in the `Queued` record's identity, and
    // a retry of the still-pending send is compared against it.
    rig.script.push(&[Step::Text("busy")]);
    rig.script.push(&[Step::Text("later")]);
    rig.script.hold_model(true);
    rig.send("turn-2", "busy", None).await;
    rig.wait_entered().await;
    let queued_with = serde_json::json!({"client_message_id": "msg-3"});
    let queued = call(
        &mut rig,
        "session.send",
        keyed("turn-3", queued_with.clone()),
    )
    .await;
    assert_eq!(result(&queued)["state"], "pending", "{}", result(&queued));
    let queued_record = rig
        .records()
        .into_iter()
        .find(|record| matches!(record, broca_wal::WalRecord::Queued { .. }))
        .expect("the send was queued");
    assert!(queued_record
        .required_features()
        .contains(&broca_wal::framing::CLIENT_MESSAGE_ID_FEATURE));
    let again = call(&mut rig, "session.send", keyed("turn-3", queued_with)).await;
    assert_eq!(result(&again)["state"], "pending", "{}", result(&again));
    for extra in [
        serde_json::json!({"client_message_id": "msg-4"}),
        serde_json::json!({}),
    ] {
        let body =
            refused_writing_nothing(&mut rig, "session.send", keyed("turn-3", extra.clone())).await;
        assert_eq!(body["code"], "send_id_reuse", "{extra}: {body}");
        assert_eq!(
            body["detail"]["field"], "client_message_id",
            "{extra}: {body}"
        );
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(3).await;
    rig.finish().await;
}

/// `session.send` params for the rig's `work` tool session under `send_id`, with `extra` merged
/// in; a `null` in `extra` removes that member.
fn keyed(send_id: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut params = serde_json::json!({
        "prompt": "hi",
        "send_id": send_id,
        "model": {"provider": "fake", "model": "fake"},
        "tools": [{"name": "work", "description": "does work", "input_schema": {"type": "object"}}]
    });
    for (key, value) in extra.as_object().unwrap() {
        if value.is_null() {
            params.as_object_mut().unwrap().remove(key);
        } else {
            params[key] = value.clone();
        }
    }
    params
}

async fn assert_projected_control_mids_match_read(rig: &mut Rig) {
    use broca_protocol::caller::ControlUnit;
    use broca_wal::WalRecord;
    let page = read(rig, serde_json::json!({})).await;
    let stored: Vec<_> = page
        .messages
        .iter()
        .map(|entry| {
            (
                entry,
                serde_json::from_value::<Message>(entry.message.clone()).unwrap(),
            )
        })
        .collect();
    let records = rig.records();
    let frames: Vec<_> = records
        .iter()
        .enumerate()
        .map(|(index, record)| broca_wal::FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect();
    let units = broca_core::project_control_with_mids(&frames, &mut Default::default()).unwrap();
    let mut checked = 0;
    for frame in units {
        let matches = |mid: &str, message: &Message| {
            match &frame.unit {
                ControlUnit::MessageBoundary { mid: boundary } => boundary == mid,
            ControlUnit::AssistantMessage { message: assistant } => message.role == Role::Assistant && message.content == assistant.content,
            ControlUnit::ToolCall { call } => message.role == Role::Assistant && message.content.iter().any(|block| matches!(block, ContentBlock::ToolCall(stored) if stored.tool_call_id == call.tool_call_id)),
            ControlUnit::ToolResult { result } => message.role == Role::Tool && message.content.iter().any(|block| matches!(block, ContentBlock::ToolResult(stored) if stored.tool_call_id == result.tool_call_id)),
            ControlUnit::StepStarted { step_id } | ControlUnit::StepFinished { step_id, .. } => records.iter().any(|record| matches!(record, WalRecord::ModelStepFinished { step_id: finished, assistant_message, .. } if finished == step_id && message.role == Role::Assistant && message.content == assistant_message.content)),
            ControlUnit::BudgetFired { ordinal, .. } => records.iter().any(|record| matches!(record, WalRecord::BudgetFired { ordinal: stored_ordinal, caller_text: Some(text), .. } if stored_ordinal == ordinal && message.role == Role::User && message.content == vec![ContentBlock::Text { text: text.clone() }])),
            _ => false,
        }
        };
        if let Some(ordinal) = frame.message_ordinal {
            let (entry, _) = stored
                .iter()
                .find(|(entry, message)| matches(&entry.mid, message))
                .unwrap_or_else(|| {
                    panic!(
                        "control payload has no session.read message: {:?}",
                        frame.unit
                    )
                });
            assert_eq!(ordinal, entry.ordinal, "{:?}", frame.unit);
            assert_eq!(
                frame.mid().as_deref(),
                Some(entry.mid.as_str()),
                "{:?}",
                frame.unit
            );
            checked += 1;
        } else {
            assert!(
                !matches!(
                    frame.unit,
                    ControlUnit::AssistantMessage { .. }
                        | ControlUnit::MessageBoundary { .. }
                        | ControlUnit::ToolCall { .. }
                        | ControlUnit::ToolResult { .. }
                        | ControlUnit::StepStarted { .. }
                        | ControlUnit::StepFinished { .. }
                        | ControlUnit::BudgetFired { .. }
                ),
                "message control has no placement: {:?}",
                frame.unit
            );
        }
    }
    assert!(
        checked >= 8,
        "the comparison must reach the complete tool turn"
    );
}

#[tokio::test]
async fn projected_control_mids_match_session_read_tool_steer_budget_and_resume() {
    let (mut tool, _) = one_tool_turn("placed-tool").await;
    assert_projected_control_mids_match_read(&mut tool).await;
    tool.finish().await;

    let mut steer = Rig::start("placed-steer").await;
    steer
        .script
        .push(&[Step::Call("steer-call"), Step::Text("steered answer")]);
    steer.script.hold_tool(true);
    steer.send("steer-turn", "task", None).await;
    steer.wait_entered().await;
    steer.send("steer-input", "new steer", Some("steer")).await;
    steer.script.hold_tool(false);
    steer.wait_for_finished(1).await;
    steer.wait_idle().await;
    assert_projected_control_mids_match_read(&mut steer).await;
    steer.finish().await;

    let mut budget = Rig::start("placed-budget").await;
    budget
        .script
        .push(&[Step::Call("budget-call"), Step::Text("budget answer")]);
    budget.send_with("budget-turn", "task", None, serde_json::json!({"budget":{"nudges":[{"at_tool_calls":1,"text":"budget coordinate"}],"finalize_at_tool_calls":5,"finalize_text":"finish"}})).await;
    budget.wait_for_finished(1).await;
    budget.wait_idle().await;
    assert!(budget
        .records()
        .iter()
        .any(|record| matches!(record, broca_wal::WalRecord::BudgetFired { .. })));
    assert_projected_control_mids_match_read(&mut budget).await;
    budget.finish().await;

    let mut resumed = Rig::start("placed-resume").await;
    resumed.script.push(&[Step::Call("resume-call")]);
    resumed.script.hold_tool(true);
    let reply = resumed
        .send_with(
            "resume-turn",
            "task",
            None,
            serde_json::json!({"on_restart":"resume"}),
        )
        .await;
    let run_id = result(&reply)["run_id"].as_str().unwrap().to_owned();
    resumed.wait_entered().await;
    resumed = resumed.restart_at_tool_boundary().await;
    resumed
        .send("resumed-steer", "restart steer", Some("steer"))
        .await;
    resumed.script.push(&[Step::Text("resumed answer")]);
    assert_eq!(
        resumed
            .registry()
            .resume_restart_pauses(
                std::sync::Arc::new(broca_core::FileWalStore::new(resumed.dir.join("wal"))),
                vec![(run_id, resumed.session.clone())],
            )
            .await,
        (1, 0)
    );
    resumed.wait_for_finished(2).await;
    resumed.wait_idle().await;
    assert_projected_control_mids_match_read(&mut resumed).await;
    resumed.finish().await;
}

#[tokio::test]
async fn unsupported_output_refs_precedes_plan_overrides_and_never_writes() {
    use cortexkit_role_llm_runner::{capabilities as groups, describe::RoleDescribe, ops};
    let mut rig = Rig::start_with_scopes("unsupported-output-refs").await;
    let describe = call(&mut rig, ops::ROLE_DESCRIBE, serde_json::json!({})).await;
    let describe: RoleDescribe = serde_json::from_value(result(&describe)).unwrap();
    assert!(describe.declares(groups::OUTPUT_REFS));
    // This rig installs no plan admitter. Even though production discovery advertises the
    // reader, a handler without an output-reference admitter must refuse before overrides.
    for flag in [true, false] {
        let before = result(&call(&mut rig, ops::SESSION_HEAD, serde_json::json!({})).await);
        let wal = rig.wal_bytes();
        let plan = serde_json::json!({"items":[{"output_refs":flag}]});
        let mut requests = vec![(
            ops::SESSION_SEND,
            serde_json::json!({
                "send_id":format!("unsupported-send-{flag}"),"prompt":"must not be stored",
                "model":{"provider":"fake","model":"fake"},
                "tools":[],"plan":plan.clone()
            }),
        )];
        if describe.declares(groups::SESSION_CHANGE) {
            requests.push((
                ops::SESSION_REFRESH,
                serde_json::json!({"plan":plan.clone(),"policy":"immediately","generation":1}),
            ));
        }
        if describe.declares(groups::ADOPT) {
            requests.push((
                ops::SESSION_ADOPT,
                serde_json::json!({"plan":plan,"send_id":format!("unsupported-adopt-{flag}")}),
            ));
        }
        for (method, params) in requests {
            let reply = call(&mut rig, method, params).await;
            let error = refusal(&reply);
            assert_eq!(error["code"], "invalid_params", "{method}: {error}");
            assert_eq!(
                error["detail"]["field"], "plan.items[].output_refs",
                "{method}: {error}"
            );
            assert_eq!(rig.wal_bytes(), wal, "{method} must refuse before writing");
            let after = result(&call(&mut rig, ops::SESSION_HEAD, serde_json::json!({})).await);
            assert_eq!(after, before, "{method} must preserve the session head");
        }
    }
    rig.finish().await;
}

fn assert_stream_sequences(frames: &[Frame]) -> Vec<serde_json::Value> {
    let values: Vec<serde_json::Value> = frames
        .iter()
        .map(|frame| {
            assert_eq!(
                frame.header.ty,
                FrameType::StreamData,
                "{}",
                String::from_utf8_lossy(&frame.body)
            );
            serde_json::from_slice(&frame.body).unwrap()
        })
        .collect();
    assert!(!values.is_empty());
    for (index, value) in values.iter().enumerate() {
        assert_eq!(value["seq"], index as u64 + 1, "{value}");
        cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&value["event"]).unwrap();
    }
    values
}

#[tokio::test]
async fn one_stream_sequence_counts_both_lanes_and_restarts_for_each_subscribe() {
    let mut rig = Rig::start("live-sequence").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "streamed text".into(),
    }])]);
    rig.script.hold_model(true);
    rig.send("sequence-turn", "task", None).await;
    let script = rig.script.clone();
    let first = rig
        .subscribe_collect_after_attach(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_secs(1),
            move || script.hold_model(false),
        )
        .await;
    let values = assert_stream_sequences(&first);
    assert!(values.iter().any(|value| value["kind"] == "control"));
    assert!(
        values.iter().any(|value| value["kind"] == "display"),
        "the forwarder must count actual display arrivals"
    );
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "run_finished"));
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let second = rig
        .subscribe_collect(
            10,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
        )
        .await;
    assert_stream_sequences(&second);
    rig.finish().await;
}

#[tokio::test]
async fn live_tool_observations_and_control_bytes_survive_a_real_serve_restart() {
    let (mut rig, _) = one_tool_turn("live-tool-restart").await;
    let records = rig.records();
    let started = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::ToolDispatchIntent {
                dispatched_at_ms, ..
            } => *dispatched_at_ms,
            _ => None,
        })
        .expect("the real tool call recorded its dispatch observation");
    let finished = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::ToolResult { finished_at_ms, .. } => *finished_at_ms,
            _ => None,
        })
        .expect("the real tool call recorded its finish observation");
    let before = rig
        .subscribe_collect(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
        )
        .await;
    let values = assert_stream_sequences(&before);
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "tool_started"
            && value["event"]["started_at"] == started));
    assert!(values
        .iter()
        .any(|value| value["event"]["type"] == "tool_finished"
            && value["event"]["finished_at"] == finished));
    // Closing a connection alone does not release a live registry's writer lease.
    let bytes = rig.wal_bytes();
    assert!(rig.registry().stop_and_seal().await);
    assert_eq!(
        rig.wal_bytes(),
        bytes,
        "stopping an already-finished run writes nothing"
    );
    rig = rig.restart().await;
    let after = rig
        .subscribe_collect(
            9,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(500),
        )
        .await;
    assert_stream_sequences(&after);
    assert_eq!(
        before.iter().map(|frame| &frame.body).collect::<Vec<_>>(),
        after.iter().map(|frame| &frame.body).collect::<Vec<_>>()
    );
    rig.finish().await;
}

#[tokio::test]
async fn stateful_text_control_is_plain_text_live_and_in_attach_replay() {
    use broca_protocol::content::{TextProviderFormat, TextProviderState};
    use broca_protocol::{ToolCall, ToolCallId};

    fn assistant_frame(frames: &[Frame]) -> Vec<u8> {
        frames
            .iter()
            .find(|frame| {
                let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
                value["kind"] == "control" && value["unit"]["type"] == "assistant_message"
            })
            .expect("assembled assistant message crossed serve")
            .body
            .clone()
    }

    #[derive(serde::Deserialize)]
    struct ControlFrame {
        unit: Box<serde_json::value::RawValue>,
        event: serde_json::Value,
    }

    let mut captures = Vec::new();
    for (name, format, data) in [
        (
            "responses",
            TextProviderFormat::OpenAiResponses,
            r#"{"id":"synthetic-response-item","type":"message","phase":"final_answer"}"#,
        ),
        (
            "gemini",
            TextProviderFormat::Gemini,
            r#"{"thoughtSignature":"synthetic-text-signature"}"#,
        ),
    ] {
        let content = vec![
            ContentBlock::TextWithState {
                text: "answer".into(),
                provider_state: TextProviderState {
                    format,
                    data: data.into(),
                },
            },
            ContentBlock::Reasoning {
                text: "thinking".into(),
                signature: Some("synthetic-reasoning-signature".into()),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("synthetic-call".into()),
                tool_name: "work".into(),
                input: serde_json::json!({"path": "/synthetic/input.txt"}),
                thought_signature: Some("synthetic-tool-signature".into()),
            }),
        ];
        let mut rig = Rig::start(&format!("subscribe-stateful-{name}")).await;
        rig.script
            .push(&[Step::Stream(content.clone()), Step::Text("done")]);
        rig.script.hold_stream(true);
        rig.send("synthetic-send", "hi", None).await;
        rig.wait_entered().await;
        let script = rig.script.clone();
        let live = assistant_frame(
            &rig.subscribe_collect_after_display_reset(
                20,
                serde_json::json!({"from":"start"}),
                Duration::from_secs(5),
                move || script.hold_stream(false),
            )
            .await,
        );
        let records = rig.wait_for_finished(1).await;
        let stored = records.iter().find_map(|record| match record {
            broca_wal::WalRecord::ModelStepFinished {
                assistant_message, ..
            } => Some(&assistant_message.content),
            _ => None,
        });
        assert_eq!(stored, Some(&content));
        rig.wait_idle().await;
        let page = read(&mut rig, serde_json::json!({})).await;
        let descriptors = read(&mut rig, serde_json::json!({"media":"descriptors"})).await;
        let wal = rig.wal_bytes();
        assert!(rig.registry().stop_and_seal().await);
        rig = rig.restart().await;
        let replay = assistant_frame(
            &rig.subscribe_collect(
                21,
                serde_json::json!({"from":"start"}),
                Duration::from_secs(5),
            )
            .await,
        );
        assert_eq!(
            rig.wal_bytes(),
            wal,
            "attaching never rewrites provider state"
        );
        rig.finish().await;
        eprintln!(
            "SUBSCRIBE_FLATTEN_SAMPLE {name}.live {}",
            std::str::from_utf8(&live).unwrap()
        );
        eprintln!(
            "SUBSCRIBE_FLATTEN_SAMPLE {name}.replay {}",
            std::str::from_utf8(&replay).unwrap()
        );
        captures.push((content, live, replay, page, descriptors));
    }

    for (content, live, replay, page, descriptors) in captures {
        let live: ControlFrame = serde_json::from_slice(&live).unwrap();
        let replay: ControlFrame = serde_json::from_slice(&replay).unwrap();
        assert_eq!(live.unit.get().as_bytes(), replay.unit.get().as_bytes());
        assert_eq!(
            live.event,
            serde_json::json!({"type":"message_finished","mid":"m1"})
        );
        assert_eq!(replay.event, live.event);
        cortexkit_role_llm_runner::live::LiveEvent::decode_strict(&live.event).unwrap();
        let unit: serde_json::Value = serde_json::from_str(live.unit.get()).unwrap();
        let expected = serde_json::json!([
            {"type":"text","text":"answer"},
            content[1],
            content[2],
        ]);
        assert_eq!(unit["message"]["content"], expected);
        assert!(!live.unit.get().contains("provider_state"));
        let entry = page
            .messages
            .iter()
            .find(|entry| entry.mid == "m1")
            .unwrap();
        assert_eq!(entry.message["content"], expected);
        let entry = descriptors
            .messages
            .iter()
            .find(|entry| entry.mid == "m1")
            .unwrap();
        assert_eq!(entry.message["content"], expected);
    }
}

async fn pin_live_sample(rig: &mut Rig, frames: &[Frame], run_id: &str, file: &str) {
    let page = read(rig, serde_json::json!({})).await;
    let lineage = page.lineage_id.as_ref().unwrap();
    let values = assert_stream_sequences(frames);
    let mut lines = Vec::new();
    for (frame, value) in frames.iter().zip(&values) {
        let event = &value["event"];
        cortexkit_role_llm_runner::live::LiveEvent::decode_strict(event).unwrap();
        if let (Some(mid), Some(part)) = (event["mid"].as_str(), event["part"].as_str()) {
            let entry = page
                .messages
                .iter()
                .find(|m| m.mid == mid)
                .expect("live mid is stored");
            let stored: Message = serde_json::from_value(entry.message.clone()).unwrap();
            let index: usize = part.parse().unwrap();
            assert!(
                index < stored.content.len(),
                "display part lies in the stored message"
            );
            match (&stored.content[index], event["type"].as_str().unwrap()) {
                (ContentBlock::Text { .. }, "text_delta")
                | (ContentBlock::Reasoning { .. }, "reasoning_delta")
                | (ContentBlock::ToolCall(_), "tool_input_delta") => {}
                pair => panic!("delta addresses a different block: {pair:?}"),
            }
        }
        let mut body = normalized(&frame.body, run_id, lineage);
        for field in ["started_at", "finished_at"] {
            if let Some(at) = event[field].as_u64() {
                body = body.replace(
                    &format!("\"{field}\":{at}"),
                    &format!("\"{field}\":1700000000000"),
                );
            }
        }
        lines.push(body);
    }
    let sample = lines.join("\n") + "\n";
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(file), &sample).unwrap();
    }
    assert_eq!(sample.trim_end(), pinned(file));
}

#[tokio::test]
async fn live_display_samples_match_stored_parts_and_attach_replays_current_step() {
    use broca_protocol::{ToolCall, ToolCallId};
    let tool = ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId("sample-call".into()),
        tool_name: "work".into(),
        input: serde_json::json!({}),
    });
    for name in ["text", "tool", "reasoning", "steer", "attach"] {
        let mut rig = Rig::start(&format!("live-sample-{name}")).await;
        let content = match name {
            "tool" | "steer" => vec![tool.clone()],
            "reasoning" => vec![
                ContentBlock::Reasoning {
                    text: "thinking".into(),
                    signature: Some("opaque".into()),
                },
                ContentBlock::Text {
                    text: "answer".into(),
                },
            ],
            _ => vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        };
        let last_part = content.len() as u64 - 1;
        rig.script
            .push(&[Step::Stream(content), Step::Text("done")]);
        rig.script.hold_stream(true);
        let reply = rig.send("sample", "hi", None).await;
        let run_id = result(&reply)["run_id"].as_str().unwrap().to_owned();
        rig.wait_entered().await;
        if name == "steer" {
            rig.send("sample-steer", "steered", Some("steer")).await;
        }
        let frames = if name == "attach" {
            let mut frames = Vec::new();
            for channel in 20..28 {
                frames = rig
                    .subscribe_collect(channel, serde_json::json!({}), Duration::from_millis(100))
                    .await;
                if frames.iter().any(|f| {
                    serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["part"]
                        == "0"
                }) {
                    break;
                }
            }
            let display: Vec<_> = frames
                .iter()
                .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
                .filter(|v| v["kind"] == "display")
                .collect();
            assert_eq!(display.len(), 2, "reset then one coalesced part");
            assert_eq!(display[0]["event"]["type"], "display_reset");
            rig.script.hold_stream(false);
            frames
        } else {
            rig.subscribe_release_after_part(20, last_part).await
        };
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        assert!(frames
            .iter()
            .any(
                |f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["mid"]
                    == "m1"
            ));
        pin_live_sample(
            &mut rig,
            &frames,
            &run_id,
            &format!("session_subscribe.live_{name}.jsonl"),
        )
        .await;
        rig.finish().await;
    }
}

#[tokio::test]
async fn live_display_mid_after_reopen_comes_from_seeded_placement() {
    let mut rig = Rig::start("display-reopen").await;
    rig.script.push(&[Step::Text("first")]);
    rig.send("first", "first prompt", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.wait_closed().await;
    let mut rig = rig.restart().await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "second".into(),
    }])]);
    rig.script.hold_stream(true);
    let admitted = rig.send("second", "second prompt", None).await;
    assert_eq!(
        admitted.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&admitted.body)
    );
    rig.wait_entered().await;
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    let stored = page.messages.last().unwrap();
    let message: Message = serde_json::from_value(stored.message.clone()).unwrap();
    assert_eq!(message.role, Role::Assistant);
    assert_eq!(
        message.content,
        vec![ContentBlock::Text {
            text: "second".into()
        }]
    );
    let delta = frames
        .iter()
        .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
        .find(|v| v["event"]["type"] == "text_delta")
        .unwrap();
    assert_eq!(delta["event"]["mid"], stored.mid);
    assert_eq!(delta["event"]["mid"], "m3");
    rig.finish().await;
}

#[tokio::test]
async fn live_finalize_filter_restreams_the_stored_block_indices() {
    let mut rig = Rig::start("display-finalize").await;
    rig.script.push(&[
        Step::Call("first-call"),
        Step::Stream(vec![
            ContentBlock::ToolCall(broca_protocol::ToolCall {
                thought_signature: None,
                tool_call_id: broca_protocol::ToolCallId("omitted-call".into()),
                tool_name: "work".into(),
                input: serde_json::json!({}),
            }),
            ContentBlock::Text {
                text: "final answer".into(),
            },
        ]),
    ]);
    rig.script.hold_tool(true);
    rig.send_with(
        "finalize",
        "task",
        None,
        serde_json::json!({"budget":{"finalize_at_tool_calls":1,"finalize_text":"finish"}}),
    )
    .await;
    rig.wait_entered().await;
    rig.script.hold_stream(true);
    rig.script.hold_tool(false);
    rig.wait_entered().await;
    let frames = rig.subscribe_release_after_part(20, 1).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let values = assert_stream_sequences(&frames);
    let last_reset = values
        .iter()
        .rposition(|v| v["kind"] == "display" && v["event"]["type"] == "display_reset")
        .unwrap();
    let final_deltas: Vec<_> = values[last_reset + 1..]
        .iter()
        .filter(|v| v["kind"] == "display")
        .collect();
    assert_eq!(
        final_deltas.len(),
        1,
        "the omitted tool's live row is wiped"
    );
    assert_eq!(final_deltas[0]["event"]["type"], "text_delta");
    assert_eq!(final_deltas[0]["event"]["part"], "0");
    let page = read(&mut rig, serde_json::json!({})).await;
    let stored = page.messages.last().unwrap();
    assert_eq!(final_deltas[0]["event"]["mid"], stored.mid);
    let message: Message = serde_json::from_value(stored.message.clone()).unwrap();
    assert_eq!(
        message.content,
        vec![ContentBlock::Text {
            text: "final answer".into()
        }]
    );
    rig.finish().await;
}

#[tokio::test]
async fn live_subscribe_limits_cut_only_typed_arguments_not_existing_units() {
    let (mut rig, _) = one_tool_turn("live-limits").await;
    let unlimited = rig
        .subscribe_collect(
            20,
            serde_json::json!({"from":"start"}),
            Duration::from_millis(100),
        )
        .await;
    let unlimited = assert_stream_sequences(&unlimited);
    let unlimited_started = unlimited
        .iter()
        .find(|v| v["event"]["type"] == "tool_started")
        .expect("tool lifecycle replays without limits");
    assert!(
        unlimited_started["event"]["call_key"].is_string(),
        "the projector attaches the call key: {unlimited_started}"
    );
    let frames = rig.subscribe_collect(20, serde_json::json!({"from":"start","limits":{"arguments_bytes":1,"result_bytes":1,"error_result_bytes":1}}), Duration::from_millis(100)).await;
    let values = assert_stream_sequences(&frames);
    let started = values
        .iter()
        .find(|v| v["event"]["type"] == "tool_started")
        .expect("tool lifecycle replays");
    assert_eq!(started["event"]["arguments"], "{");
    assert_eq!(started["event"]["arguments_truncated"], true);
    // Cutting the arguments must not drop the call identity a client joins on.
    assert_eq!(
        started["event"]["call_key"],
        unlimited_started["event"]["call_key"]
    );
    assert_eq!(
        started["event"]["schema_pin"],
        unlimited_started["event"]["schema_pin"]
    );
    let call = values
        .iter()
        .find(|v| v["unit"]["type"] == "tool_call")
        .expect("the existing authoritative call unit replays separately");
    assert_eq!(call["unit"]["call"]["input"], serde_json::json!({}));
    rig.finish().await;
}

#[tokio::test]
async fn live_display_after_steer_uses_the_next_step_placement() {
    let mut rig = Rig::start("display-steer-next").await;
    rig.script.push(&[
        Step::Stream(vec![ContentBlock::ToolCall(broca_protocol::ToolCall {
            thought_signature: None,
            tool_call_id: broca_protocol::ToolCallId("steer-call".into()),
            tool_name: "work".into(),
            input: serde_json::json!({}),
        })]),
        Step::Stream(vec![ContentBlock::Text {
            text: "after steer".into(),
        }]),
    ]);
    rig.script.hold_stream(true);
    rig.send("first", "task", None).await;
    rig.wait_entered().await;
    rig.send("steer", "new direction", Some("steer")).await;
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let values = assert_stream_sequences(&frames);
    let delta = values
        .iter()
        .find(|v| v["event"]["type"] == "text_delta" && v["event"]["delta"] == "after steer")
        .unwrap();
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(page.messages.last().unwrap().mid, "m4");
    assert_eq!(delta["event"]["mid"], page.messages.last().unwrap().mid);
    rig.finish().await;
}

#[tokio::test]
async fn live_attach_after_cancel_discards_the_uncommitted_step() {
    let mut rig = Rig::start("display-cancelled-step").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "discarded partial".into(),
    }])]);
    rig.script.hold_stream(true);
    let reply = rig.send("partial", "task", None).await;
    let run_id = result(&reply)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    // The held model has streamed its partial; wait for that live delta itself, since on a
    // loaded runner it can arrive after any fixed quiet window.
    let before = rig
        .subscribe_collect_until(20, serde_json::json!({}), Duration::from_millis(100), |f| {
            serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["delta"]
                == "discarded partial"
        })
        .await;
    assert!(before.iter().any(
        |f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap()["event"]["delta"]
            == "discarded partial"
    ));
    rig.cancel(&run_id).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let page = read(&mut rig, serde_json::json!({})).await;
    assert!(page
        .messages
        .iter()
        .all(
            |entry| serde_json::from_value::<Message>(entry.message.clone())
                .unwrap()
                .role
                != Role::Assistant
        ));
    let frames = rig
        .subscribe_collect(21, serde_json::json!({}), Duration::from_millis(100))
        .await;
    let values = assert_stream_sequences(&frames);
    assert_eq!(
        values.len(),
        1,
        "an ended run has no current-step display prefix to replay"
    );
    assert_eq!(values[0]["event"]["type"], "display_reset");
    rig.finish().await;
}

#[tokio::test]
async fn late_subscriber_gets_context_captured_before_the_step_streamed() {
    let mut rig = Rig::start("display-context-late-attach").await;
    rig.script.push(&[Step::Stream(vec![ContentBlock::Text {
        text: "before attach".into(),
    }])]);
    rig.script.hold_stream(true);
    rig.send("late", "task", None).await;
    rig.wait_entered().await;
    assert_eq!(
        rig.script.prompts.lock().unwrap().len(),
        1,
        "the provider ran before any subscribe call"
    );
    let frames = rig.subscribe_release_after_part(20, 0).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let delta = frames
        .iter()
        .map(|f| serde_json::from_slice::<serde_json::Value>(&f.body).unwrap())
        .find(|v| v["event"]["type"] == "text_delta")
        .expect("late attach replays the streamed part");
    assert_eq!(delta["event"]["mid"], "m1");
    assert_eq!(delta["event"]["part"], "0");
    assert_eq!(delta["event"]["delta"], "before attach");
    let page = read(&mut rig, serde_json::json!({})).await;
    assert_eq!(delta["event"]["mid"], page.messages.last().unwrap().mid);
    rig.finish().await;
}

fn upload_staging_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut entries = Vec::new();
    for dir in ["objects", "refs"] {
        for entry in std::fs::read_dir(root.join("media-uploads").join(dir)).unwrap() {
            let path = entry.unwrap().path();
            entries.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    entries.sort();
    entries
}

#[cfg(not(feature = "test-attachments"))]
#[tokio::test]
async fn upload_roundtrip_is_staged_but_not_advertised_or_admitted() {
    let mut rig = Rig::start("upload-roundtrip").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let begin = serde_json::json!({"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"});
    let missing = call(&mut rig, "session.upload_begin", begin.clone()).await;
    assert_eq!(refusal(&missing)["code"], "unknown_session");
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let wal_before = rig.wal_bytes();
    let created = call(&mut rig, "session.upload_begin", begin).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let append = serde_json::json!({"upload_id":id,"offset":0,"data":"YWJj"});
    let first = call(&mut rig, "session.upload_append", append.clone()).await;
    assert_eq!(result(&first)["received"], 3);
    let before = upload_staging_snapshot(&rig.dir);
    let retry = call(&mut rig, "session.upload_append", append).await;
    assert_eq!(result(&retry), result(&first));
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let conflict = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":"YWJk"}),
    )
    .await;
    assert_eq!(refusal(&conflict)["code"], "upload_offset_mismatch");
    assert_eq!(refusal(&conflict)["detail"]["received"], 3);
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let commit = serde_json::json!({"upload_id":id});
    let first = call(&mut rig, "session.upload_commit", commit.clone()).await;
    assert_eq!(
        result(&first),
        serde_json::json!({"state":"committed","sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","mime":"application/pdf","bytes":3})
    );
    let before = upload_staging_snapshot(&rig.dir);
    assert_eq!(
        result(&call(&mut rig, "session.upload_commit", commit).await),
        result(&first)
    );
    let refused = rig
        .send_with(
            "turn-2",
            "file",
            None,
            serde_json::json!({"content":[{"type":"upload","upload_id":id}]}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "invalid_params");
    assert!(refusal(&refused)["message"].as_str().unwrap().contains(&id));
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    assert_eq!(rig.wal_bytes(), wal_before);
    let describe = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert!(!checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS));
    rig.finish().await;
}

#[tokio::test]
async fn upload_requires_send_standing_before_any_staging_write() {
    let mut rig = Rig::start("upload-standing").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let begin = serde_json::json!({"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"});
    let created = call(&mut rig, "session.upload_begin", begin.clone()).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "stranger".into(),
        }),
    )
    .await;
    let before = upload_staging_snapshot(&rig.dir);
    let wal_before = rig.wal_bytes();
    for (method, params) in [
        ("session.upload_begin", begin),
        (
            "session.upload_append",
            serde_json::json!({"upload_id":id,"offset":0,"data":"YWJj"}),
        ),
        ("session.upload_commit", serde_json::json!({"upload_id":id})),
        ("session.upload_status", serde_json::json!({"upload_id":id})),
    ] {
        let refused = rig
            .request(13, serde_json::json!({"method":method,"params":params}))
            .await;
        assert_eq!(
            refusal(&refused)["code"],
            "scope_owner_mismatch",
            "{method}"
        );
        assert_eq!(upload_staging_snapshot(&rig.dir), before, "{method}");
        assert_eq!(rig.wal_bytes(), wal_before);
    }
    rig.finish().await;
}

#[tokio::test]
async fn upload_raw_chunk_cap_is_enforced_on_real_serve_path() {
    use base64::Engine;
    let mut rig = Rig::start("upload-cap").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.script.push(&[Step::Text("ready")]);
    rig.send("turn-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let size = cortexkit_role_llm_runner::attachments::CHUNK_MAX;
    let created = call(&mut rig, "session.upload_begin", serde_json::json!({"mime":"application/pdf","bytes":size+1,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"})).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let before = upload_staging_snapshot(&rig.dir);
    let data = base64::engine::general_purpose::STANDARD.encode(vec![1; size + 1]);
    let refused = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":data}),
    )
    .await;
    assert_eq!(refusal(&refused)["code"], "upload_chunk_too_large");
    assert!(refusal(&refused)["message"].as_str().unwrap().contains(&id));
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    let data = base64::engine::general_purpose::STANDARD.encode(vec![1; size]);
    let accepted = call(
        &mut rig,
        "session.upload_append",
        serde_json::json!({"upload_id":id,"offset":0,"data":data}),
    )
    .await;
    assert_eq!(result(&accepted)["received"], size);
    rig.finish().await;
}

#[tokio::test]
async fn upload_plan_named_sender_has_standing_on_the_frozen_session() {
    struct SenderPlanAdmitter;
    #[async_trait::async_trait]
    impl broca_fetch_plan::PlanAdmitter for SenderPlanAdmitter {
        async fn admit(
            &self,
            plan: &broca_fetch_plan::Plan,
            _session: &broca_protocol::BindIdentity,
            _scope: Option<&broca_protocol::SessionScope>,
        ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
            Ok(broca_fetch_plan::Admitted {
                manifest: broca_wal::PlanManifest {
                    rich_ui: None,
                    plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                    plan_identity: plan.identity.clone(),
                    composition_digest: plan.composition_digest.clone(),
                    fetches: vec![],
                    tools: std::sync::Arc::new(vec![]),
                    system_text: String::new(),
                    system_digest: String::new(),
                    join_version: 1,
                    role_versions: Default::default(),
                    declarations: vec![],
                },
            })
        }
    }
    let mut rig = Rig::start("upload-plan-sender").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    rig.registry()
        .set_plan_admitter(std::sync::Arc::new(SenderPlanAdmitter));
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let composition = serde_json::json!({"providers":[]});
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
    let plan = serde_json::json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],"composition":composition,"composition_digest":digest,"senders":[{"module":"ck-mc","idle_start":true}]});
    rig.script.push(&[Step::Text("ready")]);
    let started = call(&mut rig, "session.send", serde_json::json!({"prompt":"hi","send_id":"turn-1","model":{"provider":"fake","model":"fake"},"plan":plan})).await;
    assert_eq!(result(&started)["state"], "active");
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.bind_as(
        13,
        Some(subc_protocol::Principal::Reserved {
            module_id: "ck-mc".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    let created = rig.request(13, serde_json::json!({"method":"session.upload_begin","params":{"mime":"application/pdf","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}})).await;
    let id = result(&created)["upload_id"].as_str().unwrap().to_owned();
    let appended = rig.request(13, serde_json::json!({"method":"session.upload_append","params":{"upload_id":id,"offset":0,"data":"YWJj"}})).await;
    assert_eq!(result(&appended)["received"], 3);
    let committed = rig
        .request(
            13,
            serde_json::json!({"method":"session.upload_commit","params":{"upload_id":id}}),
        )
        .await;
    assert_eq!(result(&committed)["state"], "committed");
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[cfg(feature = "test-attachments")]
#[tokio::test]
async fn debug_pdf_send_is_refused_before_any_session_write() {
    use sha2::{Digest, Sha256};
    let mut rig = Rig::start("debug-pdf-send-gate").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let uploads = broca_core::media_uploads::Uploads::at(&rig.dir);
    let bytes = b"%PDF-1.7\n%%EOF\n";
    let now = broca_module::person_media::now_seconds();
    let id = uploads
        .begin(
            &rig.session,
            &cortexkit_role_llm_runner::attachments::UploadBeginRequest::new(
                "application/pdf",
                bytes.len() as u64,
                format!("{:x}", Sha256::digest(bytes)),
            ),
            now,
        )
        .unwrap()
        .upload_id;
    uploads.append(&rig.session, &id, 0, bytes, now).unwrap();
    uploads.commit(&rig.session, &id, now).unwrap();
    let before = upload_staging_snapshot(&rig.dir);
    assert!(!rig.wal_path.exists());
    let refused = rig
        .send_with(
            "pdf",
            "inspect",
            None,
            serde_json::json!({"content":[{"type":"upload","upload_id":id}]}),
        )
        .await;
    assert_eq!(refusal(&refused)["code"], "media_unsupported");
    let message = refusal(&refused)["message"].as_str().unwrap().to_owned();
    assert!(
        message.contains("pdf") && message.contains("modality") && message.contains("model"),
        "{message}"
    );
    assert!(!rig.wal_path.exists(), "a refused send must not open a WAL");
    assert_eq!(upload_staging_snapshot(&rig.dir), before);
    for class in ["media", "media-durable", "media-ephemeral"] {
        for dir in ["objects", "refs"] {
            assert_eq!(
                std::fs::read_dir(rig.dir.join(class).join(dir))
                    .unwrap()
                    .count(),
                0
            );
        }
    }
    let describe = call(&mut rig, "role.describe", serde_json::json!({})).await;
    let checked = cortexkit_role_llm_runner::describe::check_describe(&result(&describe)).unwrap();
    assert!(checked.declares(cortexkit_role_llm_runner::capabilities::ATTACHMENTS));
    rig.finish().await;
}
