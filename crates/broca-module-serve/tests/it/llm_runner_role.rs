//! The `llm-runner/v1` role surface through the real serve path: `session.read`,
//! `session.subscribe`, `session.head`, `run.result`, `role.describe` and their refusals.
//!
//! Every run goes through the real core loop over a real file WAL (the delivery rig), so each
//! record a test reads back was written by core or the lineage actor exactly as in production.

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
/// whose one run called a tool, pinned byte for byte. Run attribution is emitted without
/// requiring new request parameters; subscriptions retain their existing event bytes.
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
    // Every read now carries a `tool_calls` member on each message holding a tool call. With
    // that member removed, the bytes are exactly the ones pinned before it existed.
    let body = normalized(&read.body, &run_id, &lineage_id);
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
    assert_eq!(
        events.join("\n"),
        pinned("session_subscribe.tool_turn.jsonl")
    );
    rig.finish().await;
}

// ---- helpers ------------------------------------------------------------------------------

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

use cortexkit_role_llm_runner::{
    describe::check_describe,
    read::{HeadMeta, ReadPage},
    run::RunResult,
};

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
        "run_cancel",
        "retract",
        "dispatch_attribution",
        "streaming",
        "steer",
        "queue",
        "interrupt",
        "retention",
    ] {
        assert!(describe.declares(group), "{group}");
    }
    for group in [
        "model_view",
        "compaction",
        "session_change",
        "call-modes/v1",
    ] {
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
    assert_eq!(body["detail"]["message"]["session"], rig.session.session);
    assert_eq!(body["detail"]["message"]["ordinal"], 1);
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
            "applied_generation": 0})
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
