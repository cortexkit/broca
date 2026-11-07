use std::path::PathBuf;

use broca_protocol::{ContentBlock, MediaKind, MediaRef, Role, ToolOutput, ToolOutputPart};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::*;

fn fixture(path: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/head")
            .join(path),
    )
    .unwrap()
}

/// A media check that accepts any bytes under their declared type, as the store's would for
/// valid media.
fn accept(bytes: &[u8], mime: &str) -> Result<MediaRef, String> {
    Ok(MediaRef {
        kind: if mime.starts_with("image/") {
            MediaKind::Image
        } else {
            MediaKind::Document
        },
        mime: mime.to_owned(),
        byte_size: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        width: None,
        height: None,
    })
}

fn oc2(rows: Value) -> Result<HeadHistory, HeadError> {
    let bytes = serde_json::to_vec(&json!({"info": {}, "messages": rows})).unwrap();
    opencode2::read(&bytes, &mut accept)
}

fn texts(message: &HeadMessage) -> Vec<&str> {
    message
        .message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn mids(history: &HeadHistory) -> Vec<Option<&str>> {
    history
        .messages
        .iter()
        .map(|message| message.mid.as_deref())
        .collect()
}

fn moves(history: &HeadHistory) -> Vec<(&str, Option<usize>)> {
    history
        .rows
        .iter()
        .map(|row| (row.mid.as_str(), row.message))
        .collect()
}

#[test]
fn opencode2_export_maps_each_row_as_opencode_sends_it() {
    let history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
    assert_eq!(
        mids(&history),
        [
            Some("msg_u1"),
            Some("msg_a1"),
            None,
            Some("msg_loc"),
            Some("msg_shell"),
            Some("msg_syn"),
            Some("msg_a2"),
            None,
            Some("msg_a3"),
        ]
    );
    // One move-list row per row Thalamus serves as an entry; none for idle, agent switches,
    // background shells and compaction markers.
    assert_eq!(
        moves(&history),
        [
            ("msg_u1", Some(0)),
            ("msg_sys", None),
            ("msg_a1", Some(1)),
            ("msg_loc", Some(3)),
            ("msg_model", None),
            ("msg_shell", Some(4)),
            ("msg_syn", Some(5)),
            ("msg_a2", Some(6)),
            ("msg_a3", Some(8)),
        ]
    );
    assert_eq!(history.dropped_system_messages, 1);
    let user = &history.messages[0];
    assert_eq!(user.message.role, Role::User);
    assert_eq!(user.written_at_ms, Some(1_700_000_001_000));
    assert_eq!(user.created_at_ms, Some(1_700_000_001_000));
    let user_texts = texts(user);
    assert_eq!(user_texts[0], "Skill: review carefully.");
    assert_eq!(user_texts[1], "Look at this.");
    assert_eq!(
        user_texts[2],
        "\n\nAttached file: notes.txt\n\nhello from a file\n"
    );
    // The image is a media block with its bytes; the zip is not sent, so not imported.
    assert!(matches!(user.message.content[2], ContentBlock::Media(_)));
    assert_eq!(user.message.content.len(), 4);
    assert_eq!(user.media.len(), 1);
    assert_eq!((user.media[0].block, user.media[0].part), (2, None));
    assert_eq!(
        texts(&history.messages[3]),
        ["The working directory has been changed to /work/other."]
    );
    assert_eq!(
        texts(&history.messages[4]),
        ["The following shell command was executed by the user:\n\nCommand:\nls\n\nOutput:\na.txt\n"]
    );
}

#[test]
fn opencode2_reasoning_keeps_its_text_and_never_its_provider_payload() {
    let history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
    let assistant = &history.messages[1];
    assert_eq!(
        assistant.message.content[0],
        ContentBlock::Reasoning {
            text: "Thinking it over.".into(),
            signature: None,
        }
    );
    // A reasoning item with only a payload carries nothing to import.
    assert!(history.messages[6]
        .message
        .content
        .iter()
        .all(|block| !matches!(block, ContentBlock::Reasoning { .. })));
    let encoded = serde_json::to_string(&history.messages[1].message).unwrap();
    assert!(!encoded.contains("sig-abc"));
}

#[test]
fn opencode2_calls_keep_their_runner_keys_and_results_follow_as_a_tool_message() {
    let history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
    let assistant = &history.messages[1];
    let keys: Vec<_> = assistant
        .calls
        .iter()
        .map(|call| {
            (
                call.tool_call_id.as_str(),
                call.call_key.as_str(),
                call.carrier.as_str(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        [
            ("call_1", "msg_a1/call_1", "thalamus-harness"),
            ("call_2", "msg_a1/call_2", "thalamus-harness"),
            ("call_3", "msg_a1/call_3", "thalamus-harness"),
        ]
    );
    let ContentBlock::ToolCall(call) = &assistant.message.content[3] else {
        panic!("a call")
    };
    // A streamed input stored as JSON text is sent parsed.
    assert_eq!(call.input, json!({"full": true}));
    assert_eq!(
        assistant.usage,
        Some(HeadUsage {
            prompt_tokens: Some(110),
            cache_read_tokens: Some(100),
            cache_write_tokens: None,
            output_tokens: Some(25),
            reasoning_tokens: Some(5),
        })
    );
    let results = &history.messages[2];
    assert_eq!(results.message.role, Role::Tool);
    assert_eq!(results.written_at_ms, Some(1_700_000_004_000));
    assert_eq!(results.created_at_ms, None);
    let outputs: Vec<_> = results
        .message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::ToolResult(result) => (result.tool_call_id.0.as_str(), result),
            _ => panic!("only results"),
        })
        .collect();
    assert_eq!(outputs[0].0, "call_1");
    assert_eq!(
        outputs[0].1.output,
        ToolOutput::Text {
            text: "line one\nline two".into()
        }
    );
    let ToolOutput::Parts { parts } = &outputs[1].1.output else {
        panic!("a result with an image keeps its parts")
    };
    assert!(matches!(parts[1], ToolOutputPart::Media { .. }));
    assert_eq!(
        (results.media[0].block, results.media[0].part),
        (1, Some(1))
    );
    assert!(outputs[2].1.is_error);
    assert!(
        matches!(&outputs[2].1.output, ToolOutput::Error { message, .. } if message == "exit 1")
    );
    // All-zero counts are unknown.
    assert_eq!(history.messages[6].usage, None);
}

#[test]
fn a_reused_call_id_is_made_unique_and_its_result_renamed_but_its_key_kept() {
    let history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
    let again = &history.messages[6];
    assert_eq!(again.calls[0].tool_call_id, "call_1-2");
    assert_eq!(again.calls[0].call_key, "msg_a2/call_1");
    let ContentBlock::ToolResult(result) = &history.messages[7].message.content[0] else {
        panic!("a result")
    };
    assert_eq!(result.tool_call_id.0, "call_1-2");
}

#[test]
fn a_call_without_a_result_refuses_the_import() {
    let rows = json!([
        {"id": "a", "type": "assistant", "time": {"created": 1},
         "content": [{"type": "tool", "id": "c", "name": "t", "state": {"status": "running", "input": {}}}]}
    ]);
    assert_eq!(
        oc2(rows).unwrap_err(),
        HeadError::OpenCall {
            mid: "a".into(),
            call_id: "c".into()
        }
    );
}

#[test]
fn rows_the_import_cannot_map_refuse_it_by_name() {
    let refused = |row: Value| match oc2(json!([row])).unwrap_err() {
        HeadError::Row { mid, .. } => mid,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        refused(json!({"id": "x", "type": "patch", "time": {"created": 1}})),
        "x"
    );
    // OpenCode's own compaction replaced the history before it; a runner's marker didn't.
    assert_eq!(
        refused(
            json!({"id": "c", "type": "compaction", "status": "completed",
                       "metadata": {"keepID": "earlier"}})
        ),
        "c"
    );
    assert!(oc2(
        json!([{"id": "c", "type": "compaction", "status": "completed",
                         "metadata": {"keepID": "c"}}])
    )
    .unwrap()
    .rows
    .is_empty());
    assert_eq!(
        refused(
            json!({"id": "p", "type": "assistant", "time": {"created": 1},
                       "content": [{"type": "tool", "id": "c", "name": "web", "executed": true,
                                    "state": {"status": "completed", "input": {}, "content": []}}]})
        ),
        "p"
    );
    assert_eq!(
        refused(json!({"id": "r", "type": "user", "time": {"created": 1},
                       "text": {"type": "text_ref", "ref": "blob"}})),
        "r"
    );
}

#[test]
fn a_medium_that_cannot_be_stored_refuses_the_import_naming_its_message() {
    let rows = json!([{"id": "u", "type": "user", "time": {"created": 1},
                       "files": [{"mime": "image/png", "data": "AAAA"}]}]);
    let bytes = serde_json::to_vec(&json!({"info": {}, "messages": rows})).unwrap();
    let error = opencode2::read(&bytes, &mut |_: &[u8], _: &str| {
        Err("not an image".to_owned())
    })
    .unwrap_err();
    assert_eq!(
        error,
        HeadError::Media {
            mid: "u".into(),
            reason: "not an image".into()
        }
    );
    let by_reference = json!([{"id": "u", "type": "user", "time": {"created": 1},
                               "files": [{"mime": "image/png", "uri": "file:///tmp/a.png"}]}]);
    assert!(matches!(oc2(by_reference).unwrap_err(), HeadError::Media { mid, .. } if mid == "u"));
}

#[test]
fn an_export_holds_exactly_info_and_messages() {
    for body in [
        json!({"messages": []}),
        json!({"info": {}, "messages": [], "data": {}}),
        json!([]),
        json!({"info": {}, "messages": {}}),
    ] {
        let bytes = serde_json::to_vec(&body).unwrap();
        assert!(matches!(
            opencode2::read(&bytes, &mut accept),
            Err(HeadError::Unreadable(_))
        ));
    }
}

#[test]
fn side_file_facts_land_on_the_messages_they_name() {
    let mut history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
    let rows = read_sends(&fixture("opencode2/export.sends.json")).unwrap();
    apply_sends(&mut history, rows).unwrap();
    let first = history.messages[0].send.as_ref().unwrap();
    assert_eq!(first.client_message_id.as_deref(), Some("cm-1"));
    assert_eq!(first.sender, "prefrontal-core");
    assert_eq!(
        first.marks,
        Some(json!({"speaks_for": {"agent_id": "alfonso"}}))
    );
    // A queued send that started its own run has no delivery facts, as broca's own.
    assert_eq!(first.delivery_mode, None);
    let steer = history.messages[5].send.as_ref().unwrap();
    assert_eq!(steer.delivery_mode.as_deref(), Some("steer"));
    assert_eq!(steer.template.as_deref(), Some("mc-nudge"));
    assert_eq!(steer.marks, Some(json!({"notice": true})));
    let ride = &history.messages[2].rides[0];
    assert_eq!(ride.delivery_mode.as_deref(), Some("tool_result"));
    assert_eq!(ride.delivery_tool_call_id.as_deref(), Some("call_2"));
    assert_eq!(ride.text.as_deref(), Some("\n\n[note from occi]"));
    assert_eq!(ride.send_id, "send-3");
}

#[test]
fn side_file_rows_the_import_cannot_map_refuse_it() {
    let refused = |rows: Value| {
        let mut history = opencode2::read(&fixture("opencode2/export.json"), &mut accept).unwrap();
        let bytes = serde_json::to_vec(&rows).unwrap();
        read_sends(&bytes).and_then(|rows| apply_sends(&mut history, rows))
    };
    let row = |extra: Value| {
        let mut row = json!({"mid": "msg_u1", "send_id": "s", "sender": "core",
                             "delivered_as": "steer"});
        for (key, value) in extra.as_object().unwrap() {
            row[key] = value.clone();
        }
        json!([row])
    };
    assert!(refused(row(json!({}))).is_ok());
    // A sender in any form but a plain module id.
    assert!(refused(row(
        json!({"sender": {"kind": "reserved", "module_id": "core"}})
    ))
    .is_err());
    assert!(refused(row(json!({"delivered_as": "interrupt"}))).is_err());
    assert!(refused(row(json!({"mid": "msg_nowhere"}))).is_err());
    // A row naming a message the host never sent its model.
    assert!(refused(row(json!({"mid": "msg_sys"}))).is_err());
    assert!(refused(row(json!({"mid": "msg_a3"}))).is_err());
    assert!(refused(row(json!({"marks": {"pinned": true}}))).is_err());
    assert!(refused(row(json!({"origin": "x"}))).is_err());
    assert!(refused(json!([
        {"call_key": "msg_a1/call_2", "send_id": "s", "sender": "occi", "delivered_as": "tool_result"}
    ]))
    .is_err());
    assert!(refused(json!([
        {"call_key": "msg_a1/nope", "send_id": "s", "sender": "occi", "delivered_as": "tool_result",
         "text": "t"}
    ]))
    .is_err());
    assert!(refused(json!({"rows": []})).is_err());
}

#[test]
fn pi_reads_the_active_path_with_its_edits() {
    let history = pi::read(&fixture("pi/journal.jsonl"), "pi-ses", &mut accept).unwrap();
    // The abandoned branch (`x1`) and the line still being written are left out; control
    // entries have no row; removed, excluded and system entries have a row and no message.
    assert_eq!(
        moves(&history),
        [
            ("e1", Some(0)),
            ("e2", Some(1)),
            ("e3", Some(2)),
            ("e4", Some(3)),
            ("e5", Some(4)),
            ("e6", Some(5)),
            ("e7", Some(6)),
            ("e8", Some(7)),
            ("e9", None),
            ("e10", Some(8)),
            ("e11", None),
            ("e15", None),
            ("e16", Some(9)),
        ]
    );
    assert_eq!(history.dropped_system_messages, 1);
    assert_eq!(texts(&history.messages[8]), ["replaced text"]);
    assert_eq!(texts(&history.messages[4]), ["Plain string prompt."]);
    assert_eq!(texts(&history.messages[5]), ["An extension note."]);
    assert_eq!(
        texts(&history.messages[6]),
        ["The following is a summary of a branch that this conversation came back from:\n\n<summary>\nWe tried another path.</summary>"]
    );
    assert_eq!(texts(&history.messages[7]), ["Ran `ls`\n```\na\n\n```"]);
    assert!(history.messages[5..8]
        .iter()
        .all(|message| message.message.role == Role::User));
    assert_eq!(history.messages[0].written_at_ms, Some(1_767_225_601_000));
    assert_eq!(history.messages[0].media.len(), 1);
}

#[test]
fn pi_calls_keep_their_runner_keys_and_reasoning_drops_its_signature() {
    let history = pi::read(&fixture("pi/journal.jsonl"), "pi-ses", &mut accept).unwrap();
    let assistant = &history.messages[1];
    assert_eq!(
        assistant.message.content[0],
        ContentBlock::Reasoning {
            text: "Hmm.".into(),
            signature: None
        }
    );
    assert_eq!(assistant.calls[0].call_key, "e2/tc1");
    assert_eq!(assistant.calls[0].carrier, "thalamus-pi");
    assert_eq!(
        assistant.usage,
        Some(HeadUsage {
            prompt_tokens: Some(8),
            cache_read_tokens: None,
            cache_write_tokens: Some(3),
            output_tokens: Some(7),
            reasoning_tokens: None,
        })
    );
    assert_eq!(history.messages[3].usage, None);
    let result = &history.messages[2];
    assert_eq!(result.mid.as_deref(), Some("e3"));
    assert_eq!(result.created_at_ms, None);
    assert_eq!((result.media[0].block, result.media[0].part), (0, Some(1)));
}

#[test]
fn pi_side_file_facts_land_on_their_messages() {
    let mut history = pi::read(&fixture("pi/journal.jsonl"), "pi-ses", &mut accept).unwrap();
    let rows = read_sends(&fixture("pi/journal.sends.json")).unwrap();
    apply_sends(&mut history, rows).unwrap();
    assert_eq!(
        history.messages[4]
            .send
            .as_ref()
            .unwrap()
            .delivery_mode
            .as_deref(),
        Some("idle_start")
    );
    let ride = &history.messages[2].rides[0];
    assert_eq!(ride.template.as_deref(), Some("occi-ride"));
    assert_eq!(ride.delivery_tool_call_id.as_deref(), Some("tc1"));
}

fn pi_journal(entries: Value) -> Vec<u8> {
    let mut out = String::from(
        "{\"type\":\"session\",\"id\":\"s\",\"timestamp\":\"2026-01-01T00:00:00Z\"}\n",
    );
    for entry in entries.as_array().unwrap() {
        out.push_str(&entry.to_string());
        out.push('\n');
    }
    out.into_bytes()
}

fn pi_message(id: &str, parent: Option<&str>, message: Value) -> Value {
    json!({"type": "message", "id": id, "parentId": parent,
           "timestamp": "2026-01-01T00:00:01Z", "message": message})
}

#[test]
fn pi_refusals() {
    let read = |entries: Value| pi::read(&pi_journal(entries), "s", &mut accept);
    // A compaction on the active path.
    let compacted = read(json!([
        pi_message("a", None, json!({"role": "user", "content": "hi"})),
        {"type": "compaction", "id": "c", "parentId": "a", "timestamp": "2026-01-01T00:00:02Z",
         "summary": "s", "firstKeptEntryId": "a"}
    ]));
    assert!(matches!(compacted, Err(HeadError::Row { mid, .. }) if mid == "c"));
    // A call Pi never ran has no key, yet here it has a result.
    let keyless = read(json!([
        pi_message(
            "a",
            None,
            json!({"role": "assistant", "stopReason": "stop",
            "content": [{"type": "toolCall", "id": "t", "name": "x", "arguments": {}}]})
        ),
        pi_message(
            "b",
            Some("a"),
            json!({"role": "toolResult", "toolCallId": "t",
            "content": [{"type": "text", "text": "ok"}]})
        )
    ]));
    assert!(matches!(keyless, Err(HeadError::Row { mid, .. }) if mid == "a"));
    // A result for no call.
    let orphan = read(json!([pi_message(
        "b",
        None,
        json!({"role": "toolResult", "toolCallId": "t",
            "content": [{"type": "text", "text": "ok"}]})
    )]));
    assert!(matches!(orphan, Err(HeadError::OrphanResult { .. })));
    // A message with no readable timestamp.
    let mut untimed = pi_message("a", None, json!({"role": "user", "content": "hi"}));
    untimed["timestamp"] = json!("yesterday");
    assert!(matches!(read(json!([untimed])), Err(HeadError::Row { .. })));
    // Another session's journal.
    assert!(matches!(
        pi::read(&pi_journal(json!([])), "other", &mut accept),
        Err(HeadError::Unreadable(_))
    ));
    // A block the import doesn't map.
    let unknown = read(json!([pi_message(
        "a",
        None,
        json!({"role": "user", "content": [{"type": "audio", "data": ""}]})
    )]));
    assert!(matches!(unknown, Err(HeadError::Row { mid, .. }) if mid == "a"));
}

#[test]
fn rfc3339_times() {
    assert_eq!(pi::rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        pi::rfc3339_ms("2026-01-01T00:00:01.000Z"),
        Some(1_767_225_601_000)
    );
    assert_eq!(
        pi::rfc3339_ms("2026-01-01T02:00:01.5+02:00"),
        Some(1_767_225_601_500)
    );
    assert_eq!(
        pi::rfc3339_ms("2024-02-29T00:00:00.123456Z"),
        Some(1_709_164_800_123)
    );
    for bad in [
        "2026-01-01",
        "2026-13-01T00:00:00Z",
        "2025-02-29T00:00:00Z",
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:00.Z",
        "2026-01-01 00:00:00Z",
    ] {
        assert_eq!(pi::rfc3339_ms(bad), None, "{bad}");
    }
}

fn pi_read(entries: Value) -> Result<HeadHistory, HeadError> {
    pi::read(&pi_journal(entries), "s", &mut accept)
}

fn said(role: &str, text: &str, stop: &str) -> Value {
    json!({"role": role, "stopReason": stop, "content": [{"type": "text", "text": text}]})
}

fn calling(stop: &str, calls: &[&str]) -> Value {
    let content: Vec<Value> = calls
        .iter()
        .map(|id| json!({"type": "toolCall", "id": id, "name": "read", "arguments": {}}))
        .collect();
    json!({"role": "assistant", "stopReason": stop, "content": content})
}

fn answering(call: &str) -> Value {
    json!({"role": "toolResult", "toolCallId": call,
           "content": [{"type": "text", "text": "ok"}], "isError": false})
}

/// The synthetic result Pi gives a call left without one.
fn no_result(call: &str) -> ContentBlock {
    ContentBlock::ToolResult(broca_protocol::ToolResult {
        tool_call_id: broca_protocol::ToolCallId(call.into()),
        output: ToolOutput::error("No result provided"),
        is_error: true,
    })
}

/// An errored or aborted assistant turn never reaches Pi's model, nor do its calls: it imports
/// as no message, its calls aren't open, and the retry after it, Pi's own or the person's,
/// imports as the host showed it.
#[test]
fn pi_skips_an_errored_or_aborted_turn_and_keeps_its_retry() {
    let history = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "first"})),
        pi_message("a1", Some("u1"), calling("error", &["t9"])),
        pi_message("a2", Some("a1"), said("assistant", "retried by Pi", "stop")),
        pi_message(
            "u2",
            Some("a2"),
            json!({"role": "user", "content": "second"})
        ),
        pi_message("a3", Some("u2"), said("assistant", "cut off", "aborted")),
        pi_message(
            "u3",
            Some("a3"),
            json!({"role": "user", "content": "again"})
        ),
        pi_message("a4", Some("u3"), said("assistant", "answered", "stop")),
    ]))
    .unwrap();
    assert_eq!(
        mids(&history),
        [Some("u1"), Some("a2"), Some("u2"), Some("u3"), Some("a4")]
    );
    assert_eq!(
        moves(&history),
        [
            ("u1", Some(0)),
            ("a1", None),
            ("a2", Some(1)),
            ("u2", Some(2)),
            ("a3", None),
            ("u3", Some(3)),
            ("a4", Some(4)),
        ]
    );
    assert!(history
        .messages
        .iter()
        .all(|message| message.calls.is_empty()));
}

/// A kept assistant's call still without a result gets Pi's synthetic error result where Pi
/// puts it: before the next user or assistant turn (an errored one included), or at the end,
/// each in a tool message of its own under an id broca mints. A call Pi never ran keeps no key.
#[test]
fn pi_closes_an_open_call_with_pis_synthetic_result() {
    // At the end of the conversation, after the result that did come.
    let at_end = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "go"})),
        pi_message("a1", Some("u1"), calling("toolUse", &["t1", "t2"])),
        pi_message("r1", Some("a1"), answering("t1")),
    ]))
    .unwrap();
    assert_eq!(mids(&at_end), [Some("u1"), Some("a1"), Some("r1"), None]);
    let synthetic = &at_end.messages[3];
    assert_eq!(synthetic.message.role, Role::Tool);
    assert_eq!(synthetic.message.content, [no_result("t2")]);
    assert_eq!(
        (synthetic.written_at_ms, synthetic.created_at_ms),
        (None, None)
    );
    assert_eq!(
        at_end.messages[1].calls.len(),
        2,
        "both calls ran under keys"
    );

    // Before the next user turn, which moves along.
    let before_user = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "go"})),
        pi_message("a1", Some("u1"), calling("toolUse", &["t1"])),
        pi_message("u2", Some("a1"), json!({"role": "user", "content": "next"})),
    ]))
    .unwrap();
    assert_eq!(
        mids(&before_user),
        [Some("u1"), Some("a1"), None, Some("u2")]
    );
    assert_eq!(before_user.messages[2].message.content, [no_result("t1")]);
    assert_eq!(
        moves(&before_user),
        [("u1", Some(0)), ("a1", Some(1)), ("u2", Some(3))]
    );

    // Before the next assistant turn, even one Pi leaves out.
    let before_skipped = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "go"})),
        pi_message("a1", Some("u1"), calling("toolUse", &["t1"])),
        pi_message("a2", Some("a1"), said("assistant", "failed", "error")),
        pi_message("a3", Some("a2"), said("assistant", "done", "stop")),
    ]))
    .unwrap();
    assert_eq!(
        mids(&before_skipped),
        [Some("u1"), Some("a1"), None, Some("a3")]
    );
    assert_eq!(
        before_skipped.messages[2].message.content,
        [no_result("t1")]
    );
    assert_eq!(
        moves(&before_skipped),
        [
            ("u1", Some(0)),
            ("a1", Some(1)),
            ("a2", None),
            ("a3", Some(3))
        ]
    );

    // A reply that didn't stop to use tools never ran its call: it has no key.
    let unrun = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "go"})),
        pi_message("a1", Some("u1"), calling("length", &["t1"])),
    ]))
    .unwrap();
    assert!(unrun.messages[1].calls.is_empty());
    assert_eq!(unrun.messages[2].message.content, [no_result("t1")]);
}

/// A result for a call of a turn Pi left out still refuses the import as an orphan, naming that
/// turn as errored or aborted.
#[test]
fn pi_refuses_a_result_for_a_skipped_turns_call() {
    let error = pi_read(json!([
        pi_message("u1", None, json!({"role": "user", "content": "go"})),
        pi_message("a1", Some("u1"), calling("aborted", &["t1"])),
        pi_message("r1", Some("a1"), answering("t1")),
    ]))
    .unwrap_err();
    assert_eq!(
        error,
        HeadError::OrphanResult {
            mid: "r1".into(),
            call_id: "t1".into(),
            skipped_turn: Some("a1".into()),
        }
    );
    assert_eq!(error.code(), "import_orphan_result");
    assert!(
        error.to_string().contains("errored or was aborted"),
        "{error}"
    );
}

/// A file of Thalamus's published exports (`tests/fixtures/thalamus-export-v1`).
fn thalamus(path: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/thalamus-export-v1")
            .join(path),
    )
    .unwrap()
}

/// Every file of Thalamus's published exports is the one its checksum names.
#[test]
fn thalamus_exports_match_their_checksums() {
    let sums = String::from_utf8(thalamus("SHA256SUMS")).unwrap();
    let mut checked = 0;
    for line in sums.lines().filter(|line| !line.is_empty()) {
        let (digest, path) = line.split_once("  ").unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(thalamus(path))),
            digest,
            "{path}"
        );
        checked += 1;
    }
    assert_eq!(checked, 6);
}

/// The history a published export imports as: the reader, then its side file.
fn thalamus_history(dir: &str, journal: &str, session: Option<&str>) -> (HeadHistory, Value) {
    let bytes = thalamus(&format!("{dir}/{journal}"));
    let reply: Value = serde_json::from_slice(&thalamus(&format!("{dir}/reply.json"))).unwrap();
    assert_eq!(
        reply["sha256"],
        format!("{:x}", Sha256::digest(&bytes)),
        "the reply names its file's digest"
    );
    let mut history = match session {
        None => opencode2::read(&bytes, &mut accept).unwrap(),
        Some(session) => pi::read(&bytes, session, &mut accept).unwrap(),
    };
    let rows = read_sends(&thalamus(&format!("{dir}/export.sends.json"))).unwrap();
    apply_sends(&mut history, rows).unwrap();
    (history, reply)
}

fn send_of<'a>(history: &'a HeadHistory, mid: &str) -> &'a SendFacts {
    history
        .messages
        .iter()
        .find(|message| message.mid.as_deref() == Some(mid))
        .and_then(|message| message.send.as_ref())
        .unwrap_or_else(|| panic!("no send on {mid}"))
}

/// OpenCode 2.2.3's real export: each message keeps its source id, a call's results come as a
/// message broca names itself, the export's `idle` rows (a turn's end, with no message) have
/// no move-list row, and the newest message is the one the reply names. The side file's sends
/// land on their messages, the ride on its call's results.
#[test]
fn thalamus_opencode2_export_reads_as_recorded() {
    let (history, reply) = thalamus_history("opencode2", "export.json", None);
    let ids = [
        "msg_1b49586aa0016EUiZetDheU1f4",
        "msg_1b49586f1001rVgZgzWdeKZmu8",
        "msg_1b49587f10010uHhgvansXbRF1",
        "msg_1b495900c001DES0ba9sPPASsN",
        "msg_1b49591470019dpMEsZc2WOTUW",
        "msg_1b495915a001xOIirlVXwIIakW",
        "msg_1b4959296001mKWG88czxOfO3s",
        "msg_1b49592a7001zLB2AYW13z4J7F",
        "msg_1b495955f001hVi7sIosw8140K",
        "msg_1b495957b001zk2Vzqz0lhIKBJ",
        "msg_1b49595fe001dZGvbH2E283JZ2",
    ];
    assert_eq!(
        mids(&history),
        [
            Some(ids[0]),
            Some(ids[1]),
            None,
            Some(ids[2]),
            Some(ids[3]),
            Some(ids[4]),
            Some(ids[5]),
            Some(ids[6]),
            Some(ids[7]),
            Some(ids[8]),
            Some(ids[9]),
            None,
            Some(ids[10]),
        ]
    );
    assert_eq!(
        moves(&history),
        [
            (ids[0], Some(0)),
            (ids[1], Some(1)),
            (ids[2], Some(3)),
            (ids[3], Some(4)),
            (ids[4], Some(5)),
            (ids[5], Some(6)),
            (ids[6], Some(7)),
            (ids[7], Some(8)),
            (ids[8], Some(9)),
            (ids[9], Some(10)),
            (ids[10], Some(12)),
        ]
    );
    assert_eq!(history.dropped_system_messages, 0);
    assert_eq!(
        history.messages.last().unwrap().mid.as_deref(),
        reply["newest_mid"].as_str()
    );
    assert_eq!(
        reply["messages"], 15,
        "rows the export holds, idle ones too"
    );
    let keys: Vec<(&str, &str)> = history
        .messages
        .iter()
        .flat_map(|message| &message.calls)
        .map(|call| (call.call_key.as_str(), call.carrier.as_str()))
        .collect();
    assert_eq!(
        keys,
        [
            (
                "msg_1b49586f1001rVgZgzWdeKZmu8/call_probe",
                "thalamus-harness"
            ),
            (
                "msg_1b495957b001zk2Vzqz0lhIKBJ/call_each",
                "thalamus-harness"
            ),
        ]
    );

    let first = send_of(&history, ids[0]);
    assert_eq!(
        (first.sender.as_str(), first.delivery_mode.as_deref()),
        ("prefrontal-core", None),
        "a queued send has no delivery mode, as broca's own queued sends have none"
    );
    let steer = send_of(&history, ids[2]);
    assert_eq!(steer.delivery_mode.as_deref(), Some("steer"));
    let second = send_of(&history, ids[4]);
    assert_eq!(second.client_message_id.as_deref(), Some("cmid-2"));
    assert_eq!(
        second.marks,
        Some(json!({"speaks_for": {"agent_id": "agent-7"}}))
    );
    let note = send_of(&history, ids[6]);
    assert_eq!(
        (
            note.sender.as_str(),
            note.template.as_deref(),
            note.delivery_mode.as_deref()
        ),
        ("magic-context", Some("note"), Some("idle_start"))
    );
    assert_eq!(note.marks, Some(json!({"notice": true})));
    let rides = &history.messages[2].rides;
    assert_eq!(rides.len(), 1);
    assert_eq!(
        (
            rides[0].send_id.as_str(),
            rides[0].sender.as_str(),
            rides[0].text.as_deref(),
            rides[0].delivery_tool_call_id.as_deref()
        ),
        (
            "ride",
            "magic-context",
            Some("RIDE_TEXT"),
            Some("call_probe")
        )
    );
}

/// Pi 1.5.0's real journal: each message keeps its entry id, the system entry is dropped and
/// counted, the reply that stopped to use tools keys its call `<entry id>/<toolCall id>`
/// verbatim, the note arrives as a custom message, and the newest message is the one the reply
/// names.
#[test]
fn thalamus_pi_export_reads_as_recorded() {
    let (history, reply) = thalamus_history(
        "pi",
        "export.jsonl",
        Some("01a1b49a-6d6e-7506-bfaa-62da1e6cef3d"),
    );
    let ids = [
        "213ff4e5", "c62f7305", "1a089c7d", "daee0155", "b951b5cb", "62c953eb", "07b9345b",
        "a6909356", "93de564a", "5a2aaef2", "9c4dddb3", "fd58b6d7",
    ];
    assert_eq!(mids(&history), ids.map(Some));
    let mut expected = vec![("9d881624", None)];
    expected.extend(ids.iter().enumerate().map(|(at, id)| (*id, Some(at))));
    assert_eq!(moves(&history), expected);
    assert_eq!(history.dropped_system_messages, 1);
    assert_eq!(
        history.messages.last().unwrap().mid.as_deref(),
        reply["newest_mid"].as_str()
    );
    let calls = &history.messages[1].calls;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].call_key, "c62f7305/call_probe_1");
    assert_eq!(calls[0].carrier, "thalamus-pi");
    assert_eq!(history.messages[6].message.role, Role::User);
    assert_eq!(texts(&history.messages[6]), ["NOTE 3"]);

    let rides = &history.messages[2].rides;
    assert_eq!(rides.len(), 1);
    assert_eq!(
        (
            rides[0].send_id.as_str(),
            rides[0].sender.as_str(),
            rides[0].text.as_deref()
        ),
        ("ride", "notifier", Some("RIDE_REMINDER"))
    );
    let note = send_of(&history, ids[6]);
    assert_eq!(
        (
            note.sender.as_str(),
            note.template.as_deref(),
            note.delivery_mode.as_deref()
        ),
        ("notifier", Some("note"), Some("idle_start"))
    );
    assert_eq!(note.marks, Some(json!({"notice": true})));
    assert_eq!(
        send_of(&history, ids[4]).client_message_id.as_deref(),
        Some("cmid-2")
    );
    assert_eq!(
        send_of(&history, ids[10]).delivery_mode.as_deref(),
        Some("steer")
    );
    assert_eq!(send_of(&history, ids[8]).delivery_mode.as_deref(), None);
}
