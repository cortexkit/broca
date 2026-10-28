//! Captured bytes of a module-outage tool result through the real serve path.
//!
//! A tool call refused with a module-availability code past the tool-dispatch wait ends as
//! `Completed` with the attributed unavailable result (proven undelivered: nothing was sent).
//! The wait itself is pinned by broca-subc's paused-clock tests; here the rig's scripted plane
//! stands in for the wait by returning the exact result the real plane builds, so every byte
//! below comes from the real producers: the core loop, the WAL, `session.read`, the history
//! display, and the live-event projection with its subscribe encoding.
//!
//! The first test drives that outage through a synthetic session and writes the four artifacts
//! plus a SHA256SUMS file under `.cortexkit/internal-docs/samples/tool-outage/` (untracked;
//! collected out of band, never committed). The second test re-drives the same outage and
//! decodes the WAL `tool_result` record's output with the shapes a build at the base commit
//! knew, proving an older broca reads the new fields by ignoring them.

mod delivery_rig;

use std::path::PathBuf;
use std::time::Duration;

use broca_core::DispatchResult;
use broca_protocol::content::{ToolOutput, ToolResult};
use broca_protocol::ToolCallId;
use delivery_rig::*;
use serde_json::{json, Value};
use subc_protocol::Frame;

/// The module-availability refusal the outage attributes: the daemon's code and detail.
const OUTAGE_CODE: &str = "target_unavailable";
const OUTAGE_DETAIL: &str = "supervised_not_registered";

/// The exact stored payload bytes of the call's `tool_result` frame. The WAL file is
/// binary-framed (length, version, sequence, fence, digest, payload), so scan every `{`
/// position, stream-decode one JSON value there, and take the first value that is (or
/// wraps, under a `requires` envelope) the call's `tool_result` record.
fn tool_result_payload(wal: &[u8], tool_call_id: &str) -> Vec<u8> {
    for start in 0..wal.len() {
        if wal[start] != b'{' {
            continue;
        }
        let mut stream = serde_json::Deserializer::from_slice(&wal[start..]).into_iter::<Value>();
        let head = stream.next();
        let end = stream.byte_offset();
        let Some(Ok(value)) = head else {
            continue;
        };
        let record = value.get("record").unwrap_or(&value);
        if record.get("type").and_then(Value::as_str) == Some("tool_result")
            && value.to_string().contains(tool_call_id)
        {
            return wal[start..start + end].to_vec();
        }
    }
    panic!(
        "no tool_result payload for {tool_call_id} in {} WAL bytes",
        wal.len()
    );
}

/// The attributed result the real plane returns for a call refused past the wait.
fn outage_result(tool_call_id: &str) -> ToolResult {
    ToolResult {
        tool_call_id: ToolCallId(tool_call_id.into()),
        output: ToolOutput::module_unavailable("aft", OUTAGE_CODE, OUTAGE_DETAIL),
        is_error: true,
    }
}

/// Drive one `work` call that ends as `result`, and wait for the run.
async fn finished_session(tag: &str, call_id: &'static str, result: ToolResult) -> Rig {
    let mut rig = Rig::start(tag).await;
    rig.script.push(&[Step::Call(call_id), Step::Text("done")]);
    rig.script
        .tool_results
        .lock()
        .unwrap()
        .insert(call_id.into(), DispatchResult::Completed(result));
    rig.send("send-1", "hi", None).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig
}

/// Drive one `work` call that ends as an attributed module outage, and wait for the run.
async fn outage_session(tag: &str) -> Rig {
    finished_session(tag, "call-1", outage_result("call-1")).await
}

/// A plain, unattributed tool error, as it was before attribution existed.
fn plain_error_result(tool_call_id: &str) -> ToolResult {
    ToolResult {
        tool_call_id: ToolCallId(tool_call_id.into()),
        output: ToolOutput::error("plain failure"),
        is_error: true,
    }
}

/// `method` with `params` on `channel`, returning the reply frame.
async fn call(rig: &mut Rig, channel: u16, method: &str, params: Value) -> Frame {
    rig.request(channel, json!({"method": method, "params": params}))
        .await
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The samples directory under the checkout (`.cortexkit` is untracked), if the test runs
/// inside the repo. Anywhere else the capture still asserts, it just writes nothing.
fn samples_dir() -> Option<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join(".cortexkit").is_dir() {
            return Some(dir.join(".cortexkit/internal-docs/samples/tool-outage"));
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Any object on the page carrying the attributed outage output.
fn find_attributed(value: &Value) -> Option<&Value> {
    match value {
        Value::Object(map) => {
            if map.get("message") == Some(&Value::String("tool plane unavailable".into())) {
                return Some(value);
            }
            map.values().find_map(find_attributed)
        }
        Value::Array(items) => items.iter().find_map(find_attributed),
        _ => None,
    }
}

/// The tool result for `call_id` as `session.read` shows it. Matches the content block
/// carrying the call's id AND an output (not the assistant's tool-call block, which names
/// the id but carries no output).
fn read_output(page: &Value, tool_call_id: &str) -> Value {
    page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["message"]["content"].as_array().unwrap().clone())
        .find(|block| block["tool_call_id"] == tool_call_id && block["output"].is_object())
        .unwrap_or_else(|| panic!("session.read shows no result for {tool_call_id}: {page}"))
        ["output"]
        .clone()
}

/// Assert the attributed outage shape: the stable message plus the module, the daemon's
/// code and detail, and the never-sent marker.
fn assert_attributed(output: &Value) {
    assert_eq!(
        output["message"], "tool plane unavailable",
        "Core matches this message exactly: {output}"
    );
    assert_eq!(output["module"], "aft", "{output}");
    assert_eq!(output["code"], OUTAGE_CODE, "{output}");
    assert_eq!(output["detail"], OUTAGE_DETAIL, "{output}");
    assert_eq!(output["nothing_sent"], true, "{output}");
    assert!(
        output["retry"]
            .as_str()
            .is_some_and(|retry| !retry.is_empty()),
        "{output}"
    );
}

/// A module outage through the real serve path, captured as bytes for a strict reader.
#[tokio::test]
async fn capture_module_outage_tool_result_bytes() {
    let mut rig = outage_session("tool-outage-samples").await;

    // (1) The `session.read` reply with the tool result.
    let read = rig
        .request(7, json!({"method": "session.read", "params": {}}))
        .await;
    assert_eq!(read.header.ty, subc_protocol::FrameType::Response);
    let page = result(&read);
    let read_output = read_output(&page, "call-1");
    assert_attributed(&read_output);

    // (2) The `session.history` display row for it.
    let history = call(&mut rig, 7, "session.history", json!({"limit": 100})).await;
    let history = result(&history);
    let history_entry = history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| {
            entry.to_string().contains("call-1")
                && entry.to_string().contains("tool plane unavailable")
        })
        .unwrap_or_else(|| panic!("session.history shows no row for call-1: {history}"))
        .clone();
    let history_output = find_attributed(&history_entry)
        .unwrap_or_else(|| panic!("the history row carries no attributed output: {history_entry}"));
    assert_attributed(history_output);

    // (3) The live `tool_finished` event: the bare event and its subscribe frame.
    let frames = rig
        .subscribe_collect(9, json!({"from": "start"}), Duration::from_millis(500))
        .await;
    assert!(!frames.is_empty(), "the run streamed live events");
    let finished_frame = frames
        .iter()
        .find(|frame| {
            serde_json::from_slice::<Value>(&frame.body)
                .map(|value| {
                    value["event"]["type"] == "tool_finished"
                        && value["event"]["tool_call_id"] == "call-1"
                })
                .unwrap_or(false)
        })
        .expect("the stream carries the call's tool_finished event");
    let finished: Value = serde_json::from_slice(&finished_frame.body).unwrap();
    assert_eq!(finished["event"]["outcome"], "error");
    assert_eq!(finished["event"]["tool_call_id"], "call-1");

    // (4) The WAL `tool_result` record payload, exactly as stored (bare record or
    // `requires` envelope — see the framing docs).
    let record_payload = tool_result_payload(&rig.wal_bytes(), "call-1");
    let record: Value = serde_json::from_slice(&record_payload).unwrap();
    let record_output = record.get("record").unwrap_or(&record)["result"]["output"].clone();
    assert_attributed(&record_output);

    // The captured bytes, for a strict reader out of band.
    write_samples(&[
        ("session_read.reply.json", read.body.clone()),
        (
            "session_history.row.json",
            serde_json::to_vec_pretty(&history_entry).unwrap(),
        ),
        (
            "tool_finished.event.json",
            serde_json::to_vec_pretty(&finished["event"]).unwrap(),
        ),
        ("tool_finished.frame.json", finished_frame.body.clone()),
        ("tool_result.record.json", record_payload.clone()),
    ]);
    rig.finish().await;
}

/// Write sample files, then regenerate SHA256SUMS over every sample file present, so
/// tests that each add files all leave consistent sums.
fn write_samples(files: &[(&str, Vec<u8>)]) {
    let Some(dir) = samples_dir() else {
        return;
    };
    std::fs::create_dir_all(&dir).unwrap();
    for (name, bytes) in files {
        std::fs::write(dir.join(name), bytes).unwrap();
    }
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name != "SHA256SUMS")
        .collect();
    names.sort();
    let mut sums = String::new();
    for name in &names {
        let bytes = std::fs::read(dir.join(name)).unwrap();
        sums.push_str(&format!("{}  {name}\n", sha256_hex(&bytes)));
    }
    std::fs::write(dir.join("SHA256SUMS"), &sums).unwrap();
    println!("wrote outage samples to {}", dir.display());
}

/// The `session.history` display row (`form: "display"`) for the outage, and for a
/// plain unattributed error through the same path.
///
/// What changed for an error tool result is new TEXT lines only, never the shape: the
/// display text for an attributed outage is the stable message plus one line each for
/// the module, the code, the detail, the never-sent marker and the retry guidance,
/// while a plain error renders as its message alone — byte-identical to the old
/// `message.clone()` rendering (see the base `history.rs` error arm and the protocol
/// unit test pinning `error_model_text` with no attribution to the message itself).
#[tokio::test]
async fn capture_display_rows_for_outage_and_plain_error() {
    let mut outage =
        finished_session("tool-outage-display", "call-1", outage_result("call-1")).await;
    let mut plain = finished_session(
        "tool-plain-error-display",
        "call-2",
        plain_error_result("call-2"),
    )
    .await;

    let outage_row = display_row(&mut outage, "call-1").await;
    let outage_rendered = outage_row.to_string();
    // The rendered display text (as opposed to the JSON output object beside it) carries
    // the attribution on its own lines.
    for line in [
        "tool plane unavailable\\nmodule: aft",
        "module: aft",
        "code: target_unavailable",
        "detail: supervised_not_registered",
        "nothing was sent",
        "retry: the module is restarting or unavailable",
    ] {
        assert!(
            outage_rendered.contains(line),
            "the display row renders {line:?}: {outage_row}"
        );
    }

    let plain_row = display_row(&mut plain, "call-2").await;
    let plain_rendered = plain_row.to_string();
    assert!(
        plain_rendered.contains("plain failure"),
        "the display row renders the message: {plain_row}"
    );
    // No attribution leaks into a plain error's row: its display text IS its message,
    // byte-identical to what `message.clone()` produced before attribution existed.
    for leaked in [
        "module: ",
        "code: ",
        "detail: ",
        "nothing was sent",
        "retry: ",
    ] {
        assert!(
            !plain_rendered.contains(leaked),
            "a plain error must render as its message alone: {plain_row}"
        );
    }

    write_samples(&[
        (
            "session_history.display.row.json",
            serde_json::to_vec_pretty(&outage_row).unwrap(),
        ),
        (
            "session_history.display.plain-error.row.json",
            serde_json::to_vec_pretty(&plain_row).unwrap(),
        ),
    ]);
    outage.finish().await;
    plain.finish().await;
}

/// The `session.history` display row (`form: "display"`) for the call's tool result: the
/// entry whose row holds a `tool_result` part for that call, not the assistant entry
/// whose tool-call block merely names the same id.
async fn display_row(rig: &mut Rig, call_id: &str) -> Value {
    let reply = call(
        rig,
        7,
        "session.history",
        json!({"form": "display", "limit": 100}),
    )
    .await;
    let history = result(&reply);
    history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| {
            let rendered = entry.to_string();
            rendered.contains(call_id) && rendered.contains("\"type\":\"tool_result\"")
        })
        .unwrap_or_else(|| panic!("display history shows no tool result for {call_id}: {history}"))
        .clone()
}

/// The output shape a build at the base commit knew: `Error` carries only `message`, and
/// `Unavailable` carries only `detail`. Neither shape denies unknown fields, so a build that
/// old reads the new bytes by ignoring the attribution.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(dead_code)]
enum OldToolOutput {
    Text { text: String },
    Json { value: Value },
    Parts { parts: Vec<Value> },
    Denied { reason: String },
    Error { message: String },
}

/// The `Unavailable` shape a build at the base commit knew: only `detail`. Like the output
/// shape it denies nothing unknown, so new attribution fields ride along ignored.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(dead_code)]
enum OldNoDispatchReason {
    Denied { reason: String },
    ValidationFailed { detail: String },
    Unavailable { detail: String },
    PolicyBlocked { detail: String },
}

/// An older broca decodes the captured WAL record's output: the message survives exactly,
/// and the attribution it never knew rides along ignored.
#[tokio::test]
async fn older_build_reads_the_captured_outage_output() {
    let rig = outage_session("tool-outage-compat").await;
    let record_payload = tool_result_payload(&rig.wal_bytes(), "call-1");
    // The record envelope itself is new-reader business; the output object is what an older
    // build must still swallow.
    let record: Value = serde_json::from_slice(&record_payload).unwrap();
    let output_json = record.get("record").unwrap_or(&record)["result"]["output"].clone();
    // Sanity: the captured bytes really are the attributed outage.
    assert_attributed(&output_json);
    let old: OldToolOutput = serde_json::from_value(output_json).unwrap();
    let OldToolOutput::Error { message } = old else {
        panic!("an older build reads the outage as an error result");
    };
    assert_eq!(message, "tool plane unavailable");
    // The same build reads the current `Unavailable` reason the batch path writes: the
    // new attribution fields decode away, the stable message survives.
    let reason_json = serde_json::to_value(broca_wal::NoDispatchReason::Unavailable {
        detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
        module: Some("aft".into()),
        code: Some(OUTAGE_CODE.into()),
        refusal_detail: Some(OUTAGE_DETAIL.into()),
    })
    .unwrap();
    let OldNoDispatchReason::Unavailable { detail } = serde_json::from_value(reason_json).unwrap()
    else {
        panic!("an older build reads the outage as an unavailable no-dispatch");
    };
    assert_eq!(detail, "tool plane unavailable");
    rig.finish().await;
}
