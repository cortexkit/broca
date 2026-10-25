//! Output references to tool outputs (llm-runner §9.13) through the real serve path and core
//! loop over a real file WAL.
//!
//! The first tests pin bytes that output references must never change: the default
//! `session.read` page of a session whose reply names a tool output, and the plan manifest a
//! session freezes when its provider's catalog declares `reference_slots` but the plan does not
//! opt in.

use crate::delivery_rig;
use crate::plan_admission::plan_rig;

use delivery_rig::*;

use broca_wal::WalRecord;
use serde_json::{json, Value};

/// `body` with the run id and lineage id it names replaced by placeholders: both are minted per
/// run from a per-process scratch directory.
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

/// Compare `body` with the golden `file`, rewriting the golden first under `BROCA_UPDATE_GOLDEN`.
/// The bytes are printed between markers as well, so a run on another machine can hand them back.
fn check_golden(body: &str, file: &str) {
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(golden_path(file), format!("{body}\n")).unwrap();
        println!("GOLDEN_BEGIN {file}\n{body}\nGOLDEN_END {file}");
    }
    assert_eq!(body, pinned(file), "the bytes pinned in {file}");
}

/// A plan that renders runner tags and offers the rig's `work` tool from its tool module. `item`
/// members are added to the plan's one tool item.
pub(super) fn tagged_plan(item: Value) -> Value {
    let composition = json!({"providers": [{"provider": WORK_MODULE, "tools": [
        {"name": "work", "capabilities": []}
    ]}], "step_transform_items": []});
    let mut tool_item = json!({"provider": WORK_MODULE, "preset": "head", "params": {}});
    for (key, value) in item.as_object().unwrap() {
        tool_item[key] = value.clone();
    }
    json!({
        "tool_items": [tool_item],
        "system_text_items": [], "step_transform_items": [], "render_tags": true,
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    })
}

/// Send `prompt` with `plan` as the session's first send, and return the run id.
pub(super) async fn send_plan(rig: &mut Rig, send_id: &str, prompt: &str, plan: &Value) -> String {
    let active = rig
        .request(
            7,
            json!({"method": "session.send", "params": {
                "send_id": send_id, "prompt": prompt,
                "model": {"provider": "fake", "model": "fake"},
                "plan": plan,
            }}),
        )
        .await;
    result(&active)["run_id"].as_str().unwrap().to_owned()
}

/// The default `session.read` page of a tagged session whose final reply names the tool output,
/// the user's text and a number no part holds. Output references add nothing to this page.
#[tokio::test]
async fn a_reply_naming_tool_outputs_keeps_its_default_read_bytes() {
    let (mut rig, _) = plan_rig("refs-pin-read").await;
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Text("see $$tc(2), $$tc(1) and $$tc(99)"),
    ]);
    let run_id = send_plan(&mut rig, "turn-1", "hi", &tagged_plan(json!({}))).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;

    let read = rig
        .request(7, json!({"method": "session.read", "params": {}}))
        .await;
    let page = result(&read);
    let lineage_id = page["lineage_id"].as_str().unwrap().to_owned();
    let tags: Vec<Value> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["tags"].clone())
        .collect();
    assert_eq!(
        tags,
        [
            json!([{"part": "m0#0", "tag": "§1§"}]),
            json!([{"part": "m1#0"}]),
            json!([{"part": "m2#0", "tag": "§2§"}]),
            json!([{"part": "m3#0", "tag": "§3§"}]),
        ],
        "the reply names the tool result's number"
    );
    let mut body = normalized(&read.body, &run_id, &lineage_id);
    // Entry times are pinned elsewhere (session_read.written_at.json); drop each recorded time.
    for entry in page["messages"].as_array().unwrap() {
        let ordinal = entry["ordinal"].as_u64().unwrap();
        if let Some(at) = entry["written_at"].as_u64() {
            let timed = format!("\"written_at\":{at},\"ordinal\":{ordinal}");
            assert_eq!(body.matches(&timed).count(), 1);
            body = body.replace(&timed, &format!("\"ordinal\":{ordinal}"));
        }
    }
    assert!(!body.contains("output_refs"), "{body}");
    check_golden(&body, "session_read.tagged_reference_reply.json");
    rig.finish().await;
}

/// The plan manifest a session freezes when its provider's catalog declares reference slots and
/// the plan does not opt into output references: the slots are not frozen.
#[tokio::test]
async fn declared_slots_without_opt_in_keep_the_manifest_bytes() {
    let (mut rig, admitter) = plan_rig("refs-pin-manifest").await;
    admitter.answer_reference_slots(json!([
        {"name": "data", "kind": "token"},
        {"name": "program", "kind": "source:rich-ui/1"},
    ]));
    rig.script.push(&[Step::Text("answer")]);
    send_plan(&mut rig, "turn-1", "hi", &tagged_plan(json!({}))).await;
    let records = rig.wait_for_finished(1).await;
    let manifest = records
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                manifest: Some(manifest),
                ..
            } => Some(manifest.clone()),
            _ => None,
        })
        .expect("the plan froze a manifest");
    let body = serde_json::to_string(&manifest).unwrap();
    check_golden(
        &body,
        "run_started.manifest.reference_slots_not_opted_in.json",
    );
    rig.finish().await;
}
