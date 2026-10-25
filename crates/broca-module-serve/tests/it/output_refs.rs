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

/// A finished tagged session: the user's `hi` (§1§), a `work` call answering `worked` (§2§),
/// and a final reply (§3§) whose text is `reply`. Returns the rig, the run id and the lineage id.
async fn referencing_session(tag: &str, reply: &'static str) -> (Rig, String, String) {
    let (mut rig, _) = plan_rig(tag).await;
    rig.script.push(&[Step::Call("call-1"), Step::Text(reply)]);
    let run_id = send_plan(&mut rig, "turn-1", "hi", &tagged_plan(json!({}))).await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let head = rig
        .request(7, json!({"method": "session.head", "params": {}}))
        .await;
    let lineage = result(&head)["lineage_id"].as_str().unwrap().to_owned();
    (rig, run_id, lineage)
}

#[tokio::test]
async fn history_pages_seek_both_ways_without_gaps_and_expire_foreign_cursors() {
    let (mut rig, _, _) = referencing_session("history-pages", "answer").await;
    let newest = call(&mut rig, 7, "session.history", json!({"limit": 2})).await;
    let newest = result(&newest);
    assert_eq!(
        newest["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["ordinal"].clone())
            .collect::<Vec<_>>(),
        [json!(2), json!(3)]
    );
    assert!(newest.get("newer").is_none());
    let zero = call(&mut rig, 7, "session.history", json!({"limit":0})).await;
    assert_eq!(result(&zero)["messages"], json!([]));
    assert!(result(&zero)["older"].is_string());
    assert!(result(&zero).get("newer").is_none());
    let older = call(
        &mut rig,
        7,
        "session.history",
        json!({"cursor": newest["older"], "limit": 2}),
    )
    .await;
    let older = result(&older);
    assert_eq!(
        older["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["ordinal"].clone())
            .collect::<Vec<_>>(),
        [json!(0), json!(1)]
    );
    assert!(older.get("older").is_none());
    let newer = call(
        &mut rig,
        7,
        "session.history",
        json!({"cursor": older["newer"], "limit": 2}),
    )
    .await;
    assert_eq!(result(&newer), newest);
    let raw = call(
        &mut rig,
        7,
        "session.read",
        json!({"from_ordinal": 0, "limit": 100}),
    )
    .await;
    let all = call(
        &mut rig,
        7,
        "session.history",
        json!({"from_mid": "m0", "limit": 100}),
    )
    .await;
    assert_eq!(result(&all)["messages"], result(&raw)["messages"]);
    for params in [json!({"before_mid":"m2"}), json!({"before_ordinal":2})] {
        let page = call(&mut rig, 7, "session.history", params).await;
        assert_eq!(result(&page)["messages"], older["messages"]);
    }
    for (params, code, field) in [
        (json!({"cursor":"bad"}), "invalid_params", Some("cursor")),
        (json!({"from_mid":"missing"}), "unknown_mid", None),
        (json!({"from_ordinal":999}), "unknown_ordinal", None),
        (
            json!({"from_mid":"m0", "before_ordinal":1}),
            "invalid_params",
            Some("before_ordinal"),
        ),
        (json!({"view":"model"}), "invalid_params", Some("view")),
        (json!({"form":"bogus"}), "invalid_params", Some("form")),
        (
            json!({"form":"display", "media":"inline"}),
            "invalid_params",
            Some("media"),
        ),
        (
            json!({"form":"display", "include_originals":true}),
            "invalid_params",
            Some("include_originals"),
        ),
        (
            json!({"limits":{"arguments_bytes":1,"result_bytes":1,"error_result_bytes":1}}),
            "invalid_params",
            Some("limits"),
        ),
        (json!({"unknown":true}), "invalid_params", Some("unknown")),
    ] {
        let reply = call(&mut rig, 7, "session.history", params).await;
        let error = refusal(&reply);
        assert_eq!(error["code"], code, "{error}");
        if let Some(field) = field {
            assert_eq!(error["detail"]["field"], field, "{error}");
        }
    }
    let (mut other, _, _) = referencing_session("history-foreign", "answer").await;
    let foreign = call(
        &mut other,
        7,
        "session.history",
        json!({"cursor": newest["older"]}),
    )
    .await;
    assert_eq!(refusal(&foreign)["code"], "cursor_expired");
    other.finish().await;
    let (mut empty, _) = plan_rig("history-empty").await;
    for params in [json!({}), json!({"form":"display"})] {
        let page = call(&mut empty, 7, "session.history", params).await;
        assert_eq!(result(&page), json!({"messages":[]}));
    }
    for (params, code) in [
        (json!({"cursor":newest["older"]}), "cursor_expired"),
        (json!({"from_mid":"m0"}), "unknown_mid"),
        (json!({"from_ordinal":0}), "unknown_ordinal"),
    ] {
        let page = call(&mut empty, 7, "session.history", params).await;
        assert_eq!(refusal(&page)["code"], code);
    }
    empty.finish().await;
    rig.finish().await;
}

#[tokio::test]
async fn history_display_limits_and_page_budgets_keep_the_requested_end() {
    let (mut rig, _, _) = referencing_session("history-limits", "answer").await;
    let display = call(&mut rig, 7, "session.history", json!({"form":"display", "limits":{"arguments_bytes":1,"result_bytes":3,"error_result_bytes":5}})).await;
    let page = result(&display);
    assert_eq!(page["messages"][1]["row"]["parts"][0]["arguments"], "{");
    assert_eq!(
        page["messages"][1]["row"]["parts"][0]["arguments_truncated"],
        true
    );
    assert_eq!(page["messages"][2]["row"]["parts"][0]["content"], "wor");
    assert_eq!(page["messages"][2]["row"]["parts"][0]["truncated"], true);
    assert_eq!(page["messages"][3]["row"]["text"], "answer");
    assert_eq!(page["messages"][3]["row"]["model"], "fake/fake");
    assert!(page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["written_at"].is_u64()));
    let newest = call(
        &mut rig,
        7,
        "session.history",
        json!({"form":"display", "max_bytes":1}),
    )
    .await;
    assert_eq!(result(&newest)["messages"].as_array().unwrap().len(), 1);
    assert_eq!(result(&newest)["messages"][0]["ordinal"], 3);
    let first = call(
        &mut rig,
        7,
        "session.history",
        json!({"from_ordinal":0, "max_bytes":1}),
    )
    .await;
    assert_eq!(result(&first)["messages"].as_array().unwrap().len(), 1);
    assert_eq!(result(&first)["messages"][0]["ordinal"], 0);
    rig.finish().await;
}

#[tokio::test]
async fn history_display_recovers_send_identity_without_changing_read_bodies() {
    let (mut rig, _) = plan_rig("history-send-identity").await;
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "core".into(),
        }),
    )
    .await;
    rig.script.push(&[Step::Text("answer")]);
    let sent = call(&mut rig, 7, "session.send", json!({"send_id":"turn-1", "client_message_id":"phone-1", "prompt":"hi", "model":{"provider":"fake","model":"fake"}, "plan":tagged_plan(json!({}))})).await;
    assert!(result(&sent)["run_id"].is_string());
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let display = call(&mut rig, 7, "session.history", json!({"form":"display"})).await;
    let row = &result(&display)["messages"][0]["row"];
    assert_eq!(row["client_message_id"], "phone-1");
    assert_eq!(row["sender"], "core");
    let raw = call(&mut rig, 7, "session.read", json!({})).await;
    let raw = String::from_utf8(raw.body).unwrap();
    assert!(!raw.contains("client_message_id"));
    assert!(!raw.contains("\"sender\""));
    rig.finish().await;
}

#[tokio::test]
async fn imported_history_refuses_the_first_assistant_without_a_model() {
    let (mut rig, _) = plan_rig("history-imported-model").await;
    let imported = call(&mut rig, 7, "session.import", json!({
        "import_id":"imported-tool-episode", "origin":{"kind":"import","source_ref":"fixture:imported-tool-episode"},
        "messages":[
            {"role":"user","content":[{"type":"text","text":"inspect the project"}]},
            {"role":"assistant","content":[{"type":"tool_call","tool_call_id":"imported-call","tool_name":"work","input":{}}]},
            {"role":"tool","content":[{"type":"tool_result","tool_call_id":"imported-call","is_error":false,"output":{"kind":"text","text":"worked"}}]},
            {"role":"assistant","origin":{"provider_module_id":"anthropic","model_id":"recorded-model"},"content":[{"type":"text","text":"imported answer"}]}
        ]
    })).await;
    assert!(result(&imported)["run_id"].is_string());
    rig.wait_idle().await;
    let raw = call(&mut rig, 7, "session.read", json!({})).await;
    let raw = result(&raw);
    let messages = raw["messages"].as_array().unwrap();
    let missing_times = messages
        .iter()
        .filter(|entry| !entry["written_at"].is_u64())
        .count();
    let missing_models = messages
        .iter()
        .filter(|entry| {
            entry["message"]["role"] == "assistant" && entry["message"]["origin"].is_null()
        })
        .count();
    assert_eq!((messages.len(), missing_times, missing_models), (4, 0, 1));
    println!("HISTORY_REPRESENTABILITY imported: rows=4 missing_written_at=0 missing_model=1 display_refusals=1");
    let before = rig.wal_bytes();
    let display = call(&mut rig, 7, "session.history", json!({"form":"display"})).await;
    let error = refusal(&display);
    assert_eq!(error["code"], "history_unrepresentable");
    assert_eq!(error["class"], "permanent");
    assert_eq!(error["detail"], json!({"ordinal":1,"field":"model"}));
    assert!(
        error.get("messages").is_none(),
        "no partial page accompanies a refusal"
    );
    assert_eq!(rig.wal_bytes(), before);
    let history = call(&mut rig, 7, "session.history", json!({})).await;
    assert_eq!(result(&history)["messages"], raw["messages"]);
    let known = call(
        &mut rig,
        7,
        "session.history",
        json!({"form":"display","from_ordinal":2}),
    )
    .await;
    assert_eq!(
        result(&known)["messages"][1]["row"]["model"],
        "anthropic/recorded-model"
    );
    rig.finish().await;
}

#[tokio::test]
async fn repeated_ids_within_a_message_keep_unknown_dispatch_metadata_null() {
    let (mut rig, admitter) = plan_rig("refs-duplicate-occurrences").await;
    admitter.admit_output_refs();
    admitter.answer_reference_slots(json!([{"name":"data","kind":"token"}]));
    let call_block = |marker: &str| {
        broca_protocol::ContentBlock::ToolCall(broca_protocol::ToolCall {
            tool_call_id: broca_protocol::ToolCallId("repeat".into()),
            tool_name: "work".into(),
            input: json!({"data":"$$tc(99)","marker":marker}),
            thought_signature: None,
        })
    };
    rig.script.push(&[
        Step::Usage {
            content: vec![call_block("first"), call_block("second")],
            usage: Default::default(),
        },
        Step::Text("see $$tc(2) and $$tc(3)"),
    ]);
    send_plan(
        &mut rig,
        "turn-1",
        "hi",
        &tagged_plan(json!({"output_refs":true})),
    )
    .await;
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::ToolResult { result, .. } if matches!(&result.output, broca_protocol::content::ToolOutput::Error { message } if message == "duplicate tool_call_id from provider")))
            .count(),
        1
    );
    let raw = call(&mut rig, 7, "session.read", json!({})).await;
    let raw = result(&raw);
    let lineage = raw["lineage_id"].as_str().unwrap();
    let first_key = raw["messages"][1]["tool_calls"][0]["call_key"].clone();
    assert!(first_key.is_string());
    let refs = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id":format!("{lineage}/m3")}),
    )
    .await;
    let refs = result(&refs);
    assert_eq!(refs["records"].as_array().unwrap().len(), 2);
    assert_eq!(refs["records"][0]["call_key"], first_key);
    assert_eq!(refs["records"][0]["provider"], WORK_MODULE);
    assert!(refs["records"][1]["call_key"].is_null());
    assert!(refs["records"][1]["provider"].is_null());
    assert_eq!(refs["records"][1]["tool"], "work");
    let display = call(&mut rig, 7, "session.history", json!({"form":"display"})).await;
    let display = result(&display);
    let parts = &display["messages"][1]["row"]["parts"];
    assert_eq!(parts[0]["output_refs"]["call_key"], first_key);
    assert!(parts[1]["output_refs"]["call_key"].is_null());
    assert_eq!(
        parts[1]["output_refs"]["set_id"],
        format!("{lineage}/m1/c1")
    );
    rig.finish().await;
}

#[tokio::test]
async fn repeated_ids_across_messages_keep_each_messages_own_dispatch() {
    let (mut rig, admitter) = plan_rig("refs-reused-id-messages").await;
    admitter.admit_output_refs();
    admitter.answer_reference_slots(json!([{"name":"data","kind":"token"}]));
    rig.script.push(&[
        Step::Call("repeat"),
        Step::Call("repeat"),
        Step::Text("see $$tc(2) and $$tc(3)"),
    ]);
    send_plan(
        &mut rig,
        "turn-1",
        "hi",
        &tagged_plan(json!({"output_refs":true})),
    )
    .await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let raw = call(&mut rig, 7, "session.read", json!({})).await;
    let raw = result(&raw);
    let lineage = raw["lineage_id"].as_str().unwrap();
    let keys = [
        raw["messages"][1]["tool_calls"][0]["call_key"].clone(),
        raw["messages"][3]["tool_calls"][0]["call_key"].clone(),
    ];
    assert!(keys.iter().all(Value::is_string));
    assert_ne!(keys[0], keys[1]);
    let refs = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id":format!("{lineage}/m5")}),
    )
    .await;
    let refs = result(&refs);
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(refs["records"][index]["call_key"], *key);
        assert_eq!(refs["records"][index]["provider"], WORK_MODULE);
    }
    let display = call(&mut rig, 7, "session.history", json!({"form":"display"})).await;
    let display = result(&display);
    assert_eq!(
        display["messages"][1]["row"]["parts"][0]["output_refs"]["call_key"],
        keys[0]
    );
    assert_eq!(
        display["messages"][3]["row"]["parts"][0]["output_refs"]["call_key"],
        keys[1]
    );
    rig.finish().await;
}

/// `method` with `params` on `channel`.
async fn call(rig: &mut Rig, channel: u16, method: &str, params: Value) -> subc_protocol::Frame {
    rig.request(channel, json!({"method": method, "params": params}))
        .await
}

/// The error body of a refused request.
fn refusal(frame: &subc_protocol::Frame) -> Value {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The reply's set holds one record per `$$tc(N)`, in encounter order: the tool output it names
/// resolves to the stored result with its call's key, tool and module; the user's text is not a
/// tool output; a number no part holds is unknown. The resolved record's content reads back as
/// the stored bytes, without the `§2§` prefix the model saw. Every reply is pinned, the empty set
/// of the tool-call message included.
#[tokio::test]
async fn a_reply_set_resolves_against_the_writing_request() {
    let (mut rig, run_id, lineage) =
        referencing_session("refs-reply-set", "see $$tc(2), $$tc(1) and $$tc(99)").await;
    let set = format!("{lineage}/m3");
    let page = call(&mut rig, 7, "session.output_refs", json!({"set_id": set})).await;
    let records = result(&page)["records"].clone();
    assert_eq!(
        records,
        json!([
            {"ref_id": format!("{set}/r0"), "tag": "2", "part": "m2#0",
             "call_key": format!("{lineage}:5"), "tool": "work", "provider": WORK_MODULE,
             "format": "text", "bytes": 6, "sha256": sha256_hex(b"worked"), "state": "resolved"},
            {"ref_id": format!("{set}/r1"), "tag": "1", "part": "m0#0", "call_key": null,
             "tool": null, "provider": null, "format": null, "bytes": null, "sha256": null,
             "state": "unresolved", "reason": "not_a_tool_output"},
            {"ref_id": format!("{set}/r2"), "tag": "99", "part": null, "call_key": null,
             "tool": null, "provider": null, "format": null, "bytes": null, "sha256": null,
             "state": "unresolved", "reason": "unknown_tag"},
        ])
    );
    let parsed: Vec<cortexkit_role_llm_runner::output_refs::Record> =
        serde_json::from_value(records).unwrap();
    let derived = broca_module::output_refs::message_output_refs(
        rig.registry().as_ref(),
        &rig.session,
        3,
        None,
    )
    .await
    .unwrap()
    .expect("the reply has a set")
    .1;
    assert!(
        derived.reply.descriptor.check(&parsed),
        "the descriptor's count and digest match the served records"
    );
    assert_eq!(derived.reply.descriptor.mid, "m3");
    assert_eq!(derived.reply.descriptor.dialect, "rich-ui/1");
    check_golden(
        &normalized(&page.body, &run_id, &lineage),
        "output_refs.page.json",
    );

    let after = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": set, "after": format!("{set}/r0")}),
    )
    .await;
    assert_eq!(result(&after)["records"].as_array().unwrap().len(), 2);
    let last = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": set, "after": format!("{set}/r2")}),
    )
    .await;
    assert_eq!(result(&last), json!({"records": []}));

    let read = call(
        &mut rig,
        7,
        "session.output_ref_read",
        json!({"ref_id": format!("{set}/r0"), "offset": 0, "length": 131_072}),
    )
    .await;
    assert_eq!(
        result(&read),
        json!({"data": "d29ya2Vk", "size": 6, "format": "text"})
    );
    check_golden(
        &normalized(&read.body, &run_id, &lineage),
        "output_ref_read.reply.json",
    );
    let range = call(
        &mut rig,
        7,
        "session.output_ref_read",
        json!({"ref_id": format!("{set}/r0"), "offset": 2, "length": 3}),
    )
    .await;
    assert_eq!(result(&range)["data"], "cmtl", "bytes 2..5 of `worked`");
    for (offset, length) in [(0, 0), (6, 1)] {
        let metadata = call(
            &mut rig,
            7,
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r0"), "offset": offset, "length": length}),
        )
        .await;
        assert_eq!(
            result(&metadata),
            json!({"data": "", "size": 6, "format": "text"})
        );
    }

    // The tool-call message was written by a model step of the same run: its set is empty.
    let empty = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": format!("{lineage}/m1")}),
    )
    .await;
    assert_eq!(result(&empty), json!({"records": []}));
    check_golden(
        &normalized(&empty.body, &run_id, &lineage),
        "output_refs.empty.json",
    );
    rig.finish().await;
}

/// A number the transcript assigned after the reply was written was not in its request, so it
/// stays unknown even though a part holds it now.
#[tokio::test]
async fn a_number_assigned_after_the_reply_is_not_in_its_request() {
    let (mut rig, _, lineage) =
        referencing_session("refs-later-number", "see $$tc(2) and $$tc(4)").await;
    rig.script.push(&[Step::Text("again")]);
    send_plan(&mut rig, "turn-2", "more", &tagged_plan(json!({}))).await;
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let read = call(&mut rig, 7, "session.read", json!({"from_ordinal": 4})).await;
    assert_eq!(
        result(&read)["messages"][0]["tags"],
        json!([{"part": "m4#0", "tag": "§4§"}]),
        "the second prompt holds §4§ now"
    );
    let page = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": format!("{lineage}/m3")}),
    )
    .await;
    let records = result(&page)["records"].clone();
    assert_eq!(records[0]["state"], "resolved");
    assert_eq!(records[1]["tag"], "4");
    assert_eq!(records[1]["reason"], "unknown_tag");
    assert_eq!(records[1]["part"], Value::Null);
    rig.finish().await;
}

/// Every refusal carries its contract code, names the field where it has one, and writes
/// nothing. A set or reference minted under another lineage, a message without a set, a record
/// that is not a record of the set, a cursor outside the set, and a record without retained
/// content all answer `unknown_ref`; ranges outside the content or the read cap are
/// `invalid_params` naming `offset` or `length`.
#[tokio::test]
async fn refusals_carry_the_contract_codes_and_write_nothing() {
    let (mut rig, run_id, lineage) =
        referencing_session("refs-refusals", "see $$tc(2), $$tc(1) and $$tc(99)").await;
    let set = format!("{lineage}/m3");
    let foreign = "00000000-0000-4000-8000-000000000000";
    let wal = rig.wal_bytes();
    let cases = [
        (
            "session.output_refs",
            json!({"set_id": format!("{foreign}/m3")}),
            "output_refs.unknown_ref.foreign_set.json",
        ),
        (
            "session.output_refs",
            json!({"set_id": format!("{lineage}/m0")}),
            "output_refs.unknown_ref.no_set.json",
        ),
        (
            "session.output_refs",
            json!({"set_id": set, "after": format!("{set}/r9")}),
            "output_refs.unknown_ref.cursor.json",
        ),
        (
            "session.output_refs",
            json!({"after": format!("{set}/r0")}),
            "output_refs.invalid_params.set_id.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{foreign}/m3/r0"), "offset": 0, "length": 1}),
            "output_ref_read.unknown_ref.foreign_ref.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r1"), "offset": 0, "length": 1}),
            "output_ref_read.unknown_ref.not_retained.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r3"), "offset": 0, "length": 1}),
            "output_ref_read.unknown_ref.no_record.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r0"), "offset": 7, "length": 0}),
            "output_ref_read.invalid_params.offset.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r0"), "offset": 0, "length": 131_073}),
            "output_ref_read.invalid_params.length.json",
        ),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{foreign}/m3/r0"), "offset": 0, "length": 131_073}),
            "output_ref_read.invalid_params.length.json",
        ),
    ];
    for (method, params, file) in cases {
        let frame = call(&mut rig, 7, method, params.clone()).await;
        let body = refusal(&frame);
        let expected = if file.contains("invalid_params") {
            "invalid_params"
        } else {
            "unknown_ref"
        };
        assert_eq!(body["code"], expected, "{method} {params}: {body}");
        check_golden(&normalized(&frame.body, &run_id, &lineage), file);
    }
    // An id spelled differently from the one minted names nothing, even when it parses.
    for alias in [
        format!("{lineage}/m03"),
        format!("{lineage}/m3/"),
        format!("{set}/c"),
    ] {
        let frame = call(&mut rig, 7, "session.output_refs", json!({"set_id": alias})).await;
        assert_eq!(refusal(&frame)["code"], "unknown_ref", "{alias}");
    }
    assert_eq!(rig.wal_bytes(), wal, "a refusal writes nothing");
    rig.finish().await;
}

/// Another session's ids answer `unknown_ref` exactly like ids that never existed, and a caller
/// that may not read the session is refused as `session.read` refuses it, before anything else.
#[tokio::test]
async fn ids_are_session_bound_and_access_follows_session_read() {
    let (mut rig, run_id, lineage) = referencing_session("refs-access", "see $$tc(2)").await;
    let set = format!("{lineage}/m3");
    rig.bind_session(9, "another").await;
    for (method, params) in [
        ("session.output_refs", json!({"set_id": set})),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r0"), "offset": 0, "length": 0}),
        ),
    ] {
        let frame = call(&mut rig, 9, method, params).await;
        assert_eq!(refusal(&frame)["code"], "unknown_ref", "{method}");
    }

    rig.bind_as(
        11,
        Some(subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        }),
    )
    .await;
    let read = call(&mut rig, 11, "session.read", json!({})).await;
    let read_refusal = refusal(&read);
    assert_eq!(read_refusal["code"], "sender_not_owner");
    let wal = rig.wal_bytes();
    for (method, params) in [
        ("session.history", json!({"form":"display"})),
        ("session.output_refs", json!({"set_id": set})),
        (
            "session.output_ref_read",
            json!({"ref_id": format!("{set}/r0"), "offset": 0, "length": 0}),
        ),
    ] {
        let frame = call(&mut rig, 11, method, params).await;
        assert_eq!(
            refusal(&frame),
            read_refusal,
            "{method} is refused as session.read is"
        );
        check_golden(
            &normalized(&frame.body, &run_id, &lineage),
            "output_refs.sender_not_owner.json",
        );
    }
    assert_eq!(rig.wal_bytes(), wal, "a refusal writes nothing");
    rig.finish().await;
}

/// The view a past request rendered is found from the log: a view applied before the step that
/// wrote the message is in effect for it; one applied after that step is not.
#[tokio::test]
async fn the_view_in_effect_is_the_last_one_applied_before_the_writing_step() {
    let (rig, _, _) = referencing_session("refs-view-step", "see $$tc(2)").await;
    let records = rig.records();
    let last_step = records
        .iter()
        .rposition(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let applied = |version: u64| WalRecord::CompactionViewApplied {
        provider: "compactor".into(),
        view_version: version,
        view: broca_wal::OpaqueJson::from_json_text(
            json!({
                "session_ref": "session", "view_version": version, "lineage_id": "lineage",
                "covers_through": {"ordinal": 2, "mid": "m2"},
                "ranges": [{"kind": "replace", "from": 0, "to": 3, "bytes": "[]"}],
                "digest": "00",
            })
            .to_string(),
        )
        .unwrap(),
        choice: broca_wal::CompactionViewChoice::Applied,
    };
    let version = |records: &[WalRecord], ordinal| {
        broca_module::output_refs::view_at_writing_step(records, ordinal)
            .unwrap()
            .map(|view| view.view_version)
    };
    assert_eq!(version(&records, 3), None, "no view applied");

    let mut before = records.clone();
    before.insert(last_step, applied(1));
    assert_eq!(
        version(&before, 3),
        Some(1),
        "applied before the final step"
    );
    assert_eq!(
        version(&before, 1),
        None,
        "the tool-call message was written by an earlier step"
    );

    let mut after = records.clone();
    after.insert(last_step + 1, applied(2));
    assert_eq!(version(&after, 3), None, "applied after the final step");

    let mut both = before.clone();
    both.insert(last_step + 2, applied(2));
    assert_eq!(
        version(&both, 3),
        Some(1),
        "a later view does not apply backwards"
    );
    rig.finish().await;
}

/// A plan whose `work` provider opts into output references freezes its catalog's reference
/// slots (feature `plan-reference-slots/v1`), and a call of that tool gets its own set: only
/// declared string slots are scanned, a `token` slot must be exactly one reference, a
/// `source:` slot is scanned as a program (a reference inside a string literal is none), and
/// other arguments are never read. The set's descriptor names the containing call's key.
#[tokio::test]
async fn an_opted_in_call_scans_only_its_declared_slots() {
    let (mut rig, admitter) = plan_rig("refs-call-slots").await;
    admitter.admit_output_refs();
    admitter.answer_reference_slots(json!([
        {"name": "data", "kind": "token"},
        {"name": "program", "kind": "source:rich-ui/1"},
    ]));
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Usage {
            content: vec![
                broca_protocol::content::ContentBlock::Text {
                    text: "see $$tc(2)".into(),
                },
                broca_protocol::content::ContentBlock::ToolCall(broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-2".into()),
                    tool_name: "work".into(),
                    input: json!({
                        "data": "extra $$tc(2)",
                        "program": "export const x = $$tc(2)\nexport const y = \"$$tc(2)\"",
                        "other": "$$tc(2)",
                    }),
                    thought_signature: None,
                }),
            ],
            usage: Default::default(),
        },
        Step::Text("done"),
    ]);
    let run_id = send_plan(
        &mut rig,
        "turn-1",
        "hi",
        &tagged_plan(json!({"output_refs": true})),
    )
    .await;
    let records = rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let started = records
        .iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    assert!(
        started
            .required_features()
            .contains(&broca_wal::PLAN_REFERENCE_SLOTS_FEATURE),
        "frozen slots are must-understand"
    );
    let WalRecord::RunStarted {
        manifest: Some(manifest),
        ..
    } = started
    else {
        panic!("the plan froze a manifest")
    };
    let broca_wal::FetchOutcome::Answered {
        reference_slots: Some(slots),
        ..
    } = &manifest.fetches[0].outcome
    else {
        panic!("the opted-in provider's slots are frozen")
    };
    assert_eq!(
        serde_json::to_value(slots).unwrap(),
        json!({"work": [{"name": "data", "kind": "token"},
                        {"name": "program", "kind": "source:rich-ui/1"}]})
    );

    let head = rig
        .request(7, json!({"method": "session.head", "params": {}}))
        .await;
    let lineage = result(&head)["lineage_id"].as_str().unwrap().to_owned();
    let reply = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": format!("{lineage}/m3")}),
    )
    .await;
    let reply = result(&reply)["records"].clone();
    assert_eq!(reply.as_array().unwrap().len(), 1, "{reply}");
    assert_eq!(reply[0]["state"], "resolved");

    let set = format!("{lineage}/m3/c1");
    let page = call(&mut rig, 7, "session.output_refs", json!({"set_id": set})).await;
    let records = result(&page)["records"].clone();
    assert_eq!(records.as_array().unwrap().len(), 2, "{records}");
    assert_eq!(records[0]["reason"], "not_a_reference");
    assert_eq!(records[0]["tag"], Value::Null);
    assert_eq!(records[1]["state"], "resolved");
    assert_eq!(records[1]["tag"], "2");
    assert_eq!(records[1]["part"], "m2#0");
    check_golden(
        &normalized(&page.body, &run_id, &lineage),
        "output_refs.call_set.page.json",
    );
    let read = call(
        &mut rig,
        7,
        "session.output_ref_read",
        json!({"ref_id": format!("{set}/r1"), "offset": 0, "length": 6}),
    )
    .await;
    assert_eq!(result(&read)["data"], "d29ya2Vk");

    let sets = broca_module::output_refs::message_output_refs(
        rig.registry().as_ref(),
        &rig.session,
        3,
        None,
    )
    .await
    .unwrap()
    .expect("the message has sets")
    .1;
    assert_eq!(sets.calls.len(), 1);
    let (id, call_set) = &sets.calls[0];
    assert_eq!(id, "call-2");
    assert_eq!(call_set.descriptor.set_id, set);
    let display = call(
        &mut rig,
        7,
        "session.history",
        json!({"form": "display", "limit": 1000}),
    )
    .await;
    let display = result(&display);
    let row = &display["messages"][3]["row"];
    assert_eq!(
        row["output_refs"],
        serde_json::to_value(&sets.reply.descriptor).unwrap()
    );
    assert_eq!(
        row["parts"][1]["output_refs"],
        serde_json::to_value(&call_set.descriptor).unwrap()
    );
    assert_eq!(row["parts"][1]["tool_call_id"], "call-2");
    assert_eq!(row["parts"][1]["part"], "m3#1");
    let dispatched = call(&mut rig, 7, "session.read", json!({"from_ordinal": 3})).await;
    assert_eq!(
        call_set.descriptor.call_key.as_deref(),
        result(&dispatched)["messages"][0]["tool_calls"][0]["call_key"].as_str(),
        "the descriptor names the containing call"
    );
    rig.finish().await;
}

/// A plan item that does not opt in leaves its provider's calls unscanned: no call set exists.
#[tokio::test]
async fn a_call_without_opt_in_has_no_set() {
    let (mut rig, admitter) = plan_rig("refs-call-no-opt-in").await;
    admitter.admit_output_refs();
    admitter.answer_reference_slots(json!([{"name": "data", "kind": "token"}]));
    rig.script.push(&[
        Step::Call("call-1"),
        Step::Usage {
            content: vec![broca_protocol::content::ContentBlock::ToolCall(
                broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("call-2".into()),
                    tool_name: "work".into(),
                    input: json!({"data": "$$tc(2)"}),
                    thought_signature: None,
                },
            )],
            usage: Default::default(),
        },
        Step::Text("done"),
    ]);
    send_plan(
        &mut rig,
        "turn-1",
        "hi",
        &tagged_plan(json!({"output_refs": false})),
    )
    .await;
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let head = rig
        .request(7, json!({"method": "session.head", "params": {}}))
        .await;
    let lineage = result(&head)["lineage_id"].as_str().unwrap().to_owned();
    let frame = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": format!("{lineage}/m3/c0")}),
    )
    .await;
    assert_eq!(refusal(&frame)["code"], "unknown_ref");
    let reply = call(
        &mut rig,
        7,
        "session.output_refs",
        json!({"set_id": format!("{lineage}/m3")}),
    )
    .await;
    assert_eq!(result(&reply), json!({"records": []}));
    rig.finish().await;
}
