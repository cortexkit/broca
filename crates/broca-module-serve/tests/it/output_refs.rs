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
