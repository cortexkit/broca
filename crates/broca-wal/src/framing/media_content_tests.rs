use super::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn block() -> Value {
    json!({"type":"media","kind":"image","mime":"image/png","byte_size":42,"sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","width":3,"height":2})
}

fn result(marker_only: bool) -> Value {
    let part = if marker_only {
        json!({"kind":"text","text":"[media not shown: kind=image mime=image/png bytes=42 sha256=0123456789ab reason=store_failed]"})
    } else {
        let mut media = block();
        media.as_object_mut().unwrap().remove("type");
        json!({"kind":"media","media":media})
    };
    json!({"tool_call_id":"call","output":{"kind":"parts","parts":[part]},"is_error":true})
}

fn view_record(message: &Value, replace: bool) -> Value {
    let range = if replace {
        json!({"kind":"replace","from":0,"to":1,"bytes":json!([message]).to_string()})
    } else {
        json!({"kind":"keep","from":0,"to":1,"edits":[{"ordinal":0,"mid":"m","part":"m#0","bytes":message["content"][0].to_string()}]})
    };
    // Framing inspects native bytes but does not validate the provider's view digest.
    let view = json!({"session_ref":"0123456789abcdef0123456789abcdef","view_version":1,"lineage_id":"l","covers_through":{"ordinal":0,"mid":"m"},"ranges":[range],"digest":"00"});
    json!({"type":"compaction_view_applied","provider":"mc","view_version":1,"view":view.to_string(),"choice":{"kind":"applied"}})
}

fn carriers(content: Value, result: Value) -> Vec<(&'static str, Value)> {
    let message = json!({"role":"user","content":[content]});
    let mut compaction: Value = serde_json::from_str(
        include_str!("../../tests/fixtures/compaction_records.jsonl")
            .lines()
            .find(|line| line.contains("\"type\":\"compaction_message\""))
            .unwrap(),
    )
    .unwrap();
    compaction["replacement"] = json!([message.clone()]);
    vec![
        (
            "RunStarted.input",
            json!({"type":"run_started","run_id":"r","session":{"project_root":"/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[message.clone()]}),
        ),
        (
            "ModelStepFinished.assistant_message",
            json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":message["content"]},"usage":{},"retries_used":0}),
        ),
        (
            "Steered.input",
            json!({"type":"steered","input":[message.clone()]}),
        ),
        (
            "Queued.request",
            json!({"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[message.clone()],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}),
        ),
        ("CompactionMessage.replacement", compaction),
        (
            "ToolResult.result",
            json!({"type":"tool_result","batch_id":"b","result":result,"outcome":"completed"}),
        ),
        (
            "ApprovalRecorded.closing_result",
            json!({"type":"approval_recorded","batch_id":"b","call_id":"call","call_key":"k","decision":"declined","closing_result":result}),
        ),
        (
            "LateResult.result",
            json!({"type":"late_result","call_key":"k","outcome":"completed","result":result}),
        ),
        (
            "SendDelivered.input",
            json!({"type":"send_delivered","submission_id":"q","run_id":"r","mode":"steer","input":[message.clone()]}),
        ),
        ("CompactionViewApplied.view", view_record(&message, false)),
        ("CompactionViewApplied.view", view_record(&message, true)),
    ]
}

fn hook_metadata(member: &str, subject: Value, hook: &str) -> Value {
    json!({member:subject,"ops":[{"module":"transform","hook":hook,"phase":if hook == "pre_tool" { Some("mutate") } else { None },"op":"replace"}]})
}

fn media_records() -> Vec<(&'static str, Value)> {
    let mut records = carriers(block(), result(false));
    let plain_result =
        json!({"tool_call_id":"call","output":{"kind":"text","text":"old"},"is_error":false});
    let plain = carriers(json!({"type":"text","text":"old"}), plain_result);
    let message = json!({"role":"user","content":[block()]});
    let assistant = json!({"message_id":"m","content":[block()]});
    for (name, template) in plain {
        if [
            "RunStarted.input",
            "ModelStepFinished.assistant_message",
            "Steered.input",
            "Queued.request",
            "ToolResult.result",
        ]
        .contains(&name)
        {
            for subject in ["original", "final"] {
                let mut value = template.clone();
                let (native, hook) = match name {
                    "RunStarted.input" | "Steered.input" | "Queued.request" => {
                        (json!([message.clone()]), "pre_user")
                    }
                    "ModelStepFinished.assistant_message" => (assistant.clone(), "post_assistant"),
                    "ToolResult.result" => (result(true), "post_tool"),
                    _ => unreachable!(),
                };
                value["hooks"] = hook_metadata(subject, native, hook);
                let slot = match name {
                    "RunStarted.input" => "RunStarted.hooks",
                    "ModelStepFinished.assistant_message" => "ModelStepFinished.hooks",
                    "Steered.input" => "Steered.hooks",
                    "Queued.request" => "Queued.hooks",
                    "ToolResult.result" => "ToolResult.hooks",
                    _ => unreachable!(),
                };
                records.push((slot, value));
            }
        }
    }
    records
}

#[test]
fn media_content_is_must_understand_in_every_carrier() {
    let older: Vec<_> = KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != MEDIA_CONTENT_FEATURE)
        .collect();
    let mut records = media_records();
    let mut marker = result(true);
    marker["type"] = "tool_result".into();
    records.extend(carriers(marker, result(true)));
    for (slot, value) in records {
        let record: WalRecord =
            serde_json::from_value(value).unwrap_or_else(|error| panic!("{slot}: {error}"));
        assert!(
            record.required_features().contains(&MEDIA_CONTENT_FEATURE),
            "ungated {slot}"
        );
        let bytes = encode(1, 1, &record).unwrap();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [MEDIA_CONTENT_FEATURE]),
            "old reader accepted {slot}"
        );
        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("current reader refused {slot}")
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
    }
}

#[test]
fn hook_native_media_subjects_require_the_feature() {
    let records: Vec<_> = media_records()
        .into_iter()
        .filter(|(slot, _)| slot.ends_with(".hooks"))
        .collect();
    assert_eq!(
        records.len(),
        10,
        "both subjects of each native-content hook phase are exercised"
    );
    for (slot, value) in records {
        let record: WalRecord = serde_json::from_value(value).unwrap();
        assert!(
            record.required_features().contains(&MEDIA_CONTENT_FEATURE),
            "ungated native hook subject: {slot}"
        );
    }
}

#[test]
fn hook_tool_arguments_never_require_media_content() {
    let plain =
        json!({"tool_call_id":"call","output":{"kind":"text","text":"old"},"is_error":false});
    for arguments in [block(), json!({"nested":[block()]}), result(true)] {
        for member in ["original", "final"] {
            let records = [
                json!({"type":"tool_dispatch_intent","batch_id":"b","tool_call_id":"call","tool_name":"read","args":arguments}),
                json!({"type":"tool_no_dispatch","batch_id":"b","tool_call_id":"call","reason":{"kind":"denied","reason":"no"}}),
                json!({"type":"tool_result","batch_id":"b","result":plain,"outcome":"completed","reason":"pre_tool_denied"}),
                json!({"type":"tool_result","batch_id":"b","result":plain,"outcome":"completed","reason":"pre_tool_unavailable"}),
            ];
            for mut value in records {
                value["hooks"] = hook_metadata(member, arguments.clone(), "pre_tool");
                for operations in [value["hooks"]["ops"].clone(), json!([])] {
                    value["hooks"]["ops"] = operations;
                    let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
                    assert!(
                        !record.required_features().contains(&MEDIA_CONTENT_FEATURE),
                        "tool arguments are not native content: {record:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn post_tool_unavailable_keeps_media_gate_without_operations() {
    let record: WalRecord = serde_json::from_value(json!({
        "type":"tool_result","batch_id":"b","outcome":"completed","reason":"post_tool_unavailable",
        "result":{"tool_call_id":"call","output":{"kind":"error","message":"result withheld"},"is_error":true},
        "hooks":{"original":result(true),"ops":[],"unavailable":["transform"]}
    })).unwrap();
    assert!(record.required_features().contains(&MEDIA_CONTENT_FEATURE));
}

#[test]
fn hook_and_view_native_content_do_not_inspect_tool_payloads() {
    for block in [
        json!({"type":"tool_call","tool_call_id":"call","tool_name":"read","input":{"nested":[block()]}}),
        json!({"type":"tool_result","tool_call_id":"call","output":{"kind":"json","value":{"nested":[block()]}},"is_error":false}),
    ] {
        let message = json!({"role":"user","content":[block]});
        for replace in [false, true] {
            let record: WalRecord = serde_json::from_value(view_record(&message, replace)).unwrap();
            assert!(
                !record.required_features().contains(&MEDIA_CONTENT_FEATURE),
                "view contains only tool payload data"
            );
        }
        for (slot, mut value) in carriers(
            json!({"type":"text","text":"old"}),
            json!({"tool_call_id":"call","output":{"kind":"text","text":"old"},"is_error":false}),
        ) {
            let subject = match slot {
                "RunStarted.input" | "Queued.request" | "Steered.input" => {
                    Some((json!([message.clone()]), "pre_user"))
                }
                "ModelStepFinished.assistant_message" => Some((
                    json!({"message_id":"m","content":message["content"]}),
                    "post_assistant",
                )),
                "ToolResult.result" if block["type"] == "tool_result" => {
                    Some((block.clone(), "post_tool"))
                }
                _ => None,
            };
            if let Some((subject, hook)) = subject {
                for member in ["original", "final"] {
                    value["hooks"] = hook_metadata(member, subject.clone(), hook);
                    let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
                    assert!(
                        !record.required_features().contains(&MEDIA_CONTENT_FEATURE),
                        "false media gate in {slot}"
                    );
                }
            }
        }
    }
}

#[test]
fn typed_view_scan_keeps_media_gate_beside_dropped_edits() {
    let message = json!({"role":"user","content":[block()]});
    let mut record = view_record(&message, false);
    let mut view: Value = serde_json::from_str(record["view"].as_str().unwrap()).unwrap();
    view["ranges"][0]["edits"]
        .as_array_mut()
        .unwrap()
        .insert(0, json!({"ordinal":0,"mid":"m","part":"m#1"}));
    record["view"] = view.to_string().into();
    let record: WalRecord = serde_json::from_value(record).unwrap();
    assert!(record.required_features().contains(&MEDIA_CONTENT_FEATURE));
}

#[test]
fn media_carrier_inventory_covers_every_content_field() {
    // Audit field types, not just today's hand-written feature match. A newly added carrier
    // requires a test case even when a wildcard in the production scan would silently skip it.
    let source = include_str!("../record.rs");
    let body = source
        .split("pub enum WalRecord {")
        .nth(1)
        .unwrap()
        .split("\n}\n")
        .next()
        .unwrap();
    let mut variant = "";
    let mut fields = BTreeSet::new();
    for line in body.lines() {
        if let Some(line) = line.strip_prefix("    ") {
            if !line.starts_with(' ') && line.ends_with(" {") {
                variant = line.trim_end_matches(" {");
            }
        }
        if let Some(field) = line.strip_prefix("        ") {
            if !field.starts_with(' ') && !field.starts_with('/') && !field.starts_with('#') {
                if let Some((name, ty)) = field.split_once(':') {
                    if [
                        "Message",
                        "AssistantMessage",
                        "ToolResult",
                        "ContentBlock",
                        "SubmissionRequest",
                        "Hooks",
                        "Value",
                        "OpaqueJson",
                    ]
                    .iter()
                    .any(|kind| {
                        ty.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                            .any(|token| token == *kind)
                    }) {
                        fields.insert(format!("{variant}.{name}"));
                    }
                }
            }
        }
    }
    let mut covered: BTreeSet<_> = media_records()
        .into_iter()
        .map(|(slot, _)| slot.to_owned())
        .collect();
    // These JSON fields are configuration, conditions, pre-tool arguments or hash-derived
    // cache diagnostics, never native message content.
    covered.extend(
        [
            "RunConfigRefrozen.frozen_render_config",
            "ModelAttemptFinished.cache_evidence",
            "ToolDispatchIntent.args",
            "ToolDispatchIntent.hooks",
            "ToolNoDispatch.hooks",
            "CompactionMessage.call_when",
            "CompactionMessage.ceiling",
        ]
        .map(str::to_owned),
    );
    assert_eq!(
        fields, covered,
        "review new content or opaque-JSON carriers and exercise their media gate"
    );
}

#[test]
fn media_append_raises_floor_before_first_frame() {
    use crate::{FileWal, Wal, WalError};
    let root = broca_test_support::scratch_root().join(format!(
        "media-floor-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path = root.join("session.wal");
    let floor = root.join("wal-feature-floor");
    let record: WalRecord =
        serde_json::from_value(carriers(block(), result(false)).remove(5).1).unwrap();
    let plain: WalRecord =
        serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap();
    let mut wal = FileWal::open(&path).unwrap();
    wal.append(1, &plain, true).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        wal.append(1, &record, true),
        Err(WalError::FeatureFloor { path: None, .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(wal);
    std::fs::create_dir_all(&floor).unwrap();
    let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    assert!(matches!(
        wal.append(1, &record, true),
        Err(WalError::FeatureFloor { .. })
    ));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "floor failure must precede frame write"
    );
    drop(wal);
    std::fs::remove_dir(&floor).unwrap();
    let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    wal.append(1, &plain, true).unwrap();
    assert!(!floor.exists());
    wal.append(1, &record, true).unwrap();
    assert_eq!(
        crate::floor::read_floor(&floor).unwrap(),
        BTreeSet::from([MEDIA_CONTENT_FEATURE.to_owned()])
    );
    assert_eq!(wal.replay().unwrap().len(), 3);
}

#[test]
fn no_media_keeps_previous_frame_features_even_for_media_looking_tool_json() {
    let json_output =
        json!({"tool_call_id":"call","output":{"kind":"json","value":block()},"is_error":false});
    for (slot, value) in carriers(
        json!({"type":"tool_result", "tool_call_id":"call", "output":json_output["output"], "is_error":false}),
        json_output.clone(),
    ) {
        let record: WalRecord =
            serde_json::from_value(value).unwrap_or_else(|error| panic!("{slot}: {error}"));
        assert!(
            !record.required_features().contains(&MEDIA_CONTENT_FEATURE),
            "false media gate for {slot}"
        );
    }
    let ordinary: WalRecord = serde_json::from_value(
        json!({"type":"tool_result","batch_id":"b","result":json_output,"outcome":"completed"}),
    )
    .unwrap();
    assert_eq!(encode(1, 1, &ordinary).unwrap()[4], FRAMING_VERSION);
}
