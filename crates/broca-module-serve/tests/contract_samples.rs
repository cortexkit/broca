//! Wire samples exercised through the same serve loop as session callers.
mod delivery_rig;

use broca_session_store::record::Record;
use delivery_rig::*;
use serde_json::json;

fn capture(name: &str, bytes: &[u8]) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        println!(
            "BROCA_SAMPLE {name} {}",
            std::str::from_utf8(bytes).unwrap()
        );
    }
}

#[tokio::test]
async fn describe_contract_sample() {
    let mut rig = Rig::start("contract-describe-sample").await;
    let frame = rig
        .request(7, json!({"method":"role.describe","params":{}}))
        .await;
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
    let describe = result(&frame);
    let groups = describe["capabilities"].as_array().unwrap();
    for required in ["message_senders", "read_display_facts", "call_identity"] {
        assert!(
            groups.iter().any(|group| group == required),
            "missing {required}"
        );
    }
    assert_eq!(
        groups.iter().any(|group| group == "notices"),
        broca_core::delivery_order::RIDES_V2_WRITER_ENABLED,
        "notices must follow the installed envelope writer"
    );
    for undeclared in ["sender_attribution", "provider_executed"] {
        assert!(
            !groups.iter().any(|group| group == undeclared),
            "unexpected {undeclared}"
        );
    }
    capture("role.describe.json", &frame.body);
    rig.finish().await;
}

fn tool_part(value: &serde_json::Value) -> Option<&serde_json::Value> {
    if value.get("tool_call_id").is_some()
        && value
            .get("arguments")
            .is_some_and(serde_json::Value::is_string)
    {
        return Some(value);
    }
    match value {
        serde_json::Value::Object(fields) => fields.values().find_map(tool_part),
        serde_json::Value::Array(values) => values.iter().find_map(tool_part),
        _ => None,
    }
}

#[tokio::test]
async fn known_roles_are_served_on_every_read_message_envelope() {
    use std::time::Duration;
    let mut rig = Rig::start("known-read-roles").await;
    rig.script
        .push(&[Step::Call("role-call"), Step::Text("synthetic role answer")]);
    let sent = rig.send("role-send", "synthetic role prompt", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_idle().await;

    let mut pages = Vec::new();
    for (name, method, params) in [
        ("read-default", "session.read", json!({})),
        ("read-head", "session.read", json!({"limit":4})),
        (
            "read-range",
            "session.read",
            json!({"from_ordinal":1,"limit":4}),
        ),
        (
            "read-facts",
            "session.read",
            json!({"from_ordinal":1,"limit":4,"include_display_facts":true}),
        ),
        (
            "read-descriptors",
            "session.read",
            json!({"from_ordinal":1,"limit":4,"media":"descriptors"}),
        ),
        ("history-raw", "session.history", json!({"limit":4})),
        (
            "history-display",
            "session.history",
            json!({"form":"display","limit":4}),
        ),
    ] {
        let reply = rig
            .request(7, json!({"method":method,"params":params}))
            .await;
        assert_eq!(reply.header.ty, subc_protocol::FrameType::Response);
        capture(&format!("read-role.{name}.json"), &reply.body);
        pages.push((name, result(&reply)));
    }
    let mut paged = Vec::new();
    for ordinal in 1..=4 {
        let reply = rig
            .request(
                7,
                json!({"method":"session.read","params":{"from_ordinal":ordinal,"limit":1}}),
            )
            .await;
        assert_eq!(reply.header.ty, subc_protocol::FrameType::Response);
        capture(&format!("read-role.read-page-{ordinal}.json"), &reply.body);
        let page = result(&reply);
        assert_eq!(page["messages"].as_array().unwrap().len(), 1);
        assert_eq!(page["messages"][0]["ordinal"], ordinal);
        paged.push(page["messages"][0].clone());
    }
    for (method, params, name) in [
        ("session.head", json!({}), "head-control"),
        ("run.result", json!({"run_id":run_id}), "run-result-control"),
    ] {
        let reply = rig
            .request(7, json!({"method":method,"params":params}))
            .await;
        assert_eq!(reply.header.ty, subc_protocol::FrameType::Response);
        capture(&format!("read-role.{name}.json"), &reply.body);
    }
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            Duration::from_millis(30),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "tool_finished")
            },
        )
        .await;
    for (index, frame) in frames.iter().enumerate() {
        capture(
            &format!("read-role.subscribe-control-{index}.json"),
            &frame.body,
        );
    }
    rig.finish().await;

    for (name, page) in &pages {
        let entries = page["messages"].as_array().unwrap();
        assert_eq!(entries.len(), 4, "{name}: {page}");
        for (entry, expected) in entries
            .iter()
            .zip(["user", "assistant", "tool", "assistant"])
        {
            let envelope = if *name == "history-display" {
                &entry["row"]
            } else {
                entry
            };
            assert_eq!(envelope["role"], expected, "{name}: {entry}");
        }
        if *name != "history-display" {
            assert_eq!(entries[1]["message"]["content"][0]["type"], "tool_call");
            assert_eq!(entries[2]["message"]["content"][0]["type"], "tool_result");
        }
    }
    for (entry, expected) in paged.iter().zip(["user", "assistant", "tool", "assistant"]) {
        assert_eq!(entry["role"], expected);
    }
}

#[tokio::test]
async fn tool_identity_contract_samples() {
    use std::time::Duration;
    let mut rig = Rig::start("contract-tool-and-bus-samples").await;
    let digest =
        cortexkit_role_tool_provider::catalog::schema_digest(&json!({"type":"object"})).unwrap();
    *rig.script.schema_pin.lock().unwrap() = Some(broca_records::record::DispatchSchemaPin {
        schema_digest: digest.clone(),
        semantics: 7,
        major: None,
    });
    rig.script
        .push(&[Step::Call("sample-call"), Step::Text("synthetic answer")]);
    let sent = rig.send("sample-send", "synthetic prompt", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_idle().await;
    // The key the call's dispatch intent recorded.
    let expected_key = rig
        .records()
        .await
        .iter()
        .find_map(|record| match record {
            Record::ToolIntent(intent) if intent.tool_call_id.0 == "sample-call" => {
                intent.call_key.clone()
            }
            _ => None,
        })
        .expect("the call's intent recorded its key");
    let expected_pin = format!("tp1:work:{digest}:7");
    let page = rig
        .request(
            7,
            json!({"method":"session.history","params":{"form":"display","limit":50}}),
        )
        .await;
    assert_eq!(page.header.ty, subc_protocol::FrameType::Response);
    let body = result(&page);
    let call = tool_part(&body).expect("display page has a tool call");
    assert_eq!(call["call_key"], expected_key);
    assert_eq!(call["schema_pin"], expected_pin);
    capture("session.history.json", &page.body);
    // Replay from the start can outlast any fixed quiet window on a loaded runner, so wait for
    // the replayed `tool_finished`, which follows `tool_started`, before the quiet window starts.
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            Duration::from_millis(30),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "tool_finished")
            },
        )
        .await;
    for kind in ["tool_started", "tool_finished"] {
        let frame = frames
            .iter()
            .find(|frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == kind)
            })
            .expect("tool lifecycle frame");
        let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(value["event"]["call_key"], expected_key);
        assert_eq!(value["event"]["schema_pin"], expected_pin);
        capture(&format!("{kind}.frame.json"), &frame.body);
        let raw: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_slice(&frame.body).unwrap();
        capture(&format!("{kind}.json"), raw["event"].get().as_bytes());
    }
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    assert_eq!(result(&head)["last_run_state"]["state"], "completed");
    assert!(result(&head).get("last_run_error").is_none());
    capture("session.head.completed.json", &head.body);
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    assert!(!String::from_utf8_lossy(&read.body).contains("schema_pin"));
    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run_id}}))
        .await;
    assert!(!String::from_utf8_lossy(&status.body).contains("schema_pin"));

    rig.finish().await;
}

/// A call its provider refuses because it now serves another major of the tool: the call's
/// lifecycle carries its `tp2` pin, and a `tool_changed` frame, on a `message_boundary` unit of
/// the call's message, names the provider and both pins (llm-runner 0.9 `tool_versions`).
#[tokio::test]
async fn tool_changed_contract_samples() {
    use std::time::Duration;
    let mut rig = Rig::start("contract-tool-changed-samples").await;
    let digest =
        cortexkit_role_tool_provider::catalog::schema_digest(&json!({"type":"object"})).unwrap();
    *rig.script.schema_pin.lock().unwrap() = Some(broca_records::record::DispatchSchemaPin {
        schema_digest: digest.clone(),
        semantics: 1,
        major: Some(2),
    });
    let expected = format!("tp2:work:{digest}:2");
    let current = format!("tp2:work:{}:3", "b".repeat(64));
    let refusal = json!({"tool": "work", "expected": expected, "current": current});
    rig.script.tool_results.lock().unwrap().insert(
        "sample-call".into(),
        broca_core::tool_plane::DispatchResult::Refused {
            result: broca_protocol::content::ToolResult {
                tool_call_id: broca_protocol::ToolCallId("sample-call".into()),
                output: broca_protocol::content::ToolOutput::error(
                    "tool/route rejected: tool_major_changed — this tool changed incompatibly",
                ),
                is_error: true,
            },
            code: "tool_major_changed".into(),
            detail: broca_core::tool_plane::refusal_detail(
                delivery_rig::WORK_MODULE,
                Some(&refusal),
            ),
        },
    );
    rig.script
        .push(&[Step::Call("sample-call"), Step::Text("synthetic answer")]);
    rig.send("sample-send", "synthetic prompt", None).await;
    rig.wait_idle().await;
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            Duration::from_millis(30),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    let event = |kind: &str| {
        frames
            .iter()
            .find(|frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == kind)
            })
            .unwrap_or_else(|| panic!("a {kind} frame"))
    };
    let changed = event("tool_changed");
    let value: serde_json::Value = serde_json::from_slice(&changed.body).unwrap();
    assert_eq!(
        value["event"],
        json!({"type": "tool_changed", "provider": delivery_rig::WORK_MODULE, "tool": "work",
               "expected": expected, "current": current})
    );
    assert_eq!(value["unit"]["type"], "message_boundary");
    capture("tool_changed.frame.json", &changed.body);
    let finished = event("tool_finished");
    let value: serde_json::Value = serde_json::from_slice(&finished.body).unwrap();
    assert_eq!(value["event"]["schema_pin"], expected);
    capture("tool_finished.tp2.frame.json", &finished.body);
    rig.finish().await;
}

async fn subscribe_refusal(rig: &mut Rig, from: serde_json::Value, code: &str) {
    let frames = rig
        .subscribe_collect(
            8,
            json!({"from":from}),
            std::time::Duration::from_millis(200),
        )
        .await;
    assert_eq!(
        frames.len(),
        1,
        "refusal must precede every event: {frames:?}"
    );
    assert_eq!(
        frames[0].header.ty,
        subc_protocol::FrameType::Error,
        "{frames:?}"
    );
    let error: serde_json::Value = serde_json::from_slice(&frames[0].body).unwrap();
    assert_eq!(error["code"], code, "{error}");
    if code == "invalid_params" {
        assert_eq!(error["detail"]["field"], "from");
    }
}

#[tokio::test]
async fn subscribe_resume_refuses_malformed_foreign_future_and_legacy_cursors() {
    let mut rig = Rig::start("subscribe-refusals-0811").await;
    rig.script.push(&[Step::Text("first")]);
    rig.send("first", "hello", None).await;
    rig.wait_idle().await;
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    let cursor = result(&head)["head"].clone();
    assert!(cursor["incarnation"].is_string());
    assert_eq!(cursor["digest"].as_str().unwrap().len(), 16);
    for malformed in [
        json!(3),
        json!([]),
        json!("unknown"),
        json!({"wal_seq":1}),
        json!({"wal_seq":1,"sub_index":0,"digest":"broken"}),
        json!({"wal_seq":1,"sub_index":0,"incarnation":"known","digest":"0000000000000000","extra":1}),
    ] {
        subscribe_refusal(&mut rig, malformed, "invalid_params").await;
    }
    subscribe_refusal(
        &mut rig,
        json!({"wal_seq":1,"sub_index":0}),
        "cursor_expired",
    )
    .await;
    let mut future = cursor.clone();
    future["wal_seq"] = json!(u64::MAX);
    subscribe_refusal(&mut rig, future, "cursor_expired").await;
    let mut sub_index = cursor.clone();
    sub_index["sub_index"] = json!(u32::MAX);
    subscribe_refusal(&mut rig, sub_index, "cursor_expired").await;
    let mut other = Rig::start("subscribe-other-0811").await;
    other.script.push(&[Step::Text("other")]);
    other.send("other", "hello", None).await;
    other.wait_idle().await;
    subscribe_refusal(&mut other, cursor.clone(), "cursor_expired").await;
    other.finish().await;
    assert!(rig.registry().stop_and_seal().await);
    rig = rig.restart().await;
    rig.script.push(&[Step::Text("next")]);
    rig.send("next", "next", None).await;
    rig.wait_idle().await;
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":cursor}),
            std::time::Duration::from_millis(20),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    assert!(frames
        .iter()
        .all(|frame| frame.header.ty != subc_protocol::FrameType::Error));
    for from in [serde_json::Value::Null, json!("live")] {
        let frames = rig
            .subscribe_collect_until(
                8,
                json!({"from":from}),
                std::time::Duration::from_millis(20),
                |_| true,
            )
            .await;
        assert!(frames
            .iter()
            .all(|frame| frame.header.ty != subc_protocol::FrameType::Error));
    }
    rig.finish().await;
}

#[tokio::test]
async fn envelope_ride_creates_no_transcript_entry() {
    let mut rig = Rig::start("notice-no-transcript-entry").await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("notice-carrier"), Step::Text("answer")]);
    rig.send("start", "synthetic prompt", None).await;
    rig.wait_entered().await;
    let before = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    let text = "a notice must not become a transcript entry";
    rig.send_with("notice", text, None, json!({"delivery":["tool_result"]}))
        .await;
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    let receipt = result(
        &rig.send_with("notice", text, None, json!({"delivery":["tool_result"]}))
            .await,
    );
    let mut coordinates = None;
    for restart in [false, true] {
        if restart {
            rig = rig.restart().await;
        }
        for facts in [false, true] {
            let page = result(&rig.request(7, json!({"method":"session.read","params":{"from_ordinal":0,"limit":100,"include_display_facts":facts}})).await);
            assert_eq!(page["history_version"], before["history_version"]);
            let messages = page["messages"].as_array().unwrap();
            assert_eq!(messages.len(), 4, "only user, call, result and answer");
            let current: Vec<_> = messages
                .iter()
                .map(|entry| (entry["ordinal"].clone(), entry["mid"].clone()))
                .collect();
            assert_eq!(
                current
                    .iter()
                    .map(|item| item.0.clone())
                    .collect::<Vec<_>>(),
                vec![json!(1), json!(2), json!(3), json!(4)],
            );
            if let Some(expected) = &coordinates {
                assert_eq!(&current, expected, "restart preserves ordinals and mids");
            } else {
                coordinates = Some(current);
            }
            let carrier = messages
                .iter()
                .find(|entry| entry["message"]["role"] == "tool")
                .unwrap();
            let call = messages
                .iter()
                .find(|entry| {
                    entry["message"]["role"] == "assistant" && entry.get("tool_calls").is_some()
                })
                .unwrap();
            assert_eq!(
                receipt["delivered"]["call_key"],
                call["tool_calls"][0]["call_key"]
            );
            assert!(receipt["delivered"]["call_key"].is_string());
            for entry in messages {
                assert_ne!(
                    entry["delivered_as"]["mode"], "tool_result",
                    "a ride created a transcript entry: {entry}"
                );
                if entry["mid"] != carrier["mid"] {
                    assert!(
                        !entry.to_string().contains(text),
                        "notice leaked outside carrier"
                    );
                }
            }
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn notices_and_sender_contract_samples() {
    let mut rig = Rig::start("notices-and-sender-0811").await;
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "sample-sender".into(),
        }),
    )
    .await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("sample-carrying-call"), Step::Text("answer")]);
    rig.send("start", "synthetic prompt", None).await;
    rig.wait_entered().await;
    let before = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    let text = "  synthetic notice\n☃\n";
    rig.send_with(
        "sample-notice",
        text,
        None,
        json!({"delivery":["tool_result"]}),
    )
    .await;
    rig.send_with(
        "sample-notice-2",
        "second notice",
        None,
        json!({"delivery":["tool_result"]}),
    )
    .await;
    let unkeyed = result(
        &rig.send_with(
            "unused",
            "unkeyed notice",
            None,
            json!({"send_id":null,"delivery":["tool_result"]}),
        )
        .await,
    );
    let unkeyed_id = unkeyed["submission_id"]
        .as_str()
        .expect("riding send admission returns submission_id")
        .to_owned();
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    // The first notice rode the held call's result: its delivery names that call.
    let records = rig.records().await;
    let delivered = records
        .iter()
        .find_map(|record| match record {
            Record::Delivery(delivery) => Some(delivery),
            _ => None,
        })
        .expect("notice delivery record");
    assert_eq!(delivered.mode, "tool_result");
    assert_eq!(
        delivered.tool_call_id.as_ref().map(|id| id.0.as_str()),
        Some("sample-carrying-call")
    );
    let key = records
        .iter()
        .find_map(|record| match record {
            Record::ToolIntent(intent) if intent.tool_call_id.0 == "sample-carrying-call" => {
                intent.call_key.clone()
            }
            _ => None,
        })
        .expect("the carrying call's intent recorded its key");
    let default = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    capture("0811.session.read.default.json", &default.body);
    assert!(result(&default)["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry.get("sender").is_none() && entry.get("notices").is_none()));
    let facts = rig
        .request(
            7,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await;
    capture("0811.session.read.facts.json", &facts.body);
    let body = result(&facts);
    let user = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["message"]["role"] == "user")
        .unwrap();
    assert_eq!(user["sender"], "sample-sender");
    let carrying = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["message"]["role"] == "tool")
        .unwrap();
    let expected = json!({"part":0,"call_key":key,"send_id":"sample-notice","sender":"sample-sender","text":text,"delivered_as":"tool_result"});
    let expected_second = json!({"part":0,"call_key":key,"send_id":"sample-notice-2","sender":"sample-sender","text":"second notice","delivered_as":"tool_result"});
    let expected_unkeyed = json!({"part":0,"call_key":key,"send_id":unkeyed_id,"sender":"sample-sender","text":"unkeyed notice","delivered_as":"tool_result"});
    assert_eq!(
        carrying["notices"],
        json!([expected, expected_second, expected_unkeyed])
    );
    assert!(carrying.get("delivered_as").is_none());
    assert_eq!(body["history_version"], before["history_version"]);
    for form in ["raw", "display"] {
        let page = rig
            .request(
                7,
                json!({"method":"session.history","params":{"form":form,"limit":100}}),
            )
            .await;
        capture(&format!("0811.session.history.{form}.json"), &page.body);
        let body = result(&page);
        assert!(body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry.get("delivered_as").is_none()));
        let first = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["mid"] == user["mid"])
            .unwrap();
        if form == "raw" {
            assert_eq!(first["sender"], "sample-sender");
        } else {
            let row = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["mid"] == carrying["mid"])
                .unwrap();
            assert_eq!(
                row["row"]["notices"],
                json!([expected, expected_second, expected_unkeyed])
            );
        }
    }
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            std::time::Duration::from_millis(20),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    capture(
        "0811.subscribe.envelope.json",
        &serde_json::to_vec(
            &frames
                .iter()
                .map(|frame| serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    );
    let notices: Vec<_> = frames
        .iter()
        .filter(|frame| {
            serde_json::from_slice::<serde_json::Value>(&frame.body)
                .is_ok_and(|value| value["event"]["type"] == "notice_delivered")
        })
        .collect();
    assert_eq!(notices.len(), 3);
    capture("0811.notice_delivered.frame.json", &notices[0].body);
    let unkeyed_event: serde_json::Value = serde_json::from_slice(&notices[2].body).unwrap();
    assert_eq!(unkeyed_event["event"]["send_id"], unkeyed_id);
    assert_eq!(unkeyed_event["event"]["text"], "unkeyed notice");
    let event: serde_json::Value = serde_json::from_slice(&notices[0].body).unwrap();
    assert_eq!(
        event["event"],
        json!({"type":"notice_delivered","mid":carrying["mid"],"part":0,"call_key":key,"send_id":"sample-notice","sender":"sample-sender","text":text})
    );
    let resumed = rig
        .subscribe_collect_until(
            8,
            json!({"from":event["cursor"]}),
            std::time::Duration::from_millis(20),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    assert!(resumed
        .iter()
        .all(|frame| frame.header.ty != subc_protocol::FrameType::Error));
    let native = records
        .iter()
        .find_map(|record| match record {
            Record::ToolReply(reply) if reply.result.tool_call_id.0 == "sample-carrying-call" => {
                Some(&reply.result)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        native.output,
        broca_protocol::ToolOutput::Text {
            text: "worked".into()
        }
    );
    let receipt = result(
        &rig.send_with(
            "sample-notice",
            text,
            None,
            json!({"delivery":["tool_result"]}),
        )
        .await,
    );
    assert_eq!(receipt["delivered"]["call_key"], key);
    let status = rig
        .request(
            7,
            json!({"method":"send.status","params":{"send_id":unkeyed_id}}),
        )
        .await;
    assert_eq!(status.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status.body).unwrap()["code"],
        "unknown_send"
    );
    rig.finish().await;
}

#[tokio::test]
async fn upgrade_wire_samples() {
    let mut rig = Rig::start("upgrade-wire-samples").await;
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "sample-sender".into(),
        }),
    )
    .await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("sample-carrying-call"), Step::Text("answer")]);
    rig.send("start", "synthetic prompt", None).await;
    rig.wait_entered().await;
    rig.send_with(
        "sample-notice",
        "synthetic notice ☃",
        None,
        json!({"delivery":["tool_result"]}),
    )
    .await;
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    for (name, method, params) in [
        ("role.describe.json", "role.describe", json!({})),
        ("session.head.json", "session.head", json!({})),
        ("session.read.default.json", "session.read", json!({})),
        (
            "session.read.facts.json",
            "session.read",
            json!({"include_display_facts":true}),
        ),
        (
            "session.history.raw.json",
            "session.history",
            json!({"form":"raw","limit":100}),
        ),
        (
            "session.history.display.json",
            "session.history",
            json!({"form":"display","limit":100}),
        ),
    ] {
        let frame = rig
            .request(7, json!({"method":method,"params":params}))
            .await;
        assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
        capture(&format!("upgrade.{name}"), &frame.body);
    }
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            std::time::Duration::from_millis(20),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    for (index, frame) in frames.iter().enumerate() {
        capture(&format!("upgrade.subscribe.{index}.json"), &frame.body);
    }
    assert!(!frames.is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn denied_call_wire_samples() {
    let mut rig = Rig::start("denied-call-samples").await;
    *rig.script.deny_input.lock().unwrap() = Some(json!({}));
    rig.script.push(&[
        Step::Calls(&["denied-first", "denied-second"]),
        Step::Text("after denials"),
    ]);
    rig.send("denied-start", "synthetic denied calls", None)
        .await;
    rig.wait_idle().await;
    // Each denied call's key, as its never-dispatched outcome recorded it.
    let expected: std::collections::BTreeMap<_, _> = rig
        .records()
        .await
        .iter()
        .filter_map(|record| match record {
            Record::ToolOutcome(outcome)
                if outcome.outcome == broca_session_store::record::CallOutcome::NotDispatched =>
            {
                Some((outcome.tool_call_id.0.clone(), outcome.call_key.clone()?))
            }
            _ => None,
        })
        .collect();
    assert_eq!(expected.len(), 2);
    for (name, params) in [
        ("default", json!({})),
        ("facts", json!({"include_display_facts":true})),
    ] {
        let read = rig
            .request(7, json!({"method":"session.read","params":params}))
            .await;
        assert_eq!(read.header.ty, subc_protocol::FrameType::Response);
        capture(&format!("denied.session.read.{name}.json"), &read.body);
        let read = result(&read);
        let calls: Vec<_> = read["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|entry| entry["tool_calls"].as_array().into_iter().flatten())
            .collect();
        assert_eq!(calls.len(), 2);
        for call in calls {
            assert_eq!(
                call["call_key"].as_str(),
                expected
                    .get(call["tool_call_id"].as_str().unwrap())
                    .map(String::as_str)
            );
        }
    }
    let frames = rig
        .subscribe_collect_until(
            8,
            json!({"from":"start"}),
            std::time::Duration::from_millis(20),
            |frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == "run_finished")
            },
        )
        .await;
    for (index, frame) in frames
        .iter()
        .filter(|frame| {
            serde_json::from_slice::<serde_json::Value>(&frame.body).is_ok_and(|value| {
                matches!(
                    value["event"]["type"].as_str(),
                    Some("tool_started" | "tool_finished")
                )
            })
        })
        .enumerate()
    {
        capture(&format!("denied.live.{index}.json"), &frame.body);
        let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(
            value["event"]["call_key"].as_str(),
            expected
                .get(value["event"]["tool_call_id"].as_str().unwrap())
                .map(String::as_str)
        );
    }
    assert_eq!(
        rig.records()
            .await
            .iter()
            .filter(|record| matches!(
                record,
                Record::ToolOutcome(outcome)
                    if outcome.outcome == broca_session_store::record::CallOutcome::NotDispatched
            ))
            .count(),
        2
    );
    assert!(rig.registry().stop_and_seal().await);
    rig = rig.restart().await;
    let read = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    for call in read["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["tool_calls"].as_array().into_iter().flatten())
    {
        assert_eq!(
            call["call_key"].as_str(),
            expected
                .get(call["tool_call_id"].as_str().unwrap())
                .map(String::as_str)
        );
    }
    rig.finish().await;
}

#[tokio::test]
async fn undeclared_sender_attribution_refuses_send_and_refresh_before_writes() {
    let mut rig = Rig::start("undeclared-attribution").await;
    rig.script.push(&[Step::Text("answer")]);
    rig.send("opening", "synthetic prompt", None).await;
    rig.wait_idle().await;
    let before = rig.records().await;
    for method in ["session.send", "session.refresh"] {
        let plan = json!({"senders":[{"module":"sample-sender","attribution":"header"}]});
        let frame = if method == "session.send" {
            rig.send_with("attributed", "new prompt", None, json!({"plan":plan}))
                .await
        } else {
            rig.request(7, json!({"method":method,"params":{"policy":"immediately","generation":1,"plan":plan}})).await
        };
        assert_eq!(frame.header.ty, subc_protocol::FrameType::Error);
        let error: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], "plan.senders[0].attribution");
        assert_eq!(rig.records().await, before);
    }
    rig.finish().await;
}

#[tokio::test]
async fn core_reader_wire_samples() {
    let mut rig = Rig::start("core-reader-wire-samples").await;
    rig.script
        .push(&[Step::Text("first answer"), Step::Text("second answer")]);
    rig.script.hold_model(true);
    let accepted = rig.send("core-first", "first prompt", None).await;
    let run = result(&accepted)["run_id"].as_str().unwrap().to_owned();
    capture("core.session.send.accepted.json", &accepted.body);
    rig.wait_entered().await;
    let pending = rig.send("core-pending", "queued prompt", None).await;
    assert!(result(&pending)["submission_id"].is_string());
    capture("core.session.send.pending.json", &pending.body);
    let refused = rig.send("core-pending", "different prompt", None).await;
    assert_eq!(refused.header.ty, subc_protocol::FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&refused.body).unwrap()["code"],
        "send_id_reuse"
    );
    capture("core.session.send.refused.json", &refused.body);
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    for method in ["run.status", "run.result"] {
        let frame = rig
            .request(7, json!({"method":method,"params":{"run_id":run}}))
            .await;
        assert_eq!(result(&frame)["state"], "completed");
        capture(&format!("core.{method}.json"), &frame.body);
    }
    let mut next = Some(0);
    let mut pages = 0;
    let mut ordinals = Vec::new();
    while let Some(from) = next {
        let frame = rig
            .request(
                7,
                json!({"method":"session.read","params":{"from_ordinal":from,"limit":2}}),
            )
            .await;
        capture(&format!("core.session.read.page.{pages}.json"), &frame.body);
        let body = result(&frame);
        ordinals.extend(
            body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["ordinal"].as_u64().unwrap()),
        );
        next = body["next_from_ordinal"].as_u64();
        assert!(next.is_none_or(|next| next > from));
        pages += 1;
    }
    assert_eq!(ordinals, [1, 2, 3, 4]);
    assert_eq!(pages, 2);
    for (name, from, code) in [
        (
            "cursor_expired",
            json!({"wal_seq":1,"sub_index":0}),
            "cursor_expired",
        ),
        ("invalid_params", json!({"wal_seq":1}), "invalid_params"),
    ] {
        let frames = rig
            .subscribe_collect(
                8,
                json!({"from":from}),
                std::time::Duration::from_millis(100),
            )
            .await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header.ty, subc_protocol::FrameType::Error);
        let body: serde_json::Value = serde_json::from_slice(&frames[0].body).unwrap();
        assert_eq!(body["code"], code);
        if code == "invalid_params" {
            assert_eq!(body["detail"]["field"], "from");
        }
        capture(
            &format!("core.session.subscribe.{name}.error.json"),
            &frames[0].body,
        );
    }
    rig.finish().await;
}
