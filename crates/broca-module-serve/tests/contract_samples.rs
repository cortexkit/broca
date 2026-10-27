//! Wire samples exercised through the same serve loop as session callers.
mod delivery_rig;

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
async fn tool_identity_and_bus_contract_samples() {
    use broca_core::{FileWalStore, WalStore};
    use std::time::Duration;
    let mut rig = Rig::start("contract-tool-and-bus-samples").await;
    let digest =
        cortexkit_role_tool_provider::catalog::schema_digest(&json!({"type":"object"})).unwrap();
    *rig.script.schema_pin.lock().unwrap() = Some(broca_wal::DispatchSchemaPin {
        schema_digest: digest.clone(),
        semantics: 7,
    });
    rig.script
        .push(&[Step::Call("sample-call"), Step::Text("synthetic answer")]);
    let sent = rig.send("sample-send", "synthetic prompt", None).await;
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_idle().await;
    let wal = FileWalStore::new(rig.dir.join("wal"));
    let range = wal.read_records_range(&rig.session, 0, None, None).unwrap();
    let lineage = range.lineage_id.unwrap();
    let records = wal.read_records(&rig.session).unwrap();
    let seq = records.iter().find(|frame| matches!(&frame.record, broca_wal::WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == "sample-call")).unwrap().seq;
    let expected_key = format!("{lineage}:{seq}");
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
    let frames = rig
        .subscribe_collect(8, json!({"from":"start"}), Duration::from_millis(30))
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

    rig.store.enable_bus_events("synthetic_account").unwrap();
    let now = records
        .iter()
        .filter_map(|frame| broca_core::record_time_ms(&frame.record))
        .max()
        .unwrap();
    rig.store
        .project_bus_events(&rig.session, "synthetic-epoch", &records, now)
        .unwrap();
    let event = rig.store.pending_bus_event().unwrap().unwrap();
    let event_reply = rig
        .request(
            7,
            json!({"method":"run.event.get","params":{"event_key":event.event_key}}),
        )
        .await;
    assert_eq!(event_reply.header.ty, subc_protocol::FrameType::Response);
    capture("run.event.get.json", &event_reply.body);
    let event_jcs = result(&event_reply)["jcs"].as_str().unwrap().to_owned();
    capture("run.event.json", event_jcs.as_bytes());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event_jcs).unwrap()["session_ref"],
        broca_core::agent_session_ref(&rig.session)
    );
    rig.store
        .skip_stale_bus_events(now + 25 * 60 * 60 * 1000)
        .unwrap();
    let skipped = rig.store.pending_bus_event().unwrap().unwrap();
    let skipped_reply = rig
        .request(
            7,
            json!({"method":"run.event.get","params":{"event_key":skipped.event_key}}),
        )
        .await;
    assert_eq!(skipped_reply.header.ty, subc_protocol::FrameType::Response);
    capture("run.skipped.get.json", &skipped_reply.body);
    let skipped_jcs = result(&skipped_reply)["jcs"].as_str().unwrap().to_owned();
    capture("run.skipped.json", skipped_jcs.as_bytes());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&skipped_jcs).unwrap()["session_ref"],
        broca_core::agent_session_ref(&rig.session)
    );
    rig.finish().await;
}

#[tokio::test]
async fn legacy_direct_call_has_key_without_a_guessed_schema_pin() {
    use std::time::Duration;
    let mut rig = Rig::start("contract-legacy-call").await;
    rig.script
        .push(&[Step::Call("legacy-call"), Step::Text("done")]);
    rig.send("legacy-send", "synthetic prompt", None).await;
    rig.wait_idle().await;
    let page = rig
        .request(
            7,
            json!({"method":"session.history","params":{"form":"display"}}),
        )
        .await;
    let body = result(&page);
    let call = tool_part(&body).unwrap();
    assert!(call["call_key"]
        .as_str()
        .is_some_and(|key| key.contains(':')));
    assert!(call.get("schema_pin").is_none());
    let frames = rig
        .subscribe_collect(8, json!({"from":"start"}), Duration::from_millis(30))
        .await;
    let mut observed = 0;
    for frame in frames {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&frame.body) {
            if matches!(
                value["event"]["type"].as_str(),
                Some("tool_started" | "tool_finished")
            ) {
                observed += 1;
                assert!(value["event"]["call_key"].is_string());
                assert!(value["event"].get("schema_pin").is_none());
            }
        }
    }
    assert_eq!(observed, 2, "both tool lifecycle events must be observed");
    rig.finish().await;
}

fn seed_terminal(rig: &Rig, state: &str, retry: Option<f64>) {
    use broca_core::{FileWalStore, WalStore};
    let mut start: broca_wal::WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let broca_wal::WalRecord::RunStarted {
        run_id,
        session,
        ts_ms,
        ..
    } = &mut start
    {
        *run_id = broca_protocol::RunId("sample-error-run".into());
        *session = rig.session.clone();
        *ts_ms = Some(500);
    }
    let mut error = json!({"class":"transient","message":"synthetic provider failure","status":429,"provider_code":"rate_limited","resets_at_ms":1790000003000_u64});
    if let Some(retry) = retry {
        error["retry_after_secs"] = json!(retry);
    }
    let terminal = if state == "paused" {
        json!({"type":"run_paused","run_id":"sample-error-run","reason":"auth_required","ts_ms":506,"usage":{"input_tokens":0,"output_tokens":0},"retries_used":0,"error":error})
    } else {
        let mut record = json!({"type":"run_finished","reason":state,"ts_ms":506,"usage":{"input_tokens":0,"output_tokens":0},"retries_used":0});
        if state == "error" {
            record["error"] = error;
        }
        if state == "interrupted" {
            record["interrupt_reason"] = json!("runner_shutdown");
        }
        record
    };
    let terminal: broca_wal::WalRecord = serde_json::from_value(terminal).unwrap();
    let store = FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&rig.session).unwrap();
    wal.append(1, &start, false).unwrap();
    wal.append(1, &terminal, true).unwrap();
}

#[tokio::test]
async fn error_head_matches_named_run_status_and_omits_missing_delay() {
    for (name, delay) in [("fractional", Some(1.5)), ("missing-delay", None)] {
        let mut rig = Rig::start(&format!("contract-error-head-{name}")).await;
        seed_terminal(&rig, "error", delay);
        let head = rig
            .request(7, json!({"method":"session.head","params":{}}))
            .await;
        assert_eq!(head.header.ty, subc_protocol::FrameType::Response);
        let value = result(&head);
        assert_eq!(value["last_run_state"]["state"], "error");
        let summary = &value["last_run_error"];
        assert_eq!(summary["class"], "transient");
        assert_eq!(summary["resets_at_ms"], 1790000003000_u64);
        assert!(summary.get("message").is_none());
        if let Some(delay) = delay {
            assert_eq!(summary["retry_after_secs"], delay);
        } else {
            assert!(summary.get("retry_after_secs").is_none());
        }
        let status = rig
            .request(
                7,
                json!({"method":"run.status","params":{"run_id":"sample-error-run"}}),
            )
            .await;
        let status = result(&status);
        for member in ["class", "retry_after_secs", "resets_at_ms"] {
            assert_eq!(summary.get(member), status["error"].get(member));
        }
        capture(&format!("session.head.error.{name}.json"), &head.body);
        rig.finish().await;
    }
}

#[tokio::test]
async fn paused_and_interrupted_heads_never_carry_last_run_error() {
    for state in ["paused", "interrupted"] {
        let mut rig = Rig::start(&format!("contract-no-error-{state}")).await;
        seed_terminal(&rig, state, Some(1.5));
        let head = rig
            .request(7, json!({"method":"session.head","params":{}}))
            .await;
        assert_eq!(head.header.ty, subc_protocol::FrameType::Response);
        assert_eq!(result(&head)["last_run_state"]["state"], state);
        assert!(result(&head).get("last_run_error").is_none());
        rig.finish().await;
    }
}

fn seed_approved_call(rig: &Rig) -> (String, String) {
    use broca_core::{FileWalStore, WalStore};
    let run_id = "synthetic-approved-run".to_owned();
    let scope = json!({"owner":"reserved:sample-owner","ref":"approval-sample","scope_epoch":1});
    let mut start: serde_json::Value = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    start["run_id"] = json!(run_id);
    start["session"] = serde_json::to_value(&rig.session).unwrap();
    start["scope"] = scope.clone();
    start["principal"] = serde_json::to_value(broca_protocol::BindPrincipal::Reserved {
        module_id: "sample-owner".into(),
    })
    .unwrap();
    start["ts_ms"] = json!(500);
    start["written_at_ms"] = json!(500);
    start["input"] = serde_json::to_value(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "synthetic approval prompt".into(),
        }],
    )])
    .unwrap();
    let store = FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&rig.session).unwrap();
    let mut append = |value: serde_json::Value| {
        let record: broca_wal::WalRecord = serde_json::from_value(value).unwrap();
        wal.append(1, &record, true).unwrap()
    };
    append(start);
    append(json!({"type":"step_started","step_id":1}));
    append(
        json!({"type":"model_step_finished","step_id":1,"written_at_ms":501,
        "assistant_message":{"message_id":"approved-call-message","content":[{"type":"tool_call","tool_call_id":"approved-call","tool_name":"work","input":{}}]},
        "usage":{"input_tokens":0,"output_tokens":0},"finish_reason":"tool_calls","retries_used":0}),
    );
    append(
        json!({"type":"tool_batch_started","batch_id":"approved-batch","step_id":1,"planned_calls":["approved-call"]}),
    );
    let prepared_seq = append(
        json!({"type":"tool_call_deferred","deferred_at_ms":502,"batch_id":"approved-batch","call_id":"approved-call","executor":"broca","module":"synthetic-tools","tool":"work",
        "args_jcs":"{}","args_hash":"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a","scope":scope,
        "ask":{"prompt":"Allow the synthetic tool?","expires_at_ms":1799999999999_u64,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"}}),
    );
    let key = format!("{}:{prepared_seq}", wal.lineage_id().unwrap());
    let mut append = |value: serde_json::Value| {
        let record: broca_wal::WalRecord = serde_json::from_value(value).unwrap();
        wal.append(1, &record, true).unwrap()
    };
    let digest = "1".repeat(64);
    append(
        json!({"type":"approval_recorded","closed_at_ms":503,"call_key":key,"call_id":"approved-call","elicitation_id":"synthetic-approval","decision":"approved","approval_digest":digest}),
    );
    let schema_digest =
        cortexkit_role_tool_provider::catalog::schema_digest(&json!({"type":"object"})).unwrap();
    let intent_seq = append(
        json!({"type":"tool_dispatch_intent","dispatched_at_ms":504,"batch_id":"approved-batch","tool_call_id":"approved-call","tool_name":"work","args":{},"module_id":"synthetic-tools",
        "schema_pin":{"schema_digest":schema_digest,"semantics":7},"approval_digest":digest,"call_key":key}),
    );
    assert_ne!(prepared_seq, intent_seq);
    append(
        json!({"type":"tool_result","finished_at_ms":505,"batch_id":"approved-batch","result":{"tool_call_id":"approved-call","output":{"kind":"text","text":"synthetic result"},"is_error":false},"outcome":"completed"}),
    );
    append(json!({"type":"tool_batch_finished","batch_id":"approved-batch"}));
    append(json!({"type":"turn_finished","step_id":1}));
    append(json!({"type":"step_started","step_id":2}));
    append(
        json!({"type":"model_step_finished","step_id":2,"written_at_ms":506,
        "assistant_message":{"message_id":"approved-answer","content":[{"type":"text","text":"synthetic approved answer"}]},
        "usage":{"input_tokens":0,"output_tokens":0},"finish_reason":"stop","retries_used":0}),
    );
    append(json!({"type":"turn_finished","step_id":2}));
    append(
        json!({"type":"run_finished","reason":"completed","ts_ms":507,"usage":{"input_tokens":0,"output_tokens":0},"retries_used":0}),
    );
    (key, run_id)
}

fn call_keys<'a>(value: &'a serde_json::Value, keys: &mut Vec<&'a str>) {
    if let Some(key) = value.get("call_key").and_then(serde_json::Value::as_str) {
        keys.push(key);
    }
    match value {
        serde_json::Value::Object(fields) => {
            for child in fields.values() {
                call_keys(child, keys);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                call_keys(child, keys);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn approved_preparation_key_matches_all_four_served_surfaces() {
    use std::time::Duration;
    use subc_protocol::scope::{ScopeKind, ScopeStamp};
    let mut rig = Rig::start_with_scopes("contract-approved-call").await;
    let (expected, run_id) = seed_approved_call(&rig);
    let owner = subc_protocol::Principal::Reserved {
        module_id: "sample-owner".into(),
    };
    let scope = ScopeStamp::new(owner.clone(), "approval-sample", 1, ScopeKind::Head)
        .with_owner_authorized(true);
    rig.bind_with_scope(7, Some(owner.clone()), Some(scope.clone()))
        .await;
    for (name, method, params) in [
        ("approved.session.read.json", "session.read", json!({})),
        (
            "approved.run.tool_calls.json",
            "run.tool_calls",
            json!({"run_id":run_id}),
        ),
        (
            "approved.session.history.json",
            "session.history",
            json!({"form":"display"}),
        ),
    ] {
        let frame = rig
            .request(7, json!({"method":method,"params":params}))
            .await;
        assert_eq!(
            frame.header.ty,
            subc_protocol::FrameType::Response,
            "{}: {}",
            method,
            String::from_utf8_lossy(&frame.body)
        );
        let body = result(&frame);
        let mut keys = Vec::new();
        call_keys(&body, &mut keys);
        assert!(!keys.is_empty(), "{method} must expose the prepared call");
        assert!(
            keys.iter().all(|key| *key == expected),
            "{method}: {keys:?} != {expected}"
        );
        if method == "session.history" {
            assert!(tool_part(&body).unwrap()["schema_pin"].is_string());
        }
        capture(name, &frame.body);
    }
    rig.bind_with_scope(8, Some(owner), Some(scope)).await;
    let frames = rig
        .subscribe_collect_bound(8, json!({"from":"start"}), Duration::from_millis(30))
        .await;
    for kind in ["tool_started", "tool_finished"] {
        let frame = frames
            .iter()
            .find(|frame| {
                serde_json::from_slice::<serde_json::Value>(&frame.body)
                    .is_ok_and(|value| value["event"]["type"] == kind)
            })
            .expect("approved lifecycle frame");
        let value: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(value["event"]["call_key"], expected);
        if kind == "tool_finished" {
            assert!(value["event"]["schema_pin"].is_string());
        } else {
            assert!(
                value["event"].get("schema_pin").is_none(),
                "preparation has not dispatched a definition yet"
            );
        }
        capture(&format!("approved.{kind}.frame.json"), &frame.body);
        let raw: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_slice(&frame.body).unwrap();
        capture(
            &format!("approved.{kind}.json"),
            raw["event"].get().as_bytes(),
        );
    }
    rig.finish().await;
}
