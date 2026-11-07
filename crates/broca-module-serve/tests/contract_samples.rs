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
    let describe = result(&frame);
    let groups = describe["capabilities"].as_array().unwrap();
    for required in [
        "notices",
        "message_senders",
        "read_display_facts",
        "call_identity",
    ] {
        assert!(
            groups.iter().any(|group| group == required),
            "missing {required}"
        );
    }
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
    seed_approved_call_with_legacy_notice(rig, false)
}

fn seed_approved_call_with_legacy_notice(rig: &Rig, notice: bool) -> (String, String) {
    seed_approved_call_with_ride(rig, notice, false, "synthetic result")
}

fn seed_approved_call_with_ride(
    rig: &Rig,
    notice: bool,
    envelope: bool,
    output: &str,
) -> (String, String) {
    seed_approved_call_with_ride_mode(rig, notice, envelope, output, false)
}

fn seed_approved_call_with_ride_mode(
    rig: &Rig,
    notice: bool,
    envelope: bool,
    output: &str,
    mode3: bool,
) -> (String, String) {
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
    if mode3 {
        // This fixture pins the published compaction-view render plan version.
        start["compaction_plan_version"] = json!(3);
        start["manifest"] = json!({
            "plan":json!({"compaction_item":{"provider":"provider","preset":"head","params":{}},"composition":{},"render_tags":false}).to_string(),
            "plan_identity":"plan", "composition_digest":"digest", "tools":[],
            "system_text":"", "system_digest":"", "join_version":1,
            "role_versions":{}, "fetches":[], "declarations":[]
        });
    }
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
        json!({"type":"tool_result","finished_at_ms":505,"batch_id":"approved-batch","result":{"tool_call_id":"approved-call","output":{"kind":"text","text":output},"is_error":false},"outcome":"completed"}),
    );
    if notice {
        append(
            json!({"type":"queued","submission_id":"legacy-notice","send_id":"legacy-client-key","sent_by":{"kind":"reserved","module_id":"sender"},"request":{"model":{"provider_module_id":"fake","model_id":"fake"},"input":[]}}),
        );
    }
    append(json!({"type":"tool_batch_finished","batch_id":"approved-batch"}));
    if notice {
        let mut delivery = json!({"type":"send_delivered","submission_id":"legacy-notice","run_id":run_id,"mode":"tool_result","tool_call_id":"approved-call","appended_text":"\n\nlegacy note"});
        if envelope {
            delivery["ride"] = json!({"call_key":key,"send_id":"legacy-client-key","sender":"sender","forms":["tool_result"],"form":"append"});
        }
        append(delivery);
    }
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

#[tokio::test]
async fn legacy_tool_delivery_keeps_delivered_as_on_default_and_opted_in_reads() {
    let mut rig = Rig::start("legacy-tool-delivery-fact").await;
    seed_approved_call_with_legacy_notice(&rig, true);
    for facts in [false, true] {
        let page = rig.request(7, json!({"method":"session.read","params":{"from_ordinal":0,"limit":50,"include_display_facts":facts}})).await;
        assert_eq!(
            page.header.ty,
            subc_protocol::FrameType::Response,
            "{page:?}"
        );
        let body = result(&page);
        let tool = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|message| message["message"]["role"] == "tool")
            .unwrap();
        assert_eq!(tool["delivered_as"]["mode"], "tool_result", "{body}");
    }
    rig.finish().await;
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
    // Replay sends tool_started before tool_finished, so waiting for tool_finished means both
    // have arrived, however slow the runner is.
    let frames = rig
        .subscribe_collect_bound_until(
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
async fn subscribe_resume_refuses_reused_position_after_restore() {
    let mut rig = Rig::start("subscribe-restore-0811").await;
    rig.script.push(&[Step::Text("backup")]);
    rig.send("backup", "backup", None).await;
    rig.wait_idle().await;
    let backup = rig.wal_bytes();
    rig.script.push(&[Step::Text("discarded")]);
    rig.send("discarded", "discarded", None).await;
    rig.wait_idle().await;
    let old_head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    let cursor = old_head["head"].clone();
    rig = rig.restart_restoring_wal(&backup).await;
    subscribe_refusal(&mut rig, cursor.clone(), "cursor_expired").await;
    for index in 0..3 {
        rig.script.push(&[Step::Text("replacement")]);
        rig.send(&format!("replacement-{index}"), "different history", None)
            .await;
        rig.wait_idle().await;
    }
    let new_head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(old_head["incarnation"], new_head["incarnation"]);
    assert!(new_head["head"]["wal_seq"].as_u64().unwrap() > cursor["wal_seq"].as_u64().unwrap());
    subscribe_refusal(&mut rig, cursor, "cursor_expired").await;
    rig.finish().await;
}

#[tokio::test]
async fn subscribe_resume_refuses_a_restored_legacy_frame_with_only_its_fence_changed() {
    fn legacy_frames(bytes: &[u8], change_fence_at: Option<u64>) -> Vec<u8> {
        use broca_wal::framing::{decode_next, encode, DecodeStep};
        let mut output = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            match decode_next(&bytes[offset..]) {
                DecodeStep::Lineage { consumed, .. } => {
                    output.extend_from_slice(&bytes[offset..offset + consumed]);
                    offset += consumed;
                }
                DecodeStep::Record { framed, consumed } => {
                    let fence = framed.fence + u64::from(change_fence_at == Some(framed.seq));
                    output.extend(encode(framed.seq, fence, &framed.record).unwrap());
                    offset += consumed;
                }
                _ => panic!("invalid synthetic log"),
            }
        }
        output
    }

    let mut rig = Rig::start("subscribe-restore-legacy-fence-0811").await;
    rig.script.push(&[Step::Text("synthetic answer")]);
    rig.send("start", "synthetic prompt", None).await;
    rig.wait_idle().await;
    let legacy = legacy_frames(&rig.wal_bytes(), None);
    rig = rig.restart_restoring_wal(&legacy).await;
    let head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    let cursor = head["head"].clone();
    assert!(cursor["digest"].is_string(), "{head}");
    let seq = cursor["wal_seq"].as_u64().unwrap();
    let restored = legacy_frames(&legacy, Some(seq));
    assert_ne!(restored, legacy);
    // Encoding the same records again is byte-stable; only the named fence and
    // its covering digest change in the restored log.
    assert_eq!(legacy_frames(&legacy, None), legacy);
    rig = rig.restart_restoring_wal(&restored).await;
    subscribe_refusal(&mut rig, cursor, "cursor_expired").await;
    rig.finish().await;
}

#[tokio::test]
async fn notices_and_sender_contract_samples() {
    use broca_core::{FileWalStore, WalStore};
    use broca_wal::WalRecord;
    let mut rig = Rig::start_with_ride_writer("notices-and-sender-0811").await;
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
    let records = FileWalStore::new(rig.dir.join("wal"))
        .read_records(&rig.session)
        .unwrap();
    let envelope = records
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::SendDelivered { ride, .. } => Some(ride),
            _ => None,
        })
        .expect("notice delivery record");
    assert!(
        envelope.is_some(),
        "fixture explicitly enables the envelope writer"
    );
    if let Some(ride) = envelope {
        assert_eq!(ride.send_id, "sample-notice");
        assert_eq!(ride.sender, "sample-sender");
        assert_eq!(ride.forms, ["tool_result"]);
        assert_eq!(ride.form, broca_wal::record::RideForm::Append);
    }
    let lineage = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records_range(&rig.session, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    let key = records
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::ToolDispatchIntent {
                tool_call_id,
                call_key,
                ..
            } if tool_call_id.0 == "sample-carrying-call" => {
                broca_core::tool_call_key(Some(&lineage), frame.seq, call_key.as_deref())
            }
            _ => None,
        })
        .unwrap();
    if let Some(ride) = envelope {
        assert_eq!(ride.call_key, key);
    }
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
    assert_eq!(
        carrying["delivered_as"],
        json!({"mode":"tool_result","call_key":key})
    );
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
        .find_map(|frame| match &frame.record {
            WalRecord::ToolResult { result, .. }
                if result.tool_call_id.0 == "sample-carrying-call" =>
            {
                Some(result)
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
async fn carrying_tool_message_keeps_legacy_delivered_as_on_default_and_fact_reads() {
    let mut rig = Rig::start("legacy-delivered-as").await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("carrier"), Step::Text("answer")]);
    rig.send("start", "prompt", None).await;
    rig.wait_entered().await;
    rig.send_with(
        "notice",
        "notice",
        None,
        json!({"delivery":["tool_result"]}),
    )
    .await;
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    for params in [json!({}), json!({"include_display_facts":true})] {
        let frame = rig
            .request(7, json!({"method":"session.read","params":params}))
            .await;
        let body = result(&frame);
        let carrying = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["message"]["role"] == "tool")
            .unwrap();
        assert_eq!(carrying["delivered_as"]["mode"], "tool_result", "{body}");
        assert!(carrying["delivered_as"]["call_key"].is_string(), "{body}");
    }
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
    use broca_core::WalStore;
    let range = broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records_range(&rig.session, 0, None, None)
        .unwrap();
    let lineage = range.lineage_id.unwrap().to_string();
    let expected: std::collections::BTreeMap<_, _> = match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => records
            .iter()
            .filter_map(|frame| match &frame.record {
                broca_wal::WalRecord::ToolNoDispatch { tool_call_id, .. } => {
                    Some((tool_call_id.0.clone(), format!("{lineage}:{}", frame.seq)))
                }
                _ => None,
            })
            .collect(),
        _ => panic!("complete denied-call WAL"),
    };
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
            .iter()
            .filter(|record| matches!(record, broca_wal::WalRecord::ToolNoDispatch { .. }))
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
async fn imported_duplicate_synthesized_results_have_no_call_key() {
    use broca_core::WalStore;
    use broca_protocol::{
        ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
    };
    let mut rig = Rig::start("imported-keyless-duplicates").await;
    let mut start: broca_wal::WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let broca_wal::WalRecord::RunStarted {
        input,
        session,
        origin,
        ..
    } = &mut start
    {
        *session = rig.session.clone();
        *origin = serde_json::from_value(
            json!({"kind":"seeded_from","origin_kind":"import","source_ref":"synthetic import"}),
        )
        .unwrap();
        let call = ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("same".into()),
            tool_name: "work".into(),
            input: json!({}),
        });
        let result = ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("same".into()),
            output: ToolOutput::error("synthesized missing result"),
            is_error: true,
        });
        *input = std::sync::Arc::new(vec![
            Message::new(Role::Assistant, vec![call.clone(), call]),
            Message::new(Role::Tool, vec![result.clone(), result]),
        ]);
    }
    let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&rig.session).unwrap();
    wal.append(1, &start, false).unwrap();
    let end = serde_json::from_value(
        json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
    )
    .unwrap();
    wal.append(1, &end, true).unwrap();
    drop(wal);
    let read = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    let calls: Vec<_> = read["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["tool_calls"].as_array().into_iter().flatten())
        .collect();
    assert_eq!(calls.len(), 2);
    assert!(calls
        .iter()
        .all(|call| call["tool_call_id"] == "same" && call.get("call_key").is_none()));
    rig.finish().await;
}

#[tokio::test]
async fn unkeyed_notice_collision_prefers_the_senders_keyed_send() {
    let mut rig = Rig::start_with_ride_writer("unkeyed-notice-collision").await;
    rig.bind_as(
        7,
        Some(subc_protocol::Principal::Reserved {
            module_id: "collision-sender".into(),
        }),
    )
    .await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("collision-call"), Step::Text("answer")]);
    rig.send("start", "synthetic task", None).await;
    rig.wait_entered().await;
    let unkeyed = result(
        &rig.send_with(
            "unused",
            "unkeyed payload",
            None,
            json!({"send_id":null,"delivery":["tool_result"]}),
        )
        .await,
    );
    let minted = unkeyed["submission_id"]
        .as_str()
        .expect("original riding send returns a submission id")
        .to_owned();
    let keyed = result(
        &rig.send_with(
            &minted,
            "keyed payload",
            None,
            json!({"delivery":["tool_result"]}),
        )
        .await,
    );
    assert_ne!(unkeyed["submission_id"], keyed["submission_id"]);
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    let page = result(
        &rig.request(
            7,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await,
    );
    let notices: Vec<_> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["notices"].as_array().into_iter().flatten())
        .collect();
    assert_eq!(notices.len(), 1, "{page}");
    assert_eq!(notices[0]["sender"], "collision-sender");
    assert_eq!(notices[0]["send_id"], minted);
    assert_eq!(notices[0]["text"], "keyed payload");
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
    let notices: Vec<serde_json::Value> = frames
        .iter()
        .filter_map(|frame| serde_json::from_slice::<serde_json::Value>(&frame.body).ok())
        .filter(|value| value["event"]["type"] == "notice_delivered")
        .collect();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0]["event"]["send_id"], minted);
    assert_eq!(notices[0]["event"]["text"], "keyed payload");
    assert_eq!(
        rig.records()
            .iter()
            .filter(|record| matches!(record, broca_wal::WalRecord::SendDelivered { .. }))
            .count(),
        2,
        "suppression does not discard a delivered send"
    );
    rig.finish().await;
}

#[tokio::test]
async fn undeclared_sender_attribution_refuses_send_and_refresh_before_writes() {
    let mut rig = Rig::start("undeclared-attribution").await;
    rig.script.push(&[Step::Text("answer")]);
    rig.send("opening", "synthetic prompt", None).await;
    rig.wait_idle().await;
    let before = rig.wal_bytes();
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
        assert_eq!(rig.wal_bytes(), before);
    }
    rig.finish().await;
}

fn output_texts(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            if fields.get("type").is_some_and(|kind| kind == "tool_result") {
                if let Some(text) = fields.get("content").and_then(serde_json::Value::as_str) {
                    out.push(text.to_owned());
                }
            }
            if let Some(output) = fields.get("output") {
                if let Some(text) = output.as_str().or_else(|| output["text"].as_str()) {
                    out.push(text.to_owned());
                }
            }
            for child in fields.values() {
                output_texts(child, out);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                output_texts(child, out);
            }
        }
        _ => {}
    }
}

async fn recorded_ride_read_matrix(envelope: bool) {
    use broca_core::{FileWalStore, WalStore};
    let mut rig = Rig::start(if envelope {
        "recorded-envelope-reads"
    } else {
        "recorded-legacy-reads"
    })
    .await;
    let (_, _) = seed_approved_call_with_ride(&rig, true, envelope, "worked");
    let mut failures = Vec::new();
    let expected = if envelope {
        "worked"
    } else {
        "worked\n\nlegacy note"
    };
    let mut history_version = None;
    for (name, method, params) in [
        ("default read", "session.read", json!({"from_ordinal":0})),
        (
            "fact read",
            "session.read",
            json!({"from_ordinal":0,"include_display_facts":true}),
        ),
        (
            "raw history",
            "session.history",
            json!({"form":"raw","limit":100}),
        ),
        (
            "display history",
            "session.history",
            json!({"form":"display","limit":100}),
        ),
        (
            "warm default read",
            "session.read",
            json!({"from_ordinal":0}),
        ),
    ] {
        let frame = rig
            .request(7, json!({"method":method,"params":params}))
            .await;
        assert_eq!(
            frame.header.ty,
            subc_protocol::FrameType::Response,
            "{name}: {frame:?}"
        );
        let body = result(&frame);
        let mut outputs = Vec::new();
        output_texts(&body, &mut outputs);
        if outputs != [expected] {
            failures.push(format!(
                "{name}: outputs={outputs:?}, expected {expected:?}"
            ));
        }
        if method == "session.read" {
            if let Some(version) = &history_version {
                assert_eq!(&body["history_version"], version);
            } else {
                history_version = Some(body["history_version"].clone());
            }
        }
        if name == "fact read" || name == "display history" {
            let encoded = body.to_string();
            let notices = encoded.matches("\"delivered_as\":\"tool_result\"").count();
            if notices != usize::from(envelope) {
                failures.push(format!(
                    "{name}: expected {} notice facts, got {notices}",
                    usize::from(envelope)
                ));
            }
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
    let values: Vec<serde_json::Value> = frames
        .iter()
        .map(|frame| serde_json::from_slice(&frame.body).unwrap())
        .collect();
    let mut outputs = Vec::new();
    for value in &values {
        output_texts(value, &mut outputs);
    }
    assert_eq!(
        outputs,
        ["worked"],
        "subscribe keeps native results for both record formats"
    );
    let notices: Vec<_> = values
        .iter()
        .filter(|value| value["event"]["type"] == "notice_delivered")
        .collect();
    if notices.len() != usize::from(envelope) {
        failures.push(format!(
            "subscribe: expected {} notice events, got {}",
            usize::from(envelope),
            notices.len()
        ));
    }
    if envelope && notices.len() == 1 {
        assert_eq!(notices[0]["event"]["text"], "legacy note");
        assert_eq!(notices[0]["event"]["send_id"], "legacy-client-key");
    }
    let store = FileWalStore::new(rig.dir.join("wal"));
    let range = store
        .read_records_range(&rig.session, 0, None, None)
        .unwrap();
    let transcript = broca_core::compaction_v2::transcript::project(
        &store.read_records(&rig.session).unwrap(),
        &range.lineage_id.unwrap().to_string(),
    )
    .unwrap();
    let entries = serde_json::to_value(&transcript.entries).unwrap();
    let mut entry_outputs = Vec::new();
    output_texts(&entries, &mut entry_outputs);
    assert_eq!(
        entry_outputs,
        ["worked"],
        "session.entries preserves native output for both formats"
    );
    assert_eq!(
        entries
            .to_string()
            .matches("\"text\":\"legacy note\"")
            .count(),
        1,
        "session.entries retains separate Delivered metadata"
    );
    rig.finish().await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn recorded_envelope_is_served_without_joining_notice_on_every_read() {
    // Write the envelope directly so a reader build exercises a WAL produced by the writer build.
    recorded_ride_read_matrix(true).await;
}

#[tokio::test]
async fn recorded_legacy_ride_keeps_joined_reads_without_notice_facts_or_events() {
    recorded_ride_read_matrix(false).await;
}

#[test]
fn frozen_ride_model_body_equals_the_legacy_physical_append_body() {
    let original: broca_protocol::ToolResult = serde_json::from_value(json!({
        "tool_call_id":"call", "output":{"kind":"text","text":"worked"}, "is_error":false
    }))
    .unwrap();
    let ride: broca_wal::record::RideMetadata = serde_json::from_value(json!({
        "call_key":"fixture:1", "send_id":"notice", "sender":"sender", "forms":["tool_result"], "form":"append"
    })).unwrap();
    let mut frozen = original.clone();
    broca_core::delivery_order::append_ride(&mut frozen, &ride, "\n\nsynthetic notice ☃");
    let mut physical = original;
    broca_core::delivery_order::append_to_result(&mut physical, "\n\nsynthetic notice ☃");
    let body = |result: broca_protocol::ToolResult| {
        serde_json::to_vec(&json!({"input":[{
        "role":"tool", "message_id":"fixture-tool-message", "content":[broca_protocol::ContentBlock::ToolResult(result)]
    }]})).unwrap()
    };
    let frozen = body(frozen);
    let physical = body(physical);
    capture("model.frozen-ride-request.json", &frozen);
    capture("model.physical-append-request.json", &physical);
    let pinned = "{\"input\":[{\"content\":[{\"is_error\":false,\"output\":{\"kind\":\"text\",\"text\":\"worked\\n\\nsynthetic notice ☃\"},\"tool_call_id\":\"call\",\"type\":\"tool_result\"}],\"message_id\":\"fixture-tool-message\",\"role\":\"tool\"}]}";
    assert_eq!(frozen, pinned.as_bytes());
    assert_eq!(physical, pinned.as_bytes());
    assert_eq!(frozen, physical);
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

#[tokio::test]
async fn recorded_rides_keep_native_output_on_the_compaction_entries_serve_path() {
    use broca_core::compaction_v2::protocol::read::{Detail, ReadRequest, Select};
    for envelope in [false, true] {
        let mut rig = Rig::start(if envelope {
            "entries-envelope"
        } else {
            "entries-legacy"
        })
        .await;
        seed_approved_call_with_ride_mode(&rig, true, envelope, "worked", true);
        rig.bind_compaction(9, "provider").await;
        let params = serde_json::to_value(ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: Some(100),
            },
            Detail::Full,
        ))
        .unwrap();
        let frame = rig
            .request(9, json!({"method":"session.entries","params":params}))
            .await;
        assert_eq!(
            frame.header.ty,
            subc_protocol::FrameType::Response,
            "{frame:?}"
        );
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        let mut outputs = Vec::new();
        output_texts(&body, &mut outputs);
        assert_eq!(outputs, ["worked"]);
        assert_eq!(
            body.to_string().matches("\"text\":\"legacy note\"").count(),
            1,
            "separate Delivered metadata remains served"
        );
        rig.finish().await;
    }
}
