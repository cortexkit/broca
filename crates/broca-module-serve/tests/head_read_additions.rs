//! Default reply bytes captured through the session-bound management route.
mod delivery_rig;
use broca_core::{FileWalStore, WalStore};
use broca_wal::WalRecord;
use delivery_rig::*;
use serde_json::json;

fn capture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name)
}

fn assert_capture(frame: &subc_protocol::Frame, name: &str) {
    assert_eq!(frame.header.ty, subc_protocol::FrameType::Response);
    let path = capture_path(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, [&frame.body[..], b"\n"].concat()).unwrap();
        println!("{name}: {}", String::from_utf8_lossy(&frame.body));
    }
    assert_eq!(
        frame.body.as_slice(),
        std::fs::read(path).unwrap().strip_suffix(b"\n").unwrap()
    );
}

#[tokio::test]
async fn unknown_facts_keep_pre_addition_default_reply_bytes() {
    let mut rig = Rig::start("unknown-head-facts").await;
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    assert_capture(&read, "session_read.before_head_additions.json");
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    assert_capture(&head, "session_head.before_head_additions.json");
    rig.finish().await;
}

fn capture_known(frame: &subc_protocol::Frame, name: &str) {
    capture_known_with_run(frame, name, None);
}

fn capture_known_with_run(frame: &subc_protocol::Frame, name: &str, run: Option<&str>) {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Response,
        "{:?}",
        result(frame)
    );
    let answer = result(frame);
    let mut bytes = String::from_utf8(frame.body.clone()).unwrap();
    for field in ["lineage_id", "incarnation"] {
        if let Some(value) = answer[field].as_str() {
            bytes = bytes.replace(
                &format!("\"{field}\":\"{value}\""),
                &format!("\"{field}\":\"@LOG_ID@\""),
            );
        }
    }
    if let Some(at) = answer["updated_at"].as_u64() {
        bytes = bytes.replace(
            &format!("\"updated_at\":{at}"),
            "\"updated_at\":\"@UPDATED_AT@\"",
        );
    }
    if let Some(run) = run {
        bytes = bytes.replace(run, "@RUN_ID@");
    }
    if let Some(version) = answer["implementation_version"].as_str() {
        bytes = bytes.replace(
            &format!("\"implementation_version\":\"{version}\""),
            "\"implementation_version\":\"@VERSION@\"",
        );
    }
    let path = capture_path(name);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{bytes}\n")).unwrap();
        println!("{name}: {bytes}");
    }
    assert_eq!(bytes, std::fs::read_to_string(path).unwrap().trim_end());
}

fn seed_history(rig: &Rig, planned: bool, owner: Option<&str>) {
    let mut start: WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let WalRecord::RunStarted {
        run_id,
        session,
        principal,
        ts_ms,
        manifest,
        ..
    } = &mut start
    {
        *run_id = broca_protocol::RunId("r1".into());
        *session = rig.session.clone();
        *principal = owner.map(|owner| broca_protocol::BindPrincipal::Reserved {
            module_id: owner.into(),
        });
        *ts_ms = Some(500);
        *manifest = Some(Box::new(broca_wal::PlanManifest {
            plan: broca_wal::OpaqueJson::from_json_text(
                json!({"composition":{}, "senders":[
                    {"module":"named"}, {"module":"starter","idle_start":true,"interrupt":true}
                ]})
                .to_string(),
            )
            .unwrap(),
            plan_identity: "plan".into(),
            composition_digest: "digest".into(),
            tools: Default::default(),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        }));
        if !planned {
            *manifest = None;
        }
    }
    let steps = [
        start,
        serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap(),
        serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,
            "request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b",
            "outcome":"complete","ts_ms":505})).unwrap(),
        serde_json::from_value(json!({"type":"model_step_finished","step_id":1,
            "assistant_message":{"message_id":"a","content":[{"type":"text","text":"answer"}]},
            "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
        serde_json::from_value(json!({"type":"turn_finished","step_id":1})).unwrap(),
        serde_json::from_value(json!({"type":"run_finished","reason":"completed","ts_ms":506,
            "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
    ];
    let store = FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&rig.session).unwrap();
    for record in steps {
        wal.append(1, &record, false).unwrap();
    }
    wal.sync().unwrap();
}

#[tokio::test]
async fn recorded_read_times_and_caller_head_grants_match_serve_goldens() {
    let mut rig = Rig::start("head-additions-goldens").await;
    let describe = rig
        .request(7, json!({"method":"role.describe","params":{}}))
        .await;
    for capability in ["head_sender", "session_usage"] {
        assert!(result(&describe)["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(capability)));
    }
    capture_known(&describe, "role_describe.head_additions.json");
    seed_history(&rig, true, Some("owner"));
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    let entries = result(&read)["messages"].as_array().unwrap().clone();
    assert_eq!(entries[0]["written_at"], 500);
    assert_eq!(entries[1]["written_at"], 505);
    assert!(entries
        .iter()
        .all(|entry| entry["message"].get("written_at").is_none()));
    capture_known(&read, "session_read.written_at.json");
    for (channel, caller, golden, expected_modes, interrupt, owner) in [
        (
            8,
            "owner",
            "session_head.owner.json",
            vec!["tool_result", "steer", "idle_start"],
            true,
            true,
        ),
        (
            9,
            "named",
            "session_head.named.json",
            vec!["tool_result", "steer"],
            false,
            false,
        ),
        (
            10,
            "stranger",
            "session_head.ungranted.json",
            vec![],
            false,
            false,
        ),
        (
            11,
            "starter",
            "session_head.starter.json",
            vec!["tool_result", "steer", "idle_start"],
            true,
            false,
        ),
    ] {
        rig.bind_as(
            channel,
            Some(subc_protocol::Principal::Reserved {
                module_id: caller.into(),
            }),
        )
        .await;
        let head = rig
            .request(channel, json!({"method":"session.head","params":{}}))
            .await;
        let answer = result(&head);
        assert_eq!(answer["model"], json!({"provider":"p","model":"m"}));
        assert_eq!(
            answer["usage"],
            json!({"since":"r1","steps":1,"unreported_steps":0,"input_tokens":7,"output_tokens":3,
            "by_model":[{"provider":"p","model":"m","steps":1,"unreported_steps":0,"input_tokens":7,"output_tokens":3}]})
        );
        assert_eq!(answer["incarnation"], answer["lineage_id"]);
        if expected_modes.is_empty() {
            assert!(answer.get("sender").is_none());
        } else {
            assert_eq!(answer["sender"]["modes"], json!(expected_modes));
            assert_eq!(answer["sender"]["interrupt"], interrupt);
            assert_eq!(answer["sender"].get("owner").is_some(), owner);
        }
        capture_known(&head, golden);
    }
    rig.finish().await;
}

#[tokio::test]
async fn newly_written_times_and_session_spend_survive_projection_and_restart() {
    let mut rig =
        Rig::start_with_idle_ttl("new-message-times", std::time::Duration::from_millis(100)).await;
    let _diagnostics = rig.failure_diagnostics();
    *rig.script.attempt_time.lock().unwrap() = Some(777);
    rig.script.push(&[Step::Usage {
        content: vec![broca_protocol::ContentBlock::Text {
            text: "answer".into(),
        }],
        usage: broca_protocol::Usage {
            input_tokens: Some(9),
            output_tokens: Some(0),
            ..Default::default()
        },
    }]);
    let sent = rig.send("first", "question", None).await;
    let first_run = result(&sent)["run_id"].clone();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let before = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert_eq!(before["messages"][1]["written_at"], 777);
    assert!(before["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["written_at"].as_u64().is_some_and(|at| at > 0)));
    for record in rig.records() {
        if matches!(
            record,
            WalRecord::RunStarted { .. } | WalRecord::ModelStepFinished { .. }
        ) {
            assert!(record
                .required_features()
                .contains(&broca_wal::framing::MESSAGE_TIMES_FEATURE));
        }
    }
    let first_head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(first_head["usage"]["since"], first_run);
    assert!(first_head["usage"].get("reasoning_tokens").is_none());
    rig.wait_closed().await;
    let mut rig = rig.restart().await;
    let after = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    assert_eq!(before["messages"], after["messages"]);
    rig.script.push(&[Step::Text("unreported")]);
    let second = rig.send("second", "again", None).await;
    assert_eq!(
        second.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&second.body)
    );
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let head = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(head["incarnation"], first_head["incarnation"]);
    assert_eq!(head["usage"]["since"], first_run);
    assert_eq!(head["usage"]["steps"], 2);
    assert_eq!(head["usage"]["unreported_steps"], 1);
    assert_eq!(head["usage"]["input_tokens"], 9);
    assert_eq!(head["usage"]["output_tokens"], 0);
    assert!(head["usage"].get("reasoning_tokens").is_none());
    rig.finish().await;
}

#[tokio::test]
async fn planless_head_reports_a_grant_only_to_its_owner() {
    let mut rig = Rig::start("head-planless-grants").await;
    seed_history(&rig, false, Some("owner"));
    for (channel, caller) in [(8, "owner"), (9, "named")] {
        rig.bind_as(
            channel,
            Some(subc_protocol::Principal::Reserved {
                module_id: caller.into(),
            }),
        )
        .await;
        let head = result(
            &rig.request(channel, json!({"method":"session.head","params":{}}))
                .await,
        );
        if caller == "owner" {
            assert_eq!(
                head["sender"],
                json!({"modes":["tool_result","steer","idle_start"],"interrupt":true,"owner":true})
            );
        } else {
            assert!(head.get("sender").is_none());
        }
    }
    rig.finish().await;
}

#[tokio::test]
async fn queued_input_and_steer_keep_their_admission_times() {
    let mut rig = Rig::start("queued-admission-times").await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[
        Step::Call("held"),
        Step::Text("active answer"),
        Step::Text("queued answer"),
    ]);
    rig.script.hold_tool(true);
    rig.send("active", "first", None).await;
    rig.wait_entered().await;
    let queued = rig.send("queued", "queued input", Some("queue")).await;
    assert_eq!(queued.header.ty, subc_protocol::FrameType::Response);
    let steered = rig.send("steered", "steered input", Some("steer")).await;
    assert_eq!(steered.header.ty, subc_protocol::FrameType::Response);
    let records = rig.records();
    let queued_at = records
        .iter()
        .find_map(|record| match record {
            WalRecord::Queued { written_at_ms, .. } => *written_at_ms,
            _ => None,
        })
        .expect("queue admission records a time");
    let steered_at = records
        .iter()
        .find_map(|record| match record {
            WalRecord::Steered { written_at_ms, .. } => *written_at_ms,
            _ => None,
        })
        .expect("steer admission records a time");
    rig.script.hold_tool(false);
    let records = rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    assert!(records.iter().any(|record| matches!(record,
        WalRecord::RunStarted { submission_id: Some(_), written_at_ms: Some(at), .. } if *at == queued_at)));
    for record in &records {
        if matches!(record, WalRecord::Queued { .. } | WalRecord::Steered { .. }) {
            assert!(record
                .required_features()
                .contains(&broca_wal::MESSAGE_TIMES_FEATURE));
        }
    }
    let page = result(
        &rig.request(7, json!({"method":"session.read","params":{}}))
            .await,
    );
    for (text, at) in [("queued input", queued_at), ("steered input", steered_at)] {
        let entry = page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["message"]["content"][0]["text"] == text)
            .unwrap();
        assert_eq!(entry["written_at"], at);
    }
    rig.finish().await;
}

#[tokio::test]
async fn ownerless_legacy_head_matches_permissive_real_send_admission() {
    let mut rig = Rig::start("legacy-head-owner").await;
    let _diagnostics = rig.failure_diagnostics();
    seed_history(&rig, false, None);
    rig.bind_as(
        8,
        Some(subc_protocol::Principal::Reserved {
            module_id: "legacy-caller".into(),
        }),
    )
    .await;
    let head = result(
        &rig.request(8, json!({"method":"session.head","params":{}}))
            .await,
    );
    assert_eq!(
        head["sender"],
        json!({"modes":["tool_result","steer","idle_start"],"interrupt":true,"owner":true})
    );
    rig.script.push(&[Step::Text("accepted legacy send")]);
    let send = rig
        .request(
            8,
            json!({"method":"session.send","params":{"send_id":"legacy","prompt":"more","model":{"provider":"p","model":"m"}}}),
        )
        .await;
    assert_eq!(
        send.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&send.body)
    );
    rig.wait_for_finished(2).await;
    rig.finish().await;
}

#[tokio::test]
async fn retained_head_checkpoint_and_typical_read_cost_through_serve() {
    use broca_core::MaterializedStore;
    let mut rig = Rig::start("retained-head-cost").await;
    let store = FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open_with_retention(&rig.session, Some(3600)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for turn in 0..16 {
        let mut start: WalRecord = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        if let WalRecord::RunStarted {
            run_id,
            session,
            retention,
            ts_ms,
            written_at_ms,
            ..
        } = &mut start
        {
            *run_id = broca_protocol::RunId(format!("metric-{turn}"));
            *session = rig.session.clone();
            *retention = Some(3600);
            *ts_ms = Some(now + turn);
            *written_at_ms = Some(now + turn);
        }
        let records = [start,
            serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap(),
            serde_json::from_value(json!({"type":"model_step_finished","step_id":1,"written_at_ms":now + turn,
                "assistant_message":{"message_id":format!("m{turn}"),"content":[{"type":"text","text":"a typical short answer"}]},
                "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
            serde_json::from_value(json!({"type":"turn_finished","step_id":1})).unwrap(),
            serde_json::from_value(json!({"type":"run_finished","reason":"completed","ts_ms":now + turn,
                "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
        ];
        for record in records {
            wal.append(1, &record, false).unwrap();
        }
    }
    wal.sync().unwrap();
    drop(wal);
    let cold = std::time::Instant::now();
    let first = result(
        &rig.request(7, json!({"method":"session.head","params":{}}))
            .await,
    );
    let cold_ms = cold.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(first["last_ordinal"], 31);
    assert_eq!(first["usage"]["steps"], 16);
    let db = broca_store::SqliteStore::open(rig.dir.join("store.db")).unwrap();
    let lineage = first["lineage_id"].as_str().unwrap();
    assert_eq!(
        db.read_retained_head(&rig.session, lineage)
            .unwrap()
            .unwrap()
            .facts
            .usage
            .unwrap()
            .totals
            .steps,
        16
    );
    let mut costs = Vec::new();
    for _ in 0..25 {
        let started = std::time::Instant::now();
        let head = result(
            &rig.request(7, json!({"method":"session.head","params":{}}))
                .await,
        );
        costs.push(started.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(head["usage"], first["usage"]);
    }
    costs.sort_by(f64::total_cmp);
    println!("retained session.head: messages=32 runs=16 samples=25 cold_ms={cold_ms:.3} median_ms={:.3} p95_ms={:.3} mean_ms={:.3}",
        costs[12], costs[23], costs.iter().sum::<f64>() / costs.len() as f64);
    rig.wal_path = store.ephemeral_wal_path(&rig.session);
    rig.script.push(&[Step::Text("one more retained step")]);
    let sent = rig
        .request(
            7,
            json!({"method":"session.send","params":{"send_id":"retained-next","prompt":"next",
        "model":{"provider":"p","model":"m"}}}),
        )
        .await;
    assert_eq!(
        sent.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&sent.body)
    );
    rig.wait_for_finished(17).await;
    rig.wait_idle().await;
    assert_eq!(
        db.read_retained_head(&rig.session, lineage)
            .unwrap()
            .unwrap()
            .facts
            .usage
            .unwrap()
            .totals
            .steps,
        17,
        "the run writer checkpoints new spend before any subsequent head read"
    );
    drop(db);
    rig.finish().await;
}

#[tokio::test]
async fn reasoning_tokens_are_an_output_breakdown_on_run_and_session_spend() {
    let mut rig = Rig::start("reasoning-spend").await;
    let _diagnostics = rig.failure_diagnostics();
    rig.script.push(&[
        Step::Usage {
            content: vec![broca_protocol::ContentBlock::ToolCall(
                broca_protocol::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId("reasoning-call".into()),
                    tool_name: "work".into(),
                    input: json!({}),
                    thought_signature: None,
                },
            )],
            usage: broca_protocol::Usage {
                input_tokens: Some(7),
                output_tokens: Some(10),
                reasoning_tokens: Some(3),
                ..Default::default()
            },
        },
        Step::Usage {
            content: vec![broca_protocol::ContentBlock::Text {
                text: "answer".into(),
            }],
            usage: broca_protocol::Usage {
                input_tokens: Some(9),
                output_tokens: Some(12),
                reasoning_tokens: Some(4),
                ..Default::default()
            },
        },
    ]);
    let send = rig.send("reasoning", "question", None).await;
    let run = result(&send)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let result = result(
        &rig.request(7, json!({"method":"run.result","params":{"run_id":run}}))
            .await,
    );
    assert_eq!(result["usage"]["output_tokens"], 22);
    assert_eq!(result["usage"]["reasoning_tokens"], 7);
    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    let answer = delivery_rig::result(&head);
    assert_eq!(answer["usage"]["since"], run);
    assert_eq!(answer["usage"]["output_tokens"], 22);
    assert_eq!(answer["usage"]["reasoning_tokens"], 7);
    assert_eq!(answer["usage"]["input_tokens"], 16);
    assert_eq!(answer["usage"]["steps"], 2);
    capture_known_with_run(&head, "session_head.reasoning_usage.json", Some(&run));
    rig.finish().await;
}
