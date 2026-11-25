use super::*;

fn sample(name: &str, frame: &subc_protocol::Frame) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        eprintln!(
            "BROCA_SAMPLE {name} {}",
            String::from_utf8_lossy(&frame.body)
        );
    }
}

#[tokio::test]
async fn marked_sends_are_display_only_and_survive_reopen() {
    let (mut rig, _) = scoped_plan_rig("message-marks-read", false).await;
    let describe = rig
        .request(8, json!({"method":"role.describe","params":{}}))
        .await;
    sample("role-describe.json", &describe);
    rig.script.push(&[Step::Text("answer")]);
    let sent = send_plan(
        &mut rig,
        8,
        "agent-words",
        "agent words",
        Some(&vector("plans/broca-head-no-compaction.json")),
        json!({"speaks_for":{"agent_id":"Ada"}}),
    )
    .await;
    sample("marks-send.json", &sent);
    assert_eq!(
        sent.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&sent.body)
    );
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let default = rig
        .request(8, json!({"method":"session.read","params":{}}))
        .await;
    sample("marks-default-read.json", &default);
    let default = result(&default);
    let read = rig
        .request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await;
    sample("marks-read.json", &read);
    let read = result(&read);
    let entry = &read["messages"][0];
    assert_eq!(entry["sender"], "prefrontal-core");
    assert_eq!(entry["speaks_for"], json!({"agent_id":"Ada"}));
    assert!(entry.get("notice").is_none());
    assert_eq!(entry["message"], default["messages"][0]["message"]);
    assert_eq!(read["history_version"], default["history_version"]);
    for entry in default["messages"].as_array().unwrap() {
        assert!(entry.get("speaks_for").is_none());
        assert!(entry.get("notice").is_none());
        assert!(!entry["message"].to_string().contains("speaks_for"));
    }
    for form in ["raw", "display"] {
        let history = rig
            .request(
                8,
                json!({"method":"session.history","params":{"form":form}}),
            )
            .await;
        sample(&format!("marks-history-{form}.json"), &history);
        let history = result(&history);
        let entry = &history["messages"][0];
        let marked = if form == "display" {
            &entry["row"]
        } else {
            entry
        };
        assert_eq!(
            marked["speaks_for"],
            json!({"agent_id":"Ada"}),
            "{form}: {history}"
        );
    }
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    rig.bind_with_scope(8, Some(scope_owner()), Some(scope_stamp(false)))
        .await;
    let read = result(
        &rig.request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await,
    );
    assert_eq!(read["messages"][0]["speaks_for"], json!({"agent_id":"Ada"}));
    rig.script.push(&[Step::Text("noted")]);
    let notice = send_plan(
        &mut rig,
        8,
        "automatic",
        "automatic notice",
        None,
        json!({"notice":true}),
    )
    .await;
    assert_eq!(
        notice.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&notice.body)
    );
    sample("notice-send.json", &notice);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let read = rig
        .request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await;
    sample("notice-read.json", &read);
    let read = result(&read);
    let entry = read["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["message"]["content"][0]["text"] == "automatic notice")
        .unwrap();
    assert_eq!(entry["notice"], true);
    assert!(entry.get("speaks_for").is_none());
    rig.finish().await;
}

#[tokio::test]
async fn invalid_or_unauthorized_marks_refuse_before_any_write() {
    let (mut rig, _) = scoped_plan_rig("message-marks-refuse", false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    // Broca declares notice_with_speaks_for, so a notice may carry a speaker and the speaker a
    // name; a name outside its grammar still refuses.
    for (index, (marks, field)) in [
        (
            json!({"speaks_for":{"agent_id":"Ada","name":""}}),
            "speaks_for.name",
        ),
        (
            json!({"speaks_for":{"agent_id":"Ada","name":"x".repeat(65)},"notice":true}),
            "speaks_for.name",
        ),
        (
            json!({"speaks_for":{"agent_id":"Ada","name":"a\nb"}}),
            "speaks_for.name",
        ),
        (
            json!({"speaks_for":{"agent_id":"Ada","name":7}}),
            "speaks_for.name",
        ),
        (json!({"speaks_for":{"agent_id":""}}), "speaks_for.agent_id"),
        (
            json!({"speaks_for":{"agent_id":"x".repeat(129)}}),
            "speaks_for.agent_id",
        ),
        (
            json!({"speaks_for":{"agent_id":"a\nb"}}),
            "speaks_for.agent_id",
        ),
        (
            json!({"speaks_for":{"agent_id":"é"}}),
            "speaks_for.agent_id",
        ),
        (
            json!({"speaks_for":{"agent_id":"Ada","extra":true}}),
            "speaks_for",
        ),
        (json!({"speaks_for":null}), "speaks_for.agent_id"),
        (json!({"notice":"true"}), "notice"),
    ]
    .into_iter()
    .enumerate()
    {
        let before = rig.records().await;
        let reply = send_plan(
            &mut rig,
            8,
            &format!("invalid-{index}"),
            "refuse",
            Some(&plan),
            marks,
        )
        .await;
        let error = error_body(&reply);
        if index == 0 {
            sample("marks-invalid-name.json", &reply);
        }
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], field, "{error}");
        assert_eq!(rig.records().await, before);
    }
    rig.finish().await;
    let mut rig = Rig::start("unscoped-agent-words").await;
    let before = rig.records().await;
    let reply = send_plan(
        &mut rig,
        7,
        "unscoped",
        "refuse",
        None,
        json!({"speaks_for":{"agent_id":"Ada"}}),
    )
    .await;
    let error = error_body(&reply);
    assert_eq!(error["detail"]["field"], "speaks_for", "{error}");
    assert_eq!(rig.records().await, before);
    rig.finish().await;
}

/// Starts the head's session with a plain send whose run finishes: a notice never creates a
/// session, so a test of a marked send first gives it a session to land in.
async fn open_head(rig: &mut Rig, plan: &Value) {
    rig.script.push(&[Step::Text("opened")]);
    let opened = send_plan(rig, 8, "opening", "open the head", Some(plan), json!({})).await;
    assert_eq!(
        opened.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&opened.body)
    );
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
}

/// The session's one message carrying `mark`, from a read or history reply's `messages`.
fn marked<'a>(messages: &'a Value, mark: &str) -> &'a Value {
    let found: Vec<&Value> = messages
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry[mark] == true || entry["row"][mark] == true)
        .collect();
    assert_eq!(found.len(), 1, "one message marked {mark}: {messages}");
    found[0]
}

async fn retry_fixture(tag: &str, marks: Value, hold: bool) -> (Rig, Value, subc_protocol::Frame) {
    let (mut rig, _) = scoped_plan_rig(tag, false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    open_head(&mut rig, &plan).await;
    rig.script.hold_model(hold);
    rig.script.push(&[Step::Text("answer")]);
    let sent = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
    assert_eq!(
        sent.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&sent.body)
    );
    if hold {
        rig.wait_entered().await;
    } else {
        rig.wait_for_finished(2).await;
        rig.wait_idle().await;
    }
    (rig, plan, sent)
}

#[tokio::test]
async fn notice_retry_without_notice_refuses_notice_without_writing() {
    let (mut rig, plan, _) =
        retry_fixture("notice-retry-diff", json!({"notice":true}), false).await;
    let before = rig.records().await;
    let reply = send_plan(
        &mut rig,
        8,
        "marked-key",
        "same words",
        Some(&plan),
        json!({}),
    )
    .await;
    let error = error_body(&reply);
    assert_eq!(error["code"], "send_id_reuse", "{error}");
    assert_eq!(error["detail"]["field"], "notice", "{error}");
    assert_eq!(rig.records().await, before);
    sample("marks-notice-reuse.json", &reply);
    rig.finish().await;
}

#[tokio::test]
async fn changed_speaks_for_retry_refuses_speaks_for_first_without_writing() {
    let (mut rig, plan, _) = retry_fixture(
        "agent-retry-diff",
        json!({"speaks_for":{"agent_id":"Ada"}}),
        false,
    )
    .await;
    for marks in [
        json!({"speaks_for":{"agent_id":"ada"}}),
        json!({"notice":true}),
    ] {
        let before = rig.records().await;
        let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "send_id_reuse", "{error}");
        assert_eq!(error["detail"]["field"], "speaks_for", "{error}");
        assert_eq!(rig.records().await, before);
        sample("marks-speaks-for-reuse.json", &reply);
    }
    rig.finish().await;
}

/// A speaker's name is part of the send's retry identity: a retry that changes or drops it
/// refuses naming `speaks_for` and writes nothing, and the same name replays.
#[tokio::test]
async fn a_changed_speaker_name_refuses_the_retry_naming_speaks_for() {
    let named = json!({"speaks_for":{"agent_id":"Ada","name":"Ada Lovelace"},"notice":true});
    let (mut rig, plan, sent) = retry_fixture("named-mark-retry", named.clone(), true).await;
    for marks in [
        json!({"speaks_for":{"agent_id":"Ada","name":"Ada King"},"notice":true}),
        json!({"speaks_for":{"agent_id":"Ada"},"notice":true}),
    ] {
        let before = rig.records().await;
        let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "send_id_reuse", "{error}");
        assert_eq!(error["detail"]["field"], "speaks_for", "{error}");
        assert_eq!(rig.records().await, before);
        sample("named-marks-reuse.json", &reply);
    }
    let before = rig.records().await;
    let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), named).await;
    assert_eq!(reply.body, sent.body);
    assert_eq!(rig.records().await, before);
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    rig.finish().await;
}

/// The owner's automatic message carrying an agent's words under a display name is stored with
/// both marks and the name, served to opted-in reads before and after a restart, and written
/// only in records that require the name reader.
#[tokio::test]
async fn a_named_agent_notice_is_stored_and_served_with_both_marks() {
    let (mut rig, _) = scoped_plan_rig("named-agent-notice", false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    open_head(&mut rig, &plan).await;
    let marks = json!({"speaks_for":{"agent_id":"agent-a","name":"Ada"},"notice":true});
    rig.script.push(&[Step::Text("answer")]);
    let sent = send_plan(
        &mut rig,
        8,
        "named-notice",
        "words of another agent",
        Some(&plan),
        marks.clone(),
    )
    .await;
    sample("named-marks-send.json", &sent);
    assert_eq!(
        sent.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&sent.body)
    );
    let run_id = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let status = rig
        .request(8, json!({"method":"run.status","params":{"run_id":run_id}}))
        .await;
    sample("named-marks-run-status.json", &status);
    assert_eq!(status.header.ty, subc_protocol::FrameType::Response);
    let read = rig
        .request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await;
    sample("named-marks-read.json", &read);
    let read = result(&read);
    let entry = marked(&read["messages"], "notice");
    assert_eq!(entry["speaks_for"], marks["speaks_for"], "{entry}");
    assert_eq!(entry["notice"], true, "{entry}");
    let records = rig.records().await;
    assert!(
        records.iter().any(|record| matches!(
            record,
            Record::RunStart(start) if start.named_speaker == Some(true)
        )),
        "the run's start records the named speaker: {records:?}"
    );
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    rig.bind_with_scope(8, Some(scope_owner()), Some(scope_stamp(false)))
        .await;
    let read = result(
        &rig.request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await,
    );
    assert_eq!(
        marked(&read["messages"], "notice")["speaks_for"],
        marks["speaks_for"]
    );
    rig.finish().await;
}

#[tokio::test]
async fn identical_marks_retry_replays_original_reply_without_writing() {
    let marks = json!({"speaks_for":{"agent_id":"Ada"}});
    let (mut rig, plan, sent) = retry_fixture("identical-mark-retry", marks.clone(), true).await;
    let before = rig.records().await;
    let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
    assert_eq!(reply.body, sent.body);
    assert_eq!(rig.records().await, before);
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    rig.finish().await;
}

#[tokio::test]
async fn notice_absent_and_false_retry_replay_original_reply_without_writing() {
    let (mut rig, plan, sent) = retry_fixture("false-mark-retry", json!({}), true).await;
    for marks in [json!({"notice":false}), json!({})] {
        let before = rig.records().await;
        let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
        assert_eq!(reply.body, sent.body);
        assert_eq!(rig.records().await, before);
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    rig.finish().await;
}

#[tokio::test]
async fn queued_marks_retries_compare_normalized_marks_without_writing() {
    let (mut rig, plan, _) = retry_fixture("queued-marks-retry", json!({}), true).await;
    for (key, marks, changed, field) in [
        (
            "queued-notice",
            json!({"notice":true}),
            json!({}),
            Some("notice"),
        ),
        (
            "queued-agent",
            json!({"speaks_for":{"agent_id":"Ada"}}),
            json!({"speaks_for":{"agent_id":"ada"}}),
            Some("speaks_for"),
        ),
        ("queued-false", json!({"notice":false}), json!({}), None),
    ] {
        let pending = send_plan(&mut rig, 8, key, "queued words", Some(&plan), marks.clone()).await;
        assert_eq!(result(&pending)["state"], "pending");
        let before = rig.records().await;
        let retry = send_plan(&mut rig, 8, key, "queued words", Some(&plan), marks).await;
        assert_eq!(retry.body, pending.body);
        let changed = send_plan(&mut rig, 8, key, "queued words", Some(&plan), changed).await;
        if let Some(field) = field {
            let error = error_body(&changed);
            assert_eq!(error["code"], "send_id_reuse");
            assert_eq!(error["detail"]["field"], field);
        } else {
            assert_eq!(changed.body, pending.body);
        }
        assert_eq!(rig.records().await, before);
    }
    rig.script.hold_model(false);
    rig.wait_idle().await;
    rig.finish().await;
}

async fn assert_malformed_notice_member(wanted: &str) {
    let (mut rig, admitter) = scoped_plan_rig(&format!("malformed-{wanted}"), false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    for (field, extra, plan) in [
        ("yield", json!({"yield":null}), plan.clone()),
        ("yield", json!({"yield":"yes"}), plan.clone()),
        ("plan.senders[0].yields", json!({}), {
            let mut plan = plan.clone();
            plan["senders"] = json!([{"module":"prefrontal-core","yields":null}]);
            plan
        }),
        ("plan.senders[0].yields", json!({}), {
            let mut plan = plan.clone();
            plan["senders"] = json!([{"module":"prefrontal-core","yields":"yes"}]);
            plan
        }),
    ] {
        if field != wanted {
            continue;
        }
        let before = rig.records().await;
        let reply = send_plan(&mut rig, 8, "malformed", "words", Some(&plan), extra).await;
        assert_eq!(
            reply.header.ty,
            subc_protocol::FrameType::Error,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        let error: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], "invalid_params");
        assert_eq!(error["detail"]["field"], field, "{error}");
        sample(&format!("malformed-{field}.after.json"), &reply);
        assert_eq!(rig.records().await, before);
    }
    assert!(admitter.calls().is_empty());
    rig.finish().await;
}

/// The scope owner's kept notice is served with `keep` wherever its marks are, and is kept
/// through a reopen; the stored body, default reads and `history_version` don't change.
#[tokio::test]
async fn a_kept_notice_is_served_kept_and_survives_reopen() {
    let (mut rig, _) = scoped_plan_rig("message-marks-keep", false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    open_head(&mut rig, &plan).await;
    rig.script.push(&[Step::Text("answer")]);
    let sent = send_plan(
        &mut rig,
        8,
        "kept-words",
        "the person's words",
        Some(&plan),
        json!({"notice":true,"keep":true}),
    )
    .await;
    sample("keep-send.json", &sent);
    assert_eq!(
        sent.header.ty,
        subc_protocol::FrameType::Response,
        "{}",
        String::from_utf8_lossy(&sent.body)
    );
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    let default = rig
        .request(8, json!({"method":"session.read","params":{}}))
        .await;
    sample("keep-default-read.json", &default);
    let default = result(&default);
    for entry in default["messages"].as_array().unwrap() {
        assert!(entry.get("keep").is_none(), "{entry}");
        assert!(entry.get("notice").is_none(), "{entry}");
    }
    let read = read_kept(&mut rig).await;
    sample("keep-read.json", &read);
    let read = result(&read);
    let kept = marked(&read["messages"], "keep");
    let ordinal = &kept["ordinal"];
    let served = default["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["ordinal"] == *ordinal)
        .expect("the default read serves the kept notice");
    assert_eq!(kept["message"], served["message"]);
    assert_eq!(read["history_version"], default["history_version"]);
    for form in ["raw", "display"] {
        let history = rig
            .request(
                8,
                json!({"method":"session.history","params":{"form":form}}),
            )
            .await;
        sample(&format!("keep-history-{form}.json"), &history);
        let history = result(&history);
        let entry = marked(&history["messages"], "keep");
        let row = if form == "display" {
            &entry["row"]
        } else {
            entry
        };
        assert_eq!(row["notice"], true, "{form}: {history}");
    }
    assert!(rig.registry().stop_and_seal().await);
    let mut rig = rig.restart().await;
    rig.bind_with_scope(8, Some(scope_owner()), Some(scope_stamp(false)))
        .await;
    read_kept(&mut rig).await;
    rig.finish().await;
}

/// An `include_display_facts` read holding the kept notice, checked as served with both marks.
async fn read_kept(rig: &mut Rig) -> subc_protocol::Frame {
    let read = rig
        .request(
            8,
            json!({"method":"session.read","params":{"include_display_facts":true}}),
        )
        .await;
    let body = result(&read);
    let entry = marked(&body["messages"], "keep");
    assert_eq!(entry["notice"], true, "{entry}");
    read
}

/// `keep` marks only a notice, and never one that may ride a tool result; each refusal writes
/// nothing.
#[tokio::test]
async fn keep_without_a_notice_or_on_a_tool_result_ride_refuses_before_any_write() {
    let (mut rig, _) = scoped_plan_rig("message-marks-keep-refused", false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    for (field, extra) in [
        ("keep", json!({"keep":true})),
        (
            "delivery",
            json!({"keep":true,"notice":true,"delivery":["steer","tool_result"]}),
        ),
    ] {
        let before = rig.records().await;
        let reply = send_plan(&mut rig, 8, "kept", "words", Some(&plan), extra).await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], field, "{error}");
        sample(&format!("keep-refused-{field}.json"), &reply);
        assert_eq!(rig.records().await, before);
    }
    rig.finish().await;
}

#[tokio::test]
async fn keep_retry_without_keep_refuses_keep_without_writing() {
    let (mut rig, plan, _) =
        retry_fixture("keep-retry-diff", json!({"notice":true,"keep":true}), false).await;
    let before = rig.records().await;
    let reply = send_plan(
        &mut rig,
        8,
        "marked-key",
        "same words",
        Some(&plan),
        json!({"notice":true}),
    )
    .await;
    let error = error_body(&reply);
    assert_eq!(error["code"], "send_id_reuse", "{error}");
    assert_eq!(error["detail"]["field"], "keep", "{error}");
    assert_eq!(rig.records().await, before);
    sample("keep-reuse.json", &reply);
    rig.finish().await;
}

#[tokio::test]
async fn malformed_notice_yield_refuses_before_any_write() {
    assert_malformed_notice_member("yield").await;
}

#[tokio::test]
async fn malformed_notice_yield_grant_refuses_before_any_write() {
    assert_malformed_notice_member("plan.senders[0].yields").await;
}

#[tokio::test]
async fn notice_yield_checks_a_first_send_before_fetch_and_can_start_idle() {
    let (mut rig, admitter) = scoped_plan_rig("notice-yield-first", false).await;
    let mut plan = vector("plans/broca-head-no-compaction.json");
    let before = rig.records().await;
    for delivery in [json!("steer"), json!(["idle_start", "steer"])] {
        let reply = send_plan(
            &mut rig,
            8,
            "urgent",
            "words",
            Some(&plan),
            json!({"yield":true, "delivery":delivery}),
        )
        .await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], "yield", "{error}");
        assert!(admitter.calls().is_empty());
        assert_eq!(rig.records().await, before);
        plan["senders"] = json!([{"module":"prefrontal-core","yields":true}]);
    }
    rig.script.push(&[Step::Text("answer")]);
    let accepted = send_plan(
        &mut rig,
        8,
        "urgent",
        "words",
        Some(&plan),
        json!({"yield":true,"delivery":"steer"}),
    )
    .await;
    assert_eq!(result(&accepted)["state"], "active");
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

#[tokio::test]
async fn send_without_undeclared_notice_members_preserves_legacy_reply_bytes() {
    let mut rig = Rig::start("ordinary-notice-member-bytes").await;
    rig.script.hold_model(true);
    let reply = rig.send("ordinary", "ordinary words", None).await;
    sample("unmarked-send.json", &reply);
    let body = result(&reply);
    let expected = format!(
        "{{\"result\":{{\"state\":\"active\",\"run_id\":{}{}}}}}",
        serde_json::to_string(body["run_id"].as_str().unwrap()).unwrap(),
        crate::delivery_rig::ADMITTING_BASELINE,
    );
    assert_eq!(reply.body.as_slice(), expected.as_bytes());
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

fn work_plan() -> Value {
    let composition = json!({"providers":[{"provider":"fixture","tools":[{"name":"work","capabilities":[]}]}],"step_transform_items":[]});
    json!({"tool_items":[{"provider":"fixture","params":{}}],"system_text_items":[],"step_transform_items":[],"composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

#[tokio::test]
async fn marks_follow_steered_queued_and_riding_sends_into_reader_facts() {
    for delivery in ["steer", "queue", "tool_result"] {
        let (mut rig, _) = scoped_plan_rig(&format!("marks-delivery-{delivery}"), false).await;
        rig.script.hold_tool(true);
        rig.script.push(&[
            Step::Call("carrier"),
            Step::Text("done"),
            Step::Text("queued done"),
        ]);
        let opened = send_plan(&mut rig, 8, "open", "open", Some(&work_plan()), json!({})).await;
        assert_eq!(
            opened.header.ty,
            subc_protocol::FrameType::Response,
            "{}",
            String::from_utf8_lossy(&opened.body)
        );
        rig.wait_entered().await;
        let marks = if delivery == "steer" {
            json!({"notice":true})
        } else {
            json!({"speaks_for":{"agent_id":"Ada"}})
        };
        let mut extra = marks.clone();
        extra["delivery"] = if delivery == "tool_result" {
            json!(["tool_result"])
        } else {
            json!(delivery)
        };
        let sent = send_plan(&mut rig, 8, "marked", "marked continuation", None, extra).await;
        assert_eq!(
            sent.header.ty,
            subc_protocol::FrameType::Response,
            "{}",
            String::from_utf8_lossy(&sent.body)
        );
        assert!(rig.records().await.iter().any(|record| match record {
            Record::Steer(steer) if delivery == "steer" => {
                steer.message_marks.as_ref() == Some(&marks)
            }
            Record::QueuedSend(send) if delivery != "steer" => {
                send.message_marks.as_ref() == Some(&marks)
            }
            _ => false,
        }));
        rig.script.hold_tool(false);
        rig.wait_for_finished(if delivery == "queue" { 2 } else { 1 })
            .await;
        rig.wait_idle().await;
        let default = result(
            &rig.request(8, json!({"method":"session.read","params":{}}))
                .await,
        );
        let facts = result(
            &rig.request(
                8,
                json!({"method":"session.read","params":{"include_display_facts":true}}),
            )
            .await,
        );
        assert_eq!(default["history_version"], facts["history_version"]);
        for (plain, facts) in default["messages"]
            .as_array()
            .unwrap()
            .iter()
            .zip(facts["messages"].as_array().unwrap())
        {
            assert_eq!(plain["message"], facts["message"]);
            assert!(plain.get("speaks_for").is_none());
            assert!(plain.get("notice").is_none());
        }
        if delivery == "tool_result" {
            let notice = facts["messages"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|entry| entry["notices"].as_array().into_iter().flatten())
                .find(|notice| notice["text"] == "marked continuation")
                .expect("the riding send retains its own agent attribution");
            assert_eq!(notice["speaks_for"], marks["speaks_for"]);
            assert!(notice.get("notice").is_none());
            let frames = rig
                .subscribe_collect_until(
                    20,
                    json!({"from":"start"}),
                    std::time::Duration::from_millis(20),
                    |frame| {
                        serde_json::from_slice::<serde_json::Value>(&frame.body)
                            .is_ok_and(|value| value["event"]["type"] == "run_finished")
                    },
                )
                .await;
            let event = frames
                .iter()
                .filter_map(|frame| serde_json::from_slice::<serde_json::Value>(&frame.body).ok())
                .find(|value| value["event"]["type"] == "notice_delivered")
                .map(|value| value["event"].clone())
                .expect("the riding send emits its durable delivery event");
            if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.cortexkit/internal-docs/runner-0821-samples/notice-delivered.after.json");
                std::fs::write(path, serde_json::to_vec(&event).unwrap()).unwrap();
            }
            // llm-runner contract 9.7.2: notice_delivered omits delivered_as for tool_result.
            assert!(event.get("delivered_as").is_none(), "{event}");
            assert_eq!(event["speaks_for"], marks["speaks_for"]);
        } else {
            let entry = facts["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["message"]["content"][0]["text"] == "marked continuation")
                .unwrap();
            for (field, expected) in marks.as_object().unwrap() {
                assert_eq!(&entry[field], expected, "{delivery}: {field}");
            }
        }
        rig.finish().await;
    }
}

#[tokio::test]
async fn non_owner_and_automated_routes_cannot_carry_agent_words_without_writes() {
    let (mut rig, _) = scoped_plan_rig("marks-carrier-refusals", false).await;
    let sent = send_plan(&mut rig, 8, "open", "open", Some(&work_plan()), json!({})).await;
    assert_eq!(sent.header.ty, subc_protocol::FrameType::Response);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    let carrier = subc_protocol::Principal::Reserved {
        module_id: "named-sender".into(),
    };
    for (channel, principal, stamp) in [
        (9, carrier, scope_stamp(false)),
        (10, scope_owner(), scope_stamp(true)),
        (11, scope_owner(), agent_run_stamp()),
    ] {
        rig.bind_with_scope(channel, Some(principal), Some(stamp))
            .await;
        let before = rig.records().await;
        let reply = send_plan(
            &mut rig,
            channel,
            "refused",
            "refuse",
            None,
            json!({"speaks_for":{"agent_id":"Ada"}}),
        )
        .await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], "speaks_for", "{error}");
        assert_eq!(rig.records().await, before);
        // Nor can such a route keep a notice: only the scope owner marks the person's words.
        let reply = send_plan(
            &mut rig,
            channel,
            "refused-keep",
            "refuse",
            None,
            json!({"notice":true,"keep":true}),
        )
        .await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], "keep", "{error}");
        assert_eq!(rig.records().await, before);
    }
    rig.finish().await;
}
