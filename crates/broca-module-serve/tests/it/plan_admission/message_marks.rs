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
    for (index, (marks, field)) in [
        (
            json!({"speaks_for":{"agent_id":"Ada"},"notice":true}),
            "notice",
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
        let before = rig.wal_bytes();
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
            sample("marks-conflict.json", &reply);
        }
        assert_eq!(error["code"], "invalid_params", "{error}");
        assert_eq!(error["detail"]["field"], field, "{error}");
        assert_eq!(rig.wal_bytes(), before);
    }
    rig.finish().await;
    let mut rig = Rig::start("unscoped-agent-words").await;
    let before = rig.wal_bytes();
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
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

async fn retry_fixture(tag: &str, marks: Value, hold: bool) -> (Rig, Value, subc_protocol::Frame) {
    let (mut rig, _) = scoped_plan_rig(tag, false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
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
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
    }
    (rig, plan, sent)
}

#[tokio::test]
async fn notice_retry_without_notice_refuses_notice_without_writing() {
    let (mut rig, plan, _) =
        retry_fixture("notice-retry-diff", json!({"notice":true}), false).await;
    let before = rig.wal_bytes();
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
    assert_eq!(rig.wal_bytes(), before);
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
        let before = rig.wal_bytes();
        let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
        let error = error_body(&reply);
        assert_eq!(error["code"], "send_id_reuse", "{error}");
        assert_eq!(error["detail"]["field"], "speaks_for", "{error}");
        assert_eq!(rig.wal_bytes(), before);
        sample("marks-speaks-for-reuse.json", &reply);
    }
    rig.finish().await;
}

#[tokio::test]
async fn identical_marks_retry_replays_original_reply_without_writing() {
    let marks = json!({"speaks_for":{"agent_id":"Ada"}});
    let (mut rig, plan, sent) = retry_fixture("identical-mark-retry", marks.clone(), true).await;
    let before = rig.wal_bytes();
    let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
    assert_eq!(reply.body, sent.body);
    assert_eq!(rig.wal_bytes(), before);
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    rig.finish().await;
}

#[tokio::test]
async fn notice_absent_and_false_retry_replay_original_reply_without_writing() {
    let (mut rig, plan, sent) = retry_fixture("false-mark-retry", json!({}), true).await;
    for marks in [json!({"notice":false}), json!({})] {
        let before = rig.wal_bytes();
        let reply = send_plan(&mut rig, 8, "marked-key", "same words", Some(&plan), marks).await;
        assert_eq!(reply.body, sent.body);
        assert_eq!(rig.wal_bytes(), before);
    }
    rig.script.hold_model(false);
    rig.wait_for_finished(1).await;
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
        let before = rig.wal_bytes();
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
        assert_eq!(rig.wal_bytes(), before);
    }
    rig.script.hold_model(false);
    rig.wait_idle().await;
    rig.finish().await;
}

async fn assert_undeclared_notice_member(wanted: &str) {
    let (mut rig, admitter) = scoped_plan_rig(&format!("undeclared-{wanted}"), false).await;
    let plan = vector("plans/broca-head-no-compaction.json");
    for (field, extra, plan) in [
        ("keep", json!({"keep":false}), plan.clone()),
        ("keep", json!({"keep":true,"notice":true}), plan.clone()),
        ("yield", json!({"yield":false}), plan.clone()),
        ("yield", json!({"yield":true}), plan.clone()),
        ("plan.senders[0].yields", json!({}), {
            let mut plan = plan.clone();
            plan["senders"] = json!([{"module":"prefrontal-core","yields":false}]);
            plan
        }),
        ("plan.senders[0].yields", json!({}), {
            let mut plan = plan.clone();
            plan["senders"] = json!([{"module":"prefrontal-core","yields":true}]);
            plan
        }),
    ] {
        if field != wanted {
            continue;
        }
        let before = std::fs::read(&rig.wal_path).unwrap_or_default();
        let reply = send_plan(&mut rig, 8, "undeclared", "words", Some(&plan), extra).await;
        assert_eq!(
            reply.header.ty,
            subc_protocol::FrameType::Error,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        let error: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(error["code"], "invalid_params");
        assert_eq!(error["detail"]["field"], field, "{error}");
        sample(&format!("undeclared-{field}.after.json"), &reply);
        assert_eq!(std::fs::read(&rig.wal_path).unwrap_or_default(), before);
    }
    assert!(admitter.calls().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn undeclared_keep_refuses_before_any_write() {
    assert_undeclared_notice_member("keep").await;
}

#[tokio::test]
async fn undeclared_yield_refuses_before_any_write() {
    assert_undeclared_notice_member("yield").await;
}

#[tokio::test]
async fn undeclared_plan_yields_refuses_before_any_write() {
    assert_undeclared_notice_member("plan.senders[0].yields").await;
}

#[tokio::test]
async fn undeclared_tag_selector_names_tag_before_lookup_without_writing() {
    let mut rig = Rig::start("undeclared-tag-selector").await;
    let before = std::fs::read(&rig.wal_path).unwrap_or_default();
    let reply = rig
        .request(
            7,
            json!({"method":"session.read","params":{"tag":1,"call_key":"no-call"}}),
        )
        .await;
    assert_eq!(reply.header.ty, subc_protocol::FrameType::Error);
    let error: Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "tag", "{error}");
    sample("undeclared-tag.after.json", &reply);
    assert_eq!(std::fs::read(&rig.wal_path).unwrap_or_default(), before);
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
        assert!(rig.records().iter().any(|record| match record {
            broca_wal::WalRecord::Steered {
                message_marks: Some(stored),
                ..
            } if delivery == "steer" => serde_json::to_value(stored).unwrap() == marks,
            broca_wal::WalRecord::Queued {
                message_marks: Some(stored),
                ..
            } if delivery != "steer" => serde_json::to_value(stored).unwrap() == marks,
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
            let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
            let records = broca_core::WalStore::read_records(&wal, &rig.session).unwrap();
            let projected =
                broca_core::project_control_with_mids(&records, &mut Default::default()).unwrap();
            let event = projected
                .iter()
                .find_map(|placed| match &placed.event {
                    cortexkit_role_llm_runner::live::LiveEvent::NoticeDelivered(notice) => {
                        Some(notice)
                    }
                    _ => None,
                })
                .expect("the riding send emits its durable delivery event");
            if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.cortexkit/internal-docs/runner-0821-samples/notice-delivered.after.json");
                std::fs::write(path, serde_json::to_vec(event).unwrap()).unwrap();
            }
            // llm-runner contract 9.7.2: notice_delivered omits delivered_as for tool_result.
            assert_eq!(event.delivered_as, None);
            assert_eq!(
                serde_json::to_value(&event.speaks_for).unwrap(),
                marks["speaks_for"]
            );
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
        let before = rig.wal_bytes();
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
        assert_eq!(rig.wal_bytes(), before);
    }
    rig.finish().await;
}
