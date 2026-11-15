use super::*;

#[tokio::test]
async fn run_awaits_sign_in_names_only_the_frozen_account_label() {
    for store in [false, true] {
        let side = Side::new(
            store,
            Cipher::None,
            vec![super::super::super::tests::text_step("m1", "done")],
        )
        .await;
        side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
        side.executor.signed_out.store(true, Ordering::SeqCst);
        retry(&side).await;
        let body = crate::store_actor::awaits_tests::prove(
            &side.registry,
            &side.key,
            if store {
                "auth-required.after"
            } else {
                "auth-required.before"
            },
            store.then_some(("paused", "person", Some("auth_required"))),
        )
        .await;
        if store {
            assert_eq!(
                body["awaits"]["person"],
                json!({"action":"sign_in", "on":"synthetic"})
            );
        }
        assert!(side.registry.stop_and_seal().await);
    }
}

pub(super) fn selection() -> broca_wal::AuthSelection {
    serde_json::from_value(
        json!({"credential_id":"oauth:stub:synthetic", "account_ref":"synthetic-account"}),
    )
    .unwrap()
}

impl HeldExecutor {
    pub(super) fn sign_in_error(&self) -> Option<broca_protocol::ProviderError> {
        self.signed_out.load(Ordering::SeqCst).then(|| {
            serde_json::from_value(json!({
                "class":"auth", "message":"Sign in to the synthetic account again",
                "provider_code":"needs_reauth", "credential_id":"oauth:stub:synthetic"
            }))
            .unwrap()
        })
    }
}

#[tokio::test]
async fn run_awaits_default_account_uses_only_cached_email() {
    let side = Side::new(
        true,
        Cipher::None,
        vec![super::super::super::tests::text_step("m1", "done")],
    )
    .await;
    *side.executor.auth_selection.lock().unwrap() =
        Some(serde_json::from_value(json!({"credential_id":"oauth:stub"})).unwrap());
    side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
    side.executor.signed_out.store(true, Ordering::SeqCst);
    retry(&side).await;
    let body = crate::store_actor::awaits_tests::prove(
        &side.registry,
        &side.key,
        "auth-default-email.after",
        Some(("paused", "person", Some("auth_required"))),
    )
    .await;
    assert_eq!(body["awaits"]["person"]["on"], "default@example.invalid");
    assert!(side.registry.stop_and_seal().await);
}

async fn retry(side: &Side) {
    let mut req = request("hello");
    req.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    assert!(matches!(
        side.registry
            .route_submission_with_order(
                &side.key,
                RunId("r1".into()),
                req,
                RunKind::FreshOrResume,
                SendKey::SendId("sign-in-send".into()),
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    side.idle().await;
}

async fn compare_reads(sides: &[Side]) {
    alike!(
        comparable(sides[1].status().await),
        comparable(sides[0].status().await)
    );
    let normalize_result = |mut value: Value| {
        value.as_object_mut().unwrap().remove("awaits");
        if let Some(message) = value
            .get_mut("final_message")
            .and_then(Value::as_object_mut)
        {
            message.remove("mid");
        }
        value
    };
    alike!(
        normalize_result(sides[1].result().await),
        normalize_result(sides[0].result().await)
    );
    alike!(head(&sides[1]).await, head(&sides[0]).await);
    let old_records = record_meaning(&sides[0]).await;
    let new_records = record_meaning(&sides[1]).await;
    assert_eq!(new_records, old_records);
    assert_eq!(
        new_records
            .iter()
            .filter(|record| record.get("start").is_some())
            .count(),
        1
    );
    let old = super::super::streams::routed(&sides[0].registry, &sides[0].key).await;
    let new = super::super::streams::routed(&sides[1].registry, &sides[1].key).await;
    super::super::streams::assert_alike(&new, &old);
}

async fn head(side: &Side) -> Value {
    let head = side
        .registry
        .head_for(&side.key, None, &broca_protocol::SendScope::default())
        .await
        .unwrap();
    let mut value = serde_json::to_value(head).unwrap();
    if let Some(last) = value
        .get_mut("last_run_state")
        .and_then(Value::as_object_mut)
    {
        last.remove("awaits");
        if last.get("reason") == Some(&json!("tool_held")) {
            last.insert("state".into(), json!("active"));
        }
    }
    let object = value.as_object_mut().unwrap();
    for field in ["incarnation", "history_version", "head"] {
        object.remove(field);
    }
    if let Some(time) = object.get_mut("updated_at") {
        *time = json!(!time.is_null());
    }
    value
}

async fn assert_paused(side: &Side, calls: usize) {
    let status = side.status().await;
    assert_eq!(status["state"], "paused");
    assert_eq!(status["reason"], "auth_required");
    assert_eq!(status["auth_selection"], "oauth:stub:synthetic");
    assert_eq!(side.result().await["state"], "paused");
    assert_eq!(
        head(side).await["last_run_state"],
        json!({"run_id":"r1", "state":"paused", "reason":"auth_required"})
    );
    assert_eq!(side.executor.provider.inner.calls().len(), calls);
}

async fn continue_after_sign_in(sides: &[Side], calls: usize) {
    for side in sides {
        side.executor.signed_out.store(false, Ordering::SeqCst);
        retry(side).await;
        assert_eq!(side.status().await["state"], "completed");
        assert_eq!(side.result().await["final_message"]["text"], "done");
        assert_eq!(side.executor.provider.inner.calls().len(), calls + 1);
    }
    compare_reads(sides).await;
    for side in sides {
        assert!(side.registry.stop_and_seal().await);
    }
}

cipher_tests!(
    a_run_starts_paused_for_sign_in,
    a_held_resume_pauses_for_sign_in
);

async fn a_run_starts_paused_for_sign_in(cipher: Cipher) {
    let mut sides = Vec::new();
    for store in [false, true] {
        let side = Side::new(
            store,
            cipher,
            vec![super::super::super::tests::text_step("m1", "done")],
        )
        .await;
        side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
        side.executor.signed_out.store(true, Ordering::SeqCst);
        retry(&side).await;
        assert_paused(&side, 0).await;
        // An unresolved retry must not call the provider or append another pause/resume.
        retry(&side).await;
        assert_paused(&side, 0).await;
        sides.push(side);
    }
    compare_reads(&sides).await;
    continue_after_sign_in(&sides, 0).await;
}

async fn a_held_resume_pauses_for_sign_in(cipher: Cipher) {
    let mut sides = Vec::new();
    for store in [false, true] {
        let initial = Side::new(
            store,
            cipher,
            vec![super::super::super::tests::read_step("m1")],
        )
        .await;
        initial
            .executor
            .sign_in_fixture
            .store(true, Ordering::SeqCst);
        retry(&initial).await;
        let side = initial.restarted().await;
        side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
        side.executor.signed_out.store(true, Ordering::SeqCst);
        let status = side.status().await;
        let key = status["held_calls"][0]["call_key"].as_str().unwrap();
        let synced = side.executor.slots.deposit(
            &side.key,
            &ToolCallId("c1".into()),
            key,
            ToolResult {
                tool_call_id: ToolCallId("c1".into()),
                output: ToolOutput::Text {
                    text: "late answer".into(),
                },
                is_error: false,
            },
        );
        side.registry
            .resume_for_late_result(&side.key)
            .await
            .unwrap();
        broca_test_support::wait_for("signed-out held result durable", synced, || {
            "late result not synced".into()
        })
        .await
        .unwrap();
        side.idle().await;
        assert_paused(&side, 0).await;
        assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
        assert!(side.status().await["held_calls"]
            .as_array()
            .is_none_or(Vec::is_empty));
        assert!(initial.registry.stop_and_seal().await);
        sides.push(side);
    }
    compare_reads(&sides).await;
    continue_after_sign_in(&sides, 0).await;
    for side in &sides {
        let calls = side.executor.provider.inner.calls();
        assert!(serde_json::to_string(&calls[0])
            .unwrap()
            .contains("late answer"));
        assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn a_restart_resume_pauses_for_sign_in() {
    let mut sides = Vec::new();
    for store in [false, true] {
        let initial = Side::new(
            store,
            Cipher::None,
            vec![super::super::super::tests::auth_failure()],
        )
        .await;
        initial
            .executor
            .sign_in_fixture
            .store(true, Ordering::SeqCst);
        *initial.executor.pause_as.lock().unwrap() = Some(broca_protocol::PauseReason::Restart);
        retry(&initial).await;
        let side = initial.restarted().await;
        side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
        side.executor.signed_out.store(true, Ordering::SeqCst);
        side.registry.handle().enable_restart_resumes_for_test();
        let actor = side.registry.actor_for(&side.key).await.unwrap();
        let (reply, answer) = tokio::sync::oneshot::channel();
        assert!(actor
            .send(crate::actor::LineageCmd::ResumeRestart { reply })
            .await
            .is_ok());
        answer.await.unwrap().unwrap();
        side.idle().await;
        assert_paused(&side, 0).await;
        assert!(initial.registry.stop_and_seal().await);
        sides.push(side);
    }
    compare_reads(&sides).await;
    continue_after_sign_in(&sides, 0).await;
}

async fn legacy_held_auth_fixture() -> Side {
    let initial = Side::new(
        false,
        Cipher::None,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    initial
        .executor
        .sign_in_fixture
        .store(true, Ordering::SeqCst);
    retry(&initial).await;
    let side = initial.restarted().await;
    side.executor.sign_in_fixture.store(true, Ordering::SeqCst);
    side.executor.signed_out.store(true, Ordering::SeqCst);
    let status = side.status().await;
    let key = status["held_calls"][0]["call_key"].as_str().unwrap();
    let synced = side.executor.slots.deposit(
        &side.key,
        &ToolCallId("c1".into()),
        key,
        ToolResult {
            tool_call_id: ToolCallId("c1".into()),
            output: ToolOutput::Text {
                text: "late answer".into(),
            },
            is_error: false,
        },
    );
    side.registry
        .resume_for_late_result(&side.key)
        .await
        .unwrap();
    broca_test_support::wait_for("legacy held terminal synced before sign-in", synced, || {
        "result not synced".into()
    })
    .await
    .unwrap();
    side.idle().await;
    assert_paused(&side, 0).await;
    assert!(initial.registry.stop_and_seal().await);
    side
}

fn cut_after_held_terminal(side: &Side) {
    assert!(side.registry.slot_state(&side.key).is_none());
    let path = side.executor.wal.wal_path(&side.key);
    let bytes = std::fs::read(&path).unwrap();
    let mut offset = 0;
    loop {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                offset += consumed;
                if matches!(framed.record, broca_wal::WalRecord::ToolResult { .. }) {
                    std::fs::write(path, &bytes[..offset]).unwrap();
                    break;
                }
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            _ => panic!("no held terminal in the copied log"),
        }
    }
}

async fn drive_legacy_copy(side: &Side, signed_out: bool) -> RunSummary {
    let writer =
        broca_core::WalHandle::spawn(side.executor.wal.open(&side.key).unwrap(), 1).unwrap();
    let mut req = broca_core::RunRequest::from_submission(
        request("hello"),
        RunId("r1".into()),
        side.key.clone(),
        broca_protocol::SubmissionId("synthetic".into()),
    );
    req.submission_id = None;
    let (out, mut events) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
    side.executor.signed_out.store(signed_out, Ordering::SeqCst);
    let summary = broca_core::run_loop_on_writer(
        req,
        &writer,
        if signed_out {
            StartMode::FreshOrResume
        } else {
            StartMode::ResumeAfterAuth
        },
        &side.executor.provider,
        side.executor.tools.as_ref(),
        &AllowAll,
        side.executor.wal.as_ref(),
        None,
        broca_core::RunControl {
            pause_for_auth_at_start: side.executor.sign_in_error(),
            auth_selection: Some(selection()),
            provider_held_lifecycle: Some(side.executor.slots.clone()),
            ..Default::default()
        },
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    drop(out);
    drain.await.unwrap();
    summary
}

#[tokio::test]
async fn legacy_held_sign_in_recovers_a_cut_after_terminal_sync() {
    let source = legacy_held_auth_fixture().await;
    let copy = source.restarted().await;
    cut_after_held_terminal(&copy);
    assert_eq!(
        drive_legacy_copy(&copy, true).await.paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    assert!(copy.executor.provider.inner.calls().is_empty());
    assert_eq!(
        drive_legacy_copy(&copy, false).await.reason,
        broca_protocol::RunFinishReason::Completed
    );
    let records = copy.executor.wal.read_records(&copy.key).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|frame| matches!(frame.record, broca_wal::WalRecord::ToolResult { .. }))
            .count(),
        1
    );
    assert_eq!(
        copy.executor.tools.0.load(Ordering::SeqCst),
        1,
        "the original dispatch is never repeated"
    );
    assert!(source.registry.stop_and_seal().await);
}

#[tokio::test]
async fn legacy_held_sign_in_preserves_request_bytes_with_a_synced_result() {
    use broca_core::provider::Provider;
    let source = legacy_held_auth_fixture().await;
    let paused = source.restarted().await;
    let synced = source.restarted().await;
    // Without the new auth-at-start branch, a result already synced before continuation
    // reaches the normal batch recovery. Compare that path to the auth-pause continuation.
    cut_after_held_terminal(&synced);
    for side in [&paused, &synced] {
        assert_eq!(
            drive_legacy_copy(side, false).await.reason,
            broca_protocol::RunFinishReason::Completed
        );
    }
    let request = |side: &Side| {
        let calls = side.executor.provider.inner.calls();
        assert_eq!(calls.len(), 1);
        let bytes = side.executor.provider.render_request(&calls[0]);
        assert!(String::from_utf8_lossy(&bytes).contains("late answer"));
        bytes
    };
    assert_eq!(request(&paused), request(&synced));
    assert!(source.registry.stop_and_seal().await);
}

async fn record_meaning(side: &Side) -> Vec<Value> {
    let mut meaning = Vec::new();
    if side.store {
        use broca_session_store::record::Record;
        let view = side
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap();
        for (_, record) in view.records_from(0).await.unwrap() {
            let item = match record {
                Record::RunStart(start) => json!({"start":start.run_id}),
                Record::Pause(pause) => json!({"pause":pause.reason, "error":pause.error}),
                Record::Resumed(resume) => json!({"resume":resume.run_id}),
                Record::ToolReply(terminal) => json!({"result":terminal.result}),
                Record::BatchEnd(_) => json!({"batch_end":true}),
                Record::RunEnd(end) => json!({"end":end.reason, "error":end.error}),
                Record::AuthRefrozen(_) => panic!("sign-in must not change the frozen account"),
                _ => continue,
            };
            meaning.push(item);
        }
    } else {
        use broca_wal::WalRecord;
        for frame in side.executor.wal.read_records(&side.key).unwrap() {
            let item = match frame.record {
                WalRecord::RunStarted { run_id, .. } => json!({"start":run_id}),
                WalRecord::RunPaused { reason, error, .. } => {
                    json!({"pause":reason, "error":error})
                }
                WalRecord::ResumeStarted { episode, .. } => json!({"resume":episode}),
                WalRecord::ToolResult { result, .. } => json!({"result":result}),
                WalRecord::ToolBatchFinished { .. } => json!({"batch_end":true}),
                WalRecord::RunFinished { reason, error, .. } => {
                    json!({"end":reason, "error":error})
                }
                WalRecord::RunAuthRefrozen { .. } => {
                    panic!("sign-in must not change the frozen account")
                }
                _ => continue,
            };
            meaning.push(item);
        }
    }
    meaning
}
