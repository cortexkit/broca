#[tokio::test]
async fn a_noop_open_probe_never_reports_the_healthy_token_as_rejected() {
    let rig = SignedOutRig::new("probe-rejection-provenance", false, None, vec![]).await;
    let (backend, session) = seed_provider_auth_pause(&rig).await;
    drop(session);
    rig.script_a(vec![versioned_token(A, 2)]);
    for _ in 0..2 {
        let (commands, task) = rig.actor(rig.reopen(backend).await);
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_eq!(session.operations().run.as_ref().unwrap().paused, Some(broca_protocol::PauseReason::AuthRequired));
        assert_eq!(*rig.sent.lock().unwrap(), [A]);
        let reports: Vec<_> = rig.daemon.requests().into_iter().filter(|(method, _)| method == "credential.report_auth_failure").collect();
        assert_eq!(reports.len(), 1, "only the original execution sent a rejected token");
        assert!(reports.iter().all(|(_, params)| params["record_version"] == 1), "probe reported a token it never sent: {reports:?}");
        assert_eq!(rig.executor.vault.as_ref().unwrap().cache.get(CRED_A).map(|token| token.record_version), Some(2));
        assert!(!records(&session).await.iter().any(|(_, record)| matches!(record, Record::Resumed(_))));
        drop(session);
    }
}

#[tokio::test]
async fn a_rejected_resend_reports_the_version_that_attempt_sent() {
    let rig = SignedOutRig::new("sent-version-report", false, Some("account_continue_signed_out"), vec![]).await;
    rig.script_a(vec![versioned_token(A, 1), versioned_token(A, 2)]);
    let (_, session) = seed_provider_auth_pause(&rig).await;
    let reported: Vec<_> = rig.daemon.requests().into_iter().filter(|(method, _)| method == "credential.report_auth_failure")
        .map(|(_, params)| params["record_version"].as_u64().unwrap()).collect();
    assert_eq!(reported, [1, 2]);
    assert_eq!(*rig.sent.lock().unwrap(), [A, A]);
    assert_eq!(rig.executor.vault.as_ref().unwrap().cache.get(CRED_A).map(|token| token.record_version), None);
    assert_eq!(session.operations().run.as_ref().unwrap().paused, Some(broca_protocol::PauseReason::AuthRequired));
}

#[tokio::test]
async fn daemon_pre_delivery_refusals_do_not_consume_the_pause() {
    for code in ["module_warming", "module_reloading", "target_unavailable", "routing_unavailable"] {
        let daemon_refusal = Ok(Err(serde_json::to_vec(&json!({"code":code,"message":"routing did not receive this request"})).unwrap()));
        let rig = SignedOutRig::new(&format!("daemon-before-routing-{code}"), true, None, vec![daemon_refusal, offer()]).await;
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, 1);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_signed_out_question(&rig.routing, 2);
        assert_eq!(session.operations().run.as_ref().unwrap().finished, Some(broca_protocol::RunFinishReason::Completed), "{code}");
        assert_eq!(*rig.sent.lock().unwrap(), [B]);
    }
}

#[tokio::test]
async fn an_open_time_question_rebuilds_the_unfinished_steps_refused_accounts() {
    let refusal = answer(json!({"kind":"refuse","routing_code":"pin_unavailable","detail":{}}));
    let rig = SignedOutRig::new("resume-refused-history", true, None, vec![offer(), refusal]).await;
    rig.status_b.store(401, Ordering::SeqCst);
    rig.daemon.record.lock().unwrap().credential_replies.insert(CRED_B.into(), VecDeque::from([token(B, false), needs_reauth()]));
    let (backend, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(session.operations().run.as_ref().unwrap().paused, Some(broca_protocol::PauseReason::AuthRequired));
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
    let before = rig.daemon.scoped_reads().iter().filter(|params| params["credential_id"] == CRED_A).count();
    drop(session);
    rig.script_a(vec![token(A, false)]);
    rig.routing.replay_b.store(true, Ordering::SeqCst);
    rig.routing.replies.lock().unwrap().push_back(answer(json!({"kind":"offer","credential_id":CRED_A,"account":A,"decision_id":"decision-c","valid_until_ms":now_ms()+60_000})));
    let (commands, task) = rig.actor(rig.reopen(backend).await);
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(rig.routing.questions().len(), 3);
    let after = rig.daemon.scoped_reads().iter().filter(|params| params["credential_id"] == CRED_A).count();
    assert_eq!(after, before, "A was already refused in this unfinished step");
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
    assert_eq!(session.operations().run.as_ref().unwrap().paused, Some(broca_protocol::PauseReason::AuthRequired));
    let question = &rig.routing.questions()[2].1;
    assert_eq!(question["params"]["previous_decision_id"], "decision-b");
    assert_eq!(question["params"]["exhausted"]["account"], B);
}
