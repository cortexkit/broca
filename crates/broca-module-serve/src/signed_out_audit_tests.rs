fn expired_token(account: &str) -> Value {
    let mut reply = token(account, true);
    reply["result"]["expires_at_ms"] = json!(now_ms() - 1);
    reply
}

fn versioned_token(account: &str, version: u64) -> Value {
    use base64::Engine as _;
    let mut reply = token(account, false);
    reply["result"]["record_version"] = json!(version);
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(json!({"chatgpt_account_id":account}).to_string());
    reply["result"]["payload"] =
        json!(format!("e30.{claims}.test-signature-{version}").into_bytes());
    reply
}

fn vault_failure(code: &str) -> Value {
    json!({"result":{"error":{"class":if code == "not_found" { "permanent" } else { "transient" },"code":code}}})
}

async fn wait_questions(rig: &SignedOutRig, count: usize) {
    broca_test_support::wait_until("signed-out routing exchange", || async {
        let actual = rig.routing.questions().len();
        if actual >= count {
            Ok(())
        } else {
            Err(format!("got {actual}, want {count}"))
        }
    })
    .await;
}

async fn seed_provider_auth_pause(rig: &SignedOutRig) -> (&'static MemoryBackend, StoreSession) {
    let (backend, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    (backend, session)
}

fn set_offered_metadata(rig: &SignedOutRig, provider: &str, method: &str) {
    let vault = rig.executor.vault.as_ref().unwrap();
    change_offered_metadata(&vault.listing, provider, method);
}

fn change_offered_metadata(
    cache: &broca_module_serve::account_routing::ListingCache,
    provider: &str,
    method: &str,
) {
    let (listing, _) = cache.get().unwrap();
    let mut listing = (*listing).clone();
    let row = listing
        .credentials
        .iter_mut()
        .find(|row| row.id == CRED_B)
        .unwrap();
    row.provider_ids = vec![provider.into()];
    row.auth_method = Some(method.into());
    cache.store(listing);
}

#[tokio::test]
async fn rejected_auth_with_unavailable_vault_or_same_version_never_moves() {
    for code in ["refresh_failed", "timeout", "same_version"] {
        let rig = SignedOutRig::new(
            &format!("rejected-uncertain-{code}"),
            false,
            None,
            vec![offer()],
        )
        .await;
        rig.script_a(if code == "same_version" {
            vec![token(A, false)]
        } else {
            vec![expired_token(A), vault_failure(code)]
        });
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_signed_out_question(&rig.routing, 0);
        assert_eq!(*rig.sent.lock().unwrap(), [A]);
        assert_eq!(
            session.operations().run.as_ref().unwrap().paused,
            Some(broca_protocol::PauseReason::AuthRequired)
        );
    }
}

#[tokio::test]
async fn rejected_auth_moves_only_after_needs_reauth_or_rejected_newer_token() {
    for newer in [false, true] {
        let rig = SignedOutRig::new(
            &format!("rejected-definite-{newer}"),
            false,
            None,
            vec![offer()],
        )
        .await;
        rig.script_a(vec![
            token(A, false),
            if newer {
                versioned_token(A, 2)
            } else {
                needs_reauth()
            },
        ]);
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_eq!(
            session.operations().run.as_ref().unwrap().finished,
            Some(broca_protocol::RunFinishReason::Completed),
            "newer={newer}; sent={:?}; questions={:?}; error={:?}; vault={:?}",
            rig.sent.lock().unwrap(),
            rig.routing.questions(),
            session.operations().run.as_ref().unwrap().end_error,
            rig.daemon.requests()
        );
        assert_signed_out_question(&rig.routing, 1);
        assert_eq!(
            *rig.sent.lock().unwrap(),
            if newer { vec![A, A, B] } else { vec![A, B] }
        );
    }
}

#[tokio::test]
async fn a_definite_post_rejection_signal_keeps_the_original_provider_error() {
    let rig = SignedOutRig::new(
        "post-rejection-error-verbatim",
        false,
        None,
        vec![answer(
            json!({"kind":"refuse","routing_code":"pin_unavailable","detail":{}}),
        )],
    )
    .await;
    rig.script_a(vec![token(A, false), needs_reauth()]);
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.paused, Some(broca_protocol::PauseReason::AuthRequired));
    let error = run.end_error.as_ref().unwrap();
    assert_eq!(error["status"], 401);
    assert_eq!(error["provider_code"], "token_expired");
    assert_eq!(error["account_continue"]["routing_code"], "pin_unavailable");
    assert_signed_out_question(&rig.routing, 1);
}

#[tokio::test]
async fn initial_routed_needs_reauth_keeps_routings_account_and_moves() {
    let rig = SignedOutRig::new("initial-routed-needs-reauth", true, None, vec![offer()]).await;
    rig.script_a(vec![needs_reauth()]);
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_signed_out_question(&rig.routing, 1);
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert!(!records(&session)
        .await
        .iter()
        .any(|(_, record)| matches!(record, Record::Pause(_))));
}

#[tokio::test]
async fn undeclared_or_unknown_support_does_not_spend_the_pauses_exchange() {
    for missing in ["unknown", "account_continue_signed_out"] {
        let rig = SignedOutRig::new(
            &format!("capability-can-recover-{missing}"),
            true,
            Some(missing),
            vec![offer()],
        )
        .await;
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, 0);
        *rig.routing.missing.lock().unwrap() = None;
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_signed_out_question(&rig.routing, 1);
        assert_eq!(
            session.operations().run.as_ref().unwrap().finished,
            Some(broca_protocol::RunFinishReason::Completed)
        );
    }
}

#[tokio::test]
async fn a_locally_rejected_offer_does_not_spend_the_pause() {
    let rig = SignedOutRig::new(
        "local-offer-can-recover",
        true,
        None,
        vec![offer(), offer()],
    )
    .await;
    rig.daemon.record.lock().unwrap().credential_replies.insert(
        CRED_B.into(),
        VecDeque::from([token("wrong-account", false)]),
    );
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 1);
    assert!(rig.sent.lock().unwrap().is_empty());
    rig.daemon
        .record
        .lock()
        .unwrap()
        .credential_replies
        .insert(CRED_B.into(), VecDeque::from([token(B, false)]));
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_signed_out_question(&rig.routing, 2);
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

#[tokio::test]
async fn offered_metadata_must_match_the_frozen_provider_and_method() {
    for (provider, method) in [("anthropic", "chatgpt"), ("openai", "apikey")] {
        let rig = SignedOutRig::new(
            &format!("offered-type-{provider}-{method}"),
            true,
            None,
            vec![offer(), offer()],
        )
        .await;
        let listing = rig.executor.vault.as_ref().unwrap().listing.clone();
        *rig.routing.before_continue.lock().unwrap() = Some(Box::new(move || {
            change_offered_metadata(&listing, provider, method)
        }));
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert!(
            rig.sent.lock().unwrap().is_empty(),
            "incompatible offered credential was sent"
        );
        assert!(!rig
            .daemon
            .scoped_reads()
            .iter()
            .any(|params| params["credential_id"] == CRED_B));
        set_offered_metadata(&rig, "openai", "chatgpt");
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_signed_out_question(&rig.routing, 2);
        assert_eq!(
            session.operations().run.as_ref().unwrap().finished,
            Some(broca_protocol::RunFinishReason::Completed)
        );
    }
}

#[tokio::test]
async fn a_usage_wall_refuses_incompatible_offered_metadata() {
    let rig = SignedOutRig::new("quota-offered-type", false, None, vec![offer()]).await;
    rig.status_a.store(429, Ordering::SeqCst);
    let listing = rig.executor.vault.as_ref().unwrap().listing.clone();
    *rig.routing.before_continue.lock().unwrap() = Some(Box::new(move || {
        change_offered_metadata(&listing, "openai", "apikey")
    }));
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(run.finished, Some(broca_protocol::RunFinishReason::Error));
    assert_eq!(
        run.end_error.as_ref().unwrap()["provider_code"],
        "account_limit_exhausted"
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A]);
    assert!(!rig
        .daemon
        .scoped_reads()
        .iter()
        .any(|params| params["credential_id"] == CRED_B));
}

#[tokio::test]
async fn actor_open_only_probes_and_never_resumes_the_original_account() {
    for permanent in [false, true] {
        let rig = SignedOutRig::new(
            &format!("open-is-probe-{permanent}"),
            false,
            Some("account_continue_signed_out"),
            vec![offer()],
        )
        .await;
        let (backend, session) = seed_provider_auth_pause(&rig).await;
        let original_error = session.operations().run.as_ref().unwrap().end_error.clone();
        drop(session);
        *rig.routing.missing.lock().unwrap() = None;
        if permanent {
            rig.script_a(vec![
                json!({"result":{"error":{"class":"permanent","code":"not_found"}}}),
            ]);
        }
        let (commands, task) = rig.actor(rig.reopen(backend).await);
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        let run = session.operations().run.as_ref().unwrap();
        assert_eq!(
            run.finished, None,
            "an open-time admission error must not end the pause"
        );
        assert_eq!(run.paused, Some(broca_protocol::PauseReason::AuthRequired));
        assert_eq!(run.end_error, original_error);
        assert_eq!(*rig.sent.lock().unwrap(), [A]);
        assert_signed_out_question(&rig.routing, 0);
        assert!(!records(&session)
            .await
            .iter()
            .any(|(_, record)| matches!(record, Record::Resumed(_))));
    }
}

#[tokio::test]
async fn a_failed_open_admission_does_not_reserve_the_next_credential_recheck() {
    let rig = SignedOutRig::new(
        "open-admission-can-recover",
        false,
        Some("account_continue_signed_out"),
        vec![offer()],
    )
    .await;
    let (backend, session) = seed_provider_auth_pause(&rig).await;
    drop(session);
    *rig.routing.missing.lock().unwrap() = None;
    rig.script_a(vec![vault_failure("refresh_failed")]);
    let (commands, task) = rig.actor(rig.reopen(backend).await);
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 0);
    rig.script_a(vec![needs_reauth()]);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_signed_out_question(&rig.routing, 1);
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

#[tokio::test]
async fn a_timed_hold_survives_a_failed_admission_and_rearms_its_timer() {
    held_recheck_survives_admission("refresh_failed").await;
}

#[tokio::test]
async fn a_timed_hold_survives_a_permanent_admission_failure() {
    held_recheck_survives_admission("not_found").await;
}

async fn held_recheck_survives_admission(code: &str) {
    let until = now_ms() + 3_000;
    let rig = SignedOutRig::new(&format!("hold-admission-can-recover-{code}"), true, None, vec![answer(json!({"kind":"hold","routing_code":"no_compatible_account","detail":{},"until_ms":until}))]).await;
    let (backend, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 1);
    rig.script_a(vec![vault_failure(code)]);
    broca_test_support::wait_until("hold wake reached its vault read", || async {
        if rig.daemon.scoped_reads().len() >= 3 {
            Ok(())
        } else {
            Err("waiting for due recheck".into())
        }
    })
    .await;
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 1);
    let view = Session::view(
        backend,
        broca_session_store::backend::SessionAddr("0123456789abcdef".into()),
        &rig.triple(),
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
    )
    .await
    .unwrap();
    assert_eq!(
        view.operations.run.as_ref().unwrap().finished,
        None,
        "automatic failed admission must preserve its hold"
    );
    rig.script_a(vec![needs_reauth()]);
    rig.routing.replies.lock().unwrap().push_back(offer());
    wait_questions(&rig, 2).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_signed_out_question(&rig.routing, 2);
}

#[tokio::test]
async fn a_stale_hold_has_a_thirty_second_reask_floor() {
    let rig = SignedOutRig::new("stale-hold-floor", true, None, vec![answer(json!({"kind":"hold","routing_code":"no_compatible_account","detail":{},"until_ms":now_ms()-1})), offer()]).await;
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    wait_questions(&rig, 2).await;
    let questions = rig.routing.questions();
    assert!(
        questions[1].0 >= questions[0].0 + 30_000,
        "past hold hot-looped: {questions:?}"
    );
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
}

#[tokio::test]
async fn cancelling_an_open_time_sign_in_ask_ends_cancelled() {
    let rig = SignedOutRig::new(
        "cancel-open-question",
        false,
        Some("account_continue_signed_out"),
        vec![],
    )
    .await;
    let (backend, session) = seed_provider_auth_pause(&rig).await;
    drop(session);
    *rig.routing.missing.lock().unwrap() = None;
    rig.routing.pending.store(true, Ordering::SeqCst);
    rig.script_a(vec![needs_reauth()]);
    let (commands, task) = rig.actor(rig.reopen(backend).await);
    wait_questions(&rig, 1).await;
    let (reply, response) = tokio::sync::oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: broca_protocol::RunId("signed-out-run".into()),
            at: broca_records::record::RunCancelAt::Now,
            reply,
        })
        .unwrap();
    response.await.unwrap();
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Cancelled)
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A]);
}

#[tokio::test]
async fn a_pause_lift_move_never_reconsiders_the_account_it_left_in_that_step() {
    let back_to_a = answer(
        json!({"kind":"offer","credential_id":CRED_A,"account":A,"decision_id":"decision-c","valid_until_ms":now_ms()+60_000}),
    );
    let rig = SignedOutRig::new(
        "pause-lift-refused-set",
        false,
        Some("account_continue_signed_out"),
        vec![offer(), back_to_a],
    )
    .await;
    let (backend, session) = seed_provider_auth_pause(&rig).await;
    drop(session);
    *rig.routing.missing.lock().unwrap() = None;
    rig.script_a(vec![needs_reauth()]);
    rig.status_b.store(401, Ordering::SeqCst);
    rig.daemon.record.lock().unwrap().credential_replies.insert(
        CRED_B.into(),
        VecDeque::from([token(B, false), needs_reauth()]),
    );
    let (commands, task) = rig.actor(rig.reopen(backend).await);
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(rig.routing.questions().len(), 2);
    let a_reads = rig
        .daemon
        .scoped_reads()
        .iter()
        .filter(|params| params["credential_id"] == CRED_A)
        .count();
    assert_eq!(
        a_reads, 3,
        "the offered account A must be refused before re-reading its credential"
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A, B]);
    assert_eq!(
        session.operations().run.as_ref().unwrap().paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
}

#[tokio::test]
async fn a_definite_rejection_cannot_authorize_a_later_steps_uncertain_rejection() {
    let rig = SignedOutRig::new("definite-fact-is-step-local", false, None, vec![offer()]).await;
    rig.script_a(vec![token(A, false), needs_reauth()]);
    rig.status_b.store(401, Ordering::SeqCst);
    let tool_events = [
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"read-one","name":"read","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}),
        json!({"type":"response.completed","response":{"id":"response-tool","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}),
    ];
    rig.scripted_b.lock().unwrap().push_back(
        tool_events
            .iter()
            .map(|event| {
                format!(
                    "event: {}\ndata: {event}\n\n",
                    event["type"].as_str().unwrap()
                )
            })
            .collect(),
    );
    let auth = rig
        .executor
        .decide_auth("openai", None, true, Some(AuthMethod::Chatgpt), None)
        .await
        .unwrap();
    let options = broca_catalog::live::ResolveOptions {
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        auth_method: Some(AuthMethod::Chatgpt),
        account_id: auth.account_id.as_deref(),
        frozen_account_ref: Some(A),
        session_id: Some("step-local-auth"),
        secret_refresher: auth
            .in_run_refresh
            .as_ref()
            .map(|refresh| refresh.refresher.clone()),
        ..Default::default()
    };
    let resolved = resolve_provider_for_episode(
        "openai/gpt-5",
        rig.executor.provider_overrides.base_url("openai"),
        Some(128),
        &auth,
        &options,
        build_provider_http_client(),
        Default::default(),
    )
    .unwrap();
    resolved
        .provider
        .set_chatgpt_https_policy(Arc::new(|| true));
    let mut request = drift_request(&rig.root);
    request.run_id = broca_protocol::RunId("signed-out-run".into());
    request.model = broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-5".into(),
    };
    request.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "read then answer".into(),
        }],
    )];
    request.tools = vec![serde_json::from_value(
        json!({"name":"read","description":"read one item","input_schema":{"type":"object"}}),
    )
    .unwrap()];
    request.policy_allowed.insert("read".into());
    let selection = serde_json::from_value(json!({"credential_id":CRED_A,"account_ref":A,"resolved_credential_id":CRED_A,"auth_method":"chatgpt",
        "account_decision":{"path":"routed","admission_id":"signed-out-run","decision_id":"decision-a","conversation_key":"broca-session:signed-out"}})).unwrap();
    let control = broca_core::RunControl {
        auth_selection: Some(selection),
        ..Default::default()
    };
    let (_, mut session) = rig.session().await;
    let mut parts = rig.parts.clone();
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &resolved.provider,
        session.operations(),
        Some(now_ms() as u64),
    )
    .unwrap();
    let tools = broca_core::StubToolPlane::new().with_canned_text("read", "one item");
    let moves = AccountMoves::new(&rig.executor, &request, None, None);
    let now = || now_ms() as u64;
    let seams = broca_session_loop::Seams {
        steer_yields: None,
        provider: &resolved.provider,
        tools: &tools,
        bind: request.session.clone(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: control.auth_selection.clone(),
        step_progress: None,
        accepting_drain: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: Some(&moves),
    };
    let (finished, events) = broca_test_support::capture_logs(broca_session_loop::run(
        &mut session,
        &mut parts,
        admission,
        &seams,
    ))
    .await;
    let finished = finished.unwrap();
    assert_eq!(
        finished.paused,
        Some(broca_protocol::PauseReason::AuthRequired),
        "finished={finished:?}; error={:?}; sent={:?}; questions={:?}",
        session.operations().run.as_ref().unwrap().end_error,
        rig.sent.lock().unwrap(),
        rig.routing.questions()
    );
    assert_eq!(
        session.operations().run.as_ref().unwrap().completed_steps,
        1
    );
    assert_signed_out_question(&rig.routing, 1);
    assert_eq!(*rig.sent.lock().unwrap(), [A, B, B]);
    let error = session
        .operations()
        .run
        .as_ref()
        .unwrap()
        .end_error
        .as_ref()
        .unwrap();
    assert_eq!(error["status"], 401);
    assert_eq!(error["provider_code"], "token_expired");
    assert!(events.iter().any(
        |event| event.target == "accounts" && event.message().contains("sign-in continuation")
    ));
}

struct QueueBoundaryRouting {
    inner: Arc<SignedOutRouting>,
    writable: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for QueueBoundaryRouting {
    async fn capability(
        &self,
        name: &str,
    ) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        broca_module_serve::account_routing::RoutingTransport::capability(&*self.inner, name).await
    }
    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        broca_module_serve::account_routing::RoutingTransport::request(&*self.inner, body).await
    }
    async fn request_where_declared(
        &self,
        body: Vec<u8>,
        _names: &[&str],
        started: &std::sync::atomic::AtomicBool,
    ) -> Option<broca_module_serve::account_routing::RoutingReply> {
        if !self.writable.load(Ordering::SeqCst) {
            return Some(Err("writer queue unavailable".into()));
        }
        started.store(true, Ordering::Release);
        Some(self.request(body).await)
    }
}

#[tokio::test]
async fn only_a_dispatched_unanswered_continue_consumes_the_pause() {
    for dispatched in [false, true] {
        let mut rig = SignedOutRig::new(
            &format!("dispatch-boundary-{dispatched}"),
            true,
            None,
            vec![offer()],
        )
        .await;
        let writable = Arc::new(std::sync::atomic::AtomicBool::new(dispatched));
        rig.routing.pending.store(dispatched, Ordering::SeqCst);
        let transport = Arc::new(QueueBoundaryRouting {
            inner: rig.routing.clone(),
            writable: writable.clone(),
        });
        Arc::get_mut(&mut rig.executor).unwrap().routing = Some(Arc::new(
            broca_module_serve::account_routing::RoutingClient::new(transport, Default::default()),
        ));
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, usize::from(dispatched));
        writable.store(true, Ordering::SeqCst);
        rig.routing.pending.store(false, Ordering::SeqCst);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        assert_signed_out_question(&rig.routing, 1);
        if dispatched {
            assert_eq!(
                session.operations().run.as_ref().unwrap().paused,
                Some(broca_protocol::PauseReason::AuthRequired)
            );
            assert!(rig.sent.lock().unwrap().is_empty());
        } else {
            assert_eq!(
                session.operations().run.as_ref().unwrap().finished,
                Some(broca_protocol::RunFinishReason::Completed)
            );
            assert_eq!(*rig.sent.lock().unwrap(), [B]);
        }
    }
}
