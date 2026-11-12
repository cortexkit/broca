// Moving a run to another account: the executor's decisions that don't need a vault.

/// A resumed run asks routing for its account again only when it moved, its resume can record
/// a change of account (a sign-in or restart pause), and routing serves continuation (it
/// declares both `account_continue` and `account_continue_resume`).
#[test]
fn only_a_moved_run_resumed_against_a_router_that_answers_moved_runs_asks_again() {
    for moved in [false, true] {
        for records in [false, true] {
            for serves in [false, true] {
                assert_eq!(
                    resume_asks_routing(moved, records, serves),
                    moved && records && serves,
                    "moved {moved}, records a change {records}, serves {serves}"
                );
            }
        }
    }
}

/// A resumed moved run that routing gives another account renders a persona naming that
/// account; the same account, or a config without a persona, is left as recorded.
#[test]
fn a_resumed_runs_persona_follows_the_account_routing_gave_it() {
    let session = broca_protocol::BindIdentity::new("/project", "harness", "s1");
    let old = "00000000-0000-4000-8000-00000000000a";
    let new = "00000000-0000-4000-8000-00000000000c";
    let recorded = broca_module_serve::claude_code_identity::fresh_persona(
        "install",
        Some(old),
        "cred-a",
        &session,
        Some("abc".into()),
        2,
    )
    .unwrap();
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    frozen.claude_code_persona = Some(recorded.clone());
    let selection = |account: &str, credential: &str| -> broca_core::AuthSelection {
        serde_json::from_value(serde_json::json!({
            "credential_id": credential,
            "account_ref": account,
            "auth_method": "oauth",
        }))
        .unwrap()
    };

    let mut same = frozen.clone();
    persona_for_resumed_account(
        &mut same,
        Some("install"),
        Some(old),
        Some(&selection(old, "cred-a")),
        &session,
    )
    .unwrap();
    assert_eq!(same.claude_code_persona.as_ref(), Some(&recorded));

    let mut moved = frozen.clone();
    persona_for_resumed_account(
        &mut moved,
        Some("install"),
        Some(old),
        Some(&selection(new, "cred-c")),
        &session,
    )
    .unwrap();
    let expected = broca_module_serve::claude_code_identity::moved_persona(
        &recorded,
        "install",
        Some(new),
        "cred-c",
        &session,
    )
    .unwrap();
    let persona = moved.claude_code_persona.unwrap();
    assert_eq!(persona.account_uuid.as_deref(), Some(new));
    assert_eq!(persona.device_id, expected.device_id);
    assert_ne!(persona.device_id, recorded.device_id);
    assert_eq!(persona.session_id, recorded.session_id);

    // The refusal-fallback model's config carries the source's persona, and follows it.
    let mut with_fallback = frozen.clone();
    let mut fallback = broca_provider::families::anthropic_frozen_config("claude-fallback");
    fallback.claude_code_persona = Some(recorded.clone());
    with_fallback.refusal_fallback = Some(Box::new(broca_provider::wire::RefusalFallbackConfig {
        model: broca_protocol::request::ModelHandle::new("anthropic", "claude-fallback"),
        context_limit: None,
        frozen: fallback,
    }));
    persona_for_resumed_account(
        &mut with_fallback,
        Some("install"),
        Some(old),
        Some(&selection(new, "cred-c")),
        &session,
    )
    .unwrap();
    let source = with_fallback.claude_code_persona.clone().unwrap();
    assert_eq!(source.account_uuid.as_deref(), Some(new));
    assert_eq!(source.device_id, persona.device_id);
    assert_eq!(
        with_fallback
            .refusal_fallback
            .as_ref()
            .unwrap()
            .frozen
            .claude_code_persona
            .as_ref(),
        Some(&source),
        "the fallback renders with the same persona as its source"
    );

    let mut without = broca_provider::families::anthropic_frozen_config("claude-test");
    without.claude_code_persona = None;
    persona_for_resumed_account(
        &mut without,
        Some("install"),
        Some(old),
        Some(&selection(new, "cred-c")),
        &session,
    )
    .unwrap();
    assert!(without.claude_code_persona.is_none());

    let mut no_install = frozen.clone();
    assert!(persona_for_resumed_account(
        &mut no_install,
        None,
        Some(old),
        Some(&selection(new, "cred-c")),
        &session,
    )
    .is_err());
}

/// A moved run's resume that asked routing and got no usable answer (no answer in the budget,
/// routing unreachable, a hold, a refusal or a conflict) keeps the account it recorded: the
/// admission goes on as a resume that never asked. A fresh run's refusal still ends it.
#[test]
fn a_resume_routing_gives_no_account_keeps_the_recorded_one() {
    let run_id = broca_protocol::RunId("resumed".into());
    let refusal = |routing_code: &str| {
        RunError::AccountSelectionRefused(broca_core::AccountSelectionRefusal {
            routing_code: routing_code.into(),
            retry_after_s: None,
            resets_at_ms: None,
            routing_detail: None,
            decision_id: None,
            class: broca_protocol::ErrorClass::Transient,
        })
    };
    for error in [
        RunError::RoutingUnavailable(broca_core::RoutingUnavailable::no_decision_within_budget(
            1_500,
        )),
        RunError::RoutingUnavailable(broca_core::RoutingUnavailable::unreachable()),
        refusal("account_hold"),
        refusal("account_pinned"),
        refusal("admission_conflict"),
    ] {
        let mut asks = true;
        let choice = resumed_choice(Err(error), &mut asks, &run_id);
        assert!(matches!(choice, Ok(AccountChoice::NotApplicable)));
        assert!(!asks, "nothing after follows an account routing didn't give");
    }
    let mut fresh = false;
    assert!(matches!(
        resumed_choice(Err(refusal("account_pinned")), &mut fresh, &run_id),
        Err(RunError::AccountSelectionRefused(_))
    ));
    let mut asks = true;
    assert!(matches!(
        resumed_choice(Ok(AccountChoice::NotApplicable), &mut asks, &run_id),
        Ok(AccountChoice::NotApplicable)
    ));
    assert!(asks, "an answer routing gave is taken");
}

/// A move's recorded render config takes the new account's persona in its own
/// `claude_code_persona` and in its refusal-fallback model's, when that carries one, and keeps
/// every other byte.
#[test]
fn a_refrozen_persona_reaches_the_refusal_fallbacks_config() {
    let old = serde_json::json!({"account_uuid": "a"});
    let new = serde_json::json!({"account_uuid": "b"});
    let mut config = serde_json::json!({
        "claude_code_persona": old,
        "wire_model_id": "claude-test",
        "refusal_fallback": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-fallback"},
            "frozen": {"claude_code_persona": old, "wire_model_id": "claude-fallback"},
        },
    });
    refreeze_persona(&mut config, new.clone());
    assert_eq!(
        config,
        serde_json::json!({
            "claude_code_persona": new,
            "wire_model_id": "claude-test",
            "refusal_fallback": {
                "model": {"provider_module_id": "anthropic", "model_id": "claude-fallback"},
                "frozen": {"claude_code_persona": new, "wire_model_id": "claude-fallback"},
            },
        })
    );
    // A config without a fallback gains none.
    let mut plain = serde_json::json!({"claude_code_persona": old});
    refreeze_persona(&mut plain, new.clone());
    assert_eq!(plain, serde_json::json!({"claude_code_persona": new}));
}

/// Routing that declares both continuation capabilities and offers one account at every wall.
struct OffersAccount {
    credential_id: &'static str,
    account: &'static str,
    requests: std::sync::Mutex<Vec<serde_json::Value>>,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for OffersAccount {
    async fn capability(&self, _name: &str) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        broca_module_serve::routing_capabilities::CapabilitySupport::Declared
    }

    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        let valid_until_ms = epoch_millis().max(0) as u64 + 300_000;
        Ok(Ok(serde_json::to_vec(&serde_json::json!({"result": {
            "kind": "offer",
            "credential_id": self.credential_id,
            "account": self.account,
            "decision_id": "decision-b",
            "valid_until_ms": valid_until_ms,
        }}))
        .unwrap()))
    }
}

/// A ChatGPT run on `chatgpt-a` hits its weekly wall; routing offers `chatgpt:openai:second`
/// for `chatgpt-b`, and the vault serves that credential's token for `vault_account`. What the
/// executor's side of the move answers the loop, and what it asked routing and the vault.
async fn a_chatgpt_wall_offered_b(
    label: &str,
    vault_account: &'static str,
) -> (
    broca_session_loop::account_move::Answer<Box<dyn broca_core::provider::Provider>>,
    Vec<serde_json::Value>,
    Vec<(String, serde_json::Value)>,
) {
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_expiring(
        &root,
        ExpiryScript {
            account_id: Some(vault_account),
            ..ExpiryScript::default()
        },
    )
    .await;
    let mut executor = executor_with_capturing_vault(&root, &daemon).await;
    let routing = Arc::new(OffersAccount {
        credential_id: "chatgpt:openai:second",
        account: "chatgpt-b",
        requests: Default::default(),
    });
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("chatgpt-run".into());
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-6.1".into(),
    };
    let selection: broca_core::AuthSelection = serde_json::from_value(serde_json::json!({
        "credential_id": "chatgpt:openai",
        "account_ref": "chatgpt-a",
        "resolved_credential_id": "chatgpt:openai",
        "auth_method": "chatgpt",
        "account_decision": {
            "path": "routed",
            "admission_id": "chatgpt-run",
            "decision_id": "decision-a",
            "conversation_key": "broca-session:chatgpt",
        },
    }))
    .unwrap();
    let frozen = broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-6.1");
    let config: broca_wal::RunConfig = serde_json::from_value(serde_json::json!({
        "model": req.model,
        "tools": [],
        "tool_choice": broca_protocol::request::ToolChoice::default(),
        "generation": req.generation,
        "response_format": broca_protocol::request::ResponseFormat::default(),
        "context_epoch": 0,
        "renderer_version": frozen.renderer_version,
        "frozen_render_config": frozen,
        "auth_selection": selection,
    }))
    .unwrap();
    let refused = vec!["chatgpt-a".to_owned()];
    let now = epoch_millis().max(0) as u64;
    let wall = broca_session_loop::account_move::Wall {
        run_id: "chatgpt-run",
        step_id: 2,
        selection: &selection,
        credential_id: &selection.credential_id,
        account: "chatgpt-a",
        limit_window: "seven_day",
        resets_at_ms: Some(now + 3 * 86_400_000),
        retry_after_s: None,
        observed_at_ms: now,
        attempt_seq: 7,
        refused_accounts: &refused,
        renderer_version: config.renderer_version,
        render_config: &config.frozen_render_config,
        run_config: &config,
        reask: false,
    };
    let moves = AccountMoves::new(&executor, &req, &None, None, None);
    let answer =
        broca_session_loop::account_move::AccountContinue::ask(&moves, &wall).await;
    let asked = routing.requests.lock().unwrap().clone();
    let vault = daemon.requests();
    drop(moves);
    drop(executor);
    daemon.assert_healthy();
    (answer, asked, vault)
}

/// Through the executor: an offer whose credential the vault serves for the offered account is
/// a move to it, under the offer's decision, with a provider built for it; routing was asked
/// once, with the run's own admission and the walled account.
#[tokio::test]
async fn the_executor_moves_to_an_offer_the_vault_serves_for_the_offered_account() {
    let (answer, asked, vault) = a_chatgpt_wall_offered_b("move-offer-b", "chatgpt-b").await;
    let broca_session_loop::account_move::Answer::Move(moved) = answer else {
        panic!("the offer is taken");
    };
    assert_eq!(moved.account, "chatgpt-b");
    assert_eq!(moved.selection.credential_id, "chatgpt:openai:second");
    assert_eq!(moved.selection.account_ref.as_deref(), Some("chatgpt-b"));
    assert_eq!(
        moved
            .selection
            .account_decision
            .as_ref()
            .and_then(|decision| decision.decision_id.as_deref()),
        Some("decision-b")
    );
    assert_eq!(asked.len(), 1, "{asked:#?}");
    assert_eq!(asked[0]["method"], "route.account_for");
    assert_eq!(asked[0]["params"]["mode"], "continue");
    assert_eq!(asked[0]["params"]["admission_id"], "chatgpt-run");
    assert_eq!(asked[0]["params"]["previous_decision_id"], "decision-a");
    assert_eq!(asked[0]["params"]["exhausted"]["account"], "chatgpt-a");
    assert!(vault
        .iter()
        .any(|(method, params)| method == "credential.get_scoped"
            && params["credential_id"] == "chatgpt:openai:second"));
}

/// Through the executor: an offer whose credential the vault serves for another account than
/// the offer names is refused, so the run ends at its wall as before.
#[tokio::test]
async fn the_executor_refuses_an_offer_whose_token_names_another_account() {
    let (answer, _, _) = a_chatgpt_wall_offered_b("move-offer-wrong", "chatgpt-wrong").await;
    assert!(matches!(
        answer,
        broca_session_loop::account_move::Answer::Stay(None)
    ));
}

/// Routing answering a resumed run's repeated admission with `reply`, and counting the
/// questions.
struct AnswersAdmission {
    reply: Result<serde_json::Value, serde_json::Value>,
    asked: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for AnswersAdmission {
    async fn capability(&self, _name: &str) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        broca_module_serve::routing_capabilities::CapabilitySupport::Declared
    }

    async fn request(&self, _body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(match &self.reply {
            Ok(body) => Ok(serde_json::to_vec(body).unwrap()),
            Err(body) => Err(serde_json::to_vec(body).unwrap()),
        })
    }
}

/// Admits the resume of a ChatGPT run that moved to `chatgpt-a` at a wall and is paused for a
/// restart (or, with `records_change` false, resumes where it can't record a change), against
/// routing that declares both continuation capabilities and answers `reply`. The vault serves
/// tokens for `vault_account`. The admitted selection and how often routing was asked.
async fn a_moved_chatgpt_resume(
    label: &str,
    vault_account: &'static str,
    records_change: bool,
    reply: Result<serde_json::Value, serde_json::Value>,
) -> (Result<broca_core::AuthSelection, String>, usize) {
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_expiring(
        &root,
        ExpiryScript {
            account_id: Some(vault_account),
            ..ExpiryScript::default()
        },
    )
    .await;
    let mut executor = executor_with_capturing_vault(&root, &daemon).await;
    let listing = serde_json::from_value(serde_json::json!({
        "credentials": [
            {"id": "chatgpt:openai", "type": "chatgpt", "categories": ["openai"], "serves": ["openai"],
             "state": "active", "record_version": 1, "operations": ["read"], "account_id": "chatgpt-a",
             "auth_method": "chatgpt", "provider_ids": ["openai"]},
            {"id": "chatgpt:openai:second", "type": "chatgpt", "categories": ["openai"], "serves": ["openai"],
             "state": "active", "record_version": 1, "operations": ["read"], "account_id": "chatgpt-b",
             "auth_method": "chatgpt", "provider_ids": ["openai"]},
        ],
        "grants": 1, "grant_tuples": [], "view": "moved-resume",
    }))
    .unwrap();
    executor.vault.as_ref().unwrap().listing.store(listing);
    let routing = Arc::new(AnswersAdmission {
        reply,
        asked: Default::default(),
    });
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("chatgpt-run".into());
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-6.1".into(),
    };
    let frozen = broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-6.1");
    let config: broca_wal::RunConfig = serde_json::from_value(serde_json::json!({
        "model": req.model,
        "tools": [],
        "tool_choice": broca_protocol::request::ToolChoice::default(),
        "generation": req.generation,
        "response_format": broca_protocol::request::ResponseFormat::default(),
        "context_epoch": 0,
        "renderer_version": frozen.renderer_version,
        "frozen_render_config": frozen,
        "auth_selection": {
            "credential_id": "chatgpt:openai",
            "account_ref": "chatgpt-a",
            "resolved_credential_id": "chatgpt:openai",
            "auth_method": "chatgpt",
            "account_decision": {
                "path": "routed",
                "admission_id": "chatgpt-run",
                "decision_id": "decision-a",
                "conversation_key": "broca-session:chatgpt",
            },
        },
    }))
    .unwrap();
    let history = EpisodeHistory {
        prior_run: Some(PriorRunView {
            run_id: req.run_id.clone(),
            config: config.clone(),
            idle: false,
            moved: true,
            records_account_change: records_change,
        }),
        last_config: Some(&config),
        head_seq: 1,
        persona: PersonaHistory::Store {
            first_user: None,
            first_user_sealed: false,
            human_prompts: 0,
        },
    };
    let mut queued_service_tier = None;
    let admitted = executor
        .admit_episode(
            EpisodeJob {
                req: &req,
                kind: RunKind::FreshOrResume,
                auth: &None,
                conversation_key: &None,
                queued_service_tier: &mut queued_service_tier,
                media_profile: &None,
                clock: None,
                new_storage: true,
                continue_offer: None,
                routing_serves_continuation: true,
            },
            &history,
        )
        .await
        .map_err(|error| format!("{error}"))
        .and_then(|admitted| {
            assert!(!admitted.fresh_episode, "a resume");
            admitted
                .resolution
                .selection
                .ok_or_else(|| "no selection".to_owned())
        });
    let asked = routing.asked.load(std::sync::atomic::Ordering::SeqCst);
    drop(executor);
    daemon.assert_healthy();
    (admitted, asked)
}

fn chosen(account: &str, credential: &str) -> serde_json::Value {
    serde_json::json!({"result": {
        "account": {"provider": "openai", "method": "chatgpt", "account_id": account, "credential_id": credential},
        "reason": {"decision_id": format!("decision-{account}")},
    }})
}

/// Through the executor's admission: a moved run's restart resume against routing that answers
/// moved runs with their current account asks routing and takes the account it names.
#[tokio::test]
async fn a_moved_runs_resume_takes_the_account_routing_names_now() {
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-routed",
        "chatgpt-b",
        true,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
    )
    .await;
    let selection = selection.unwrap();
    assert_eq!(asked, 1);
    assert_eq!(selection.credential_id, "chatgpt:openai:second");
    assert_eq!(selection.account_ref.as_deref(), Some("chatgpt-b"));
}

/// Through the executor's admission: when routing gives the moved run's resume no account (a
/// hold, here), the resume keeps the account it recorded instead of failing.
#[tokio::test]
async fn a_moved_runs_resume_keeps_its_account_when_routing_holds_it() {
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-hold",
        "chatgpt-a",
        true,
        Err(serde_json::json!({"code": "account_hold", "message": "held",
            "detail": {"until_ms": 4_102_444_800_000u64}})),
    )
    .await;
    let selection = selection.unwrap();
    assert_eq!(asked, 1);
    assert_eq!(selection.credential_id, "chatgpt:openai");
    assert_eq!(selection.account_ref.as_deref(), Some("chatgpt-a"));
}

/// Through the executor's admission: a moved run resuming where it can't record a change of
/// account (not a sign-in or restart pause) doesn't ask routing, and keeps its account.
#[tokio::test]
async fn a_moved_runs_resume_that_cant_record_a_change_does_not_ask() {
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-held",
        "chatgpt-a",
        false,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
    )
    .await;
    assert_eq!(asked, 0);
    assert_eq!(selection.unwrap().account_ref.as_deref(), Some("chatgpt-a"));
}
