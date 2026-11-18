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

/// A moved run's resume that asked routing and got no account routing chose (no answer in the
/// budget, routing unreachable, a hold, a refusal, a conflict, a sign-in for every candidate, or
/// a static pick) keeps the account it recorded: the admission reads that account and its
/// decision from the run's record, as a resume that doesn't ask routing does. A fresh run's
/// refusal still ends it, and an account routing chose is taken.
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
    let decision: broca_core::AccountDecision = serde_json::from_value(serde_json::json!({
        "path": "routed", "admission_id": "resumed", "decision_id": "decision-b",
    }))
    .unwrap();
    for unusable in [
        AccountChoice::NotApplicable,
        AccountChoice::Static(decision.clone()),
        AccountChoice::AuthRequired(decision.clone()),
    ] {
        let mut asks = true;
        assert!(matches!(
            resumed_choice(Ok(unusable), &mut asks, &run_id),
            Ok(AccountChoice::NotApplicable)
        ));
        assert!(!asks, "only an account routing chose is taken");
    }
    let mut asks = true;
    let routed = AccountChoice::Routed {
        account_id: "chatgpt-b".into(),
        credential_id: "chatgpt:openai:second".into(),
        method: AuthMethod::Chatgpt,
        decision,
        view_digest: String::new(),
        service_tier_chosen: None,
    };
    assert!(matches!(
        resumed_choice(Ok(routed), &mut asks, &run_id),
        Ok(AccountChoice::Routed { .. })
    ));
    assert!(asks, "an account routing chose is taken");
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

/// Routing that declares `account_continue` and `account_continue_resume`, and offers the same
/// account whenever a run asks to continue after its account's usage window was spent.
struct OffersAccount {
    credential_id: &'static str,
    account: &'static str,
    /// How long routing takes to answer.
    answer_after: Duration,
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
        tokio::time::sleep(self.answer_after).await;
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
    a_chatgpt_wall_offered_b_after(label, vault_account, Duration::ZERO).await
}

/// [`a_chatgpt_wall_offered_b`], with routing taking `answer_after` to answer.
async fn a_chatgpt_wall_offered_b_after(
    label: &str,
    vault_account: &'static str,
    answer_after: Duration,
) -> (
    broca_session_loop::account_move::Answer<Box<dyn broca_core::provider::Provider>>,
    Vec<serde_json::Value>,
    Vec<(String, serde_json::Value)>,
) {
    a_wall_offered(
        label,
        WalledRun {
            model: ("openai", "gpt-6.1"),
            method: "chatgpt",
            records_method: true,
            credential: "chatgpt:openai",
            account: "chatgpt-a",
            frozen: broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-6.1"),
            offered_credential: "chatgpt:openai:second",
            offered_account: "chatgpt-b",
        },
        vault_account,
        answer_after,
    )
    .await
}

/// A run stopped at its account's weekly wall, and the account routing offers it.
struct WalledRun {
    /// The run's model, as provider and model id.
    model: (&'static str, &'static str),
    /// The auth method of the run's credential and of the offered one.
    method: &'static str,
    /// Whether the run's selection records that method (it does when its credential id is the
    /// vault's own); a run admitted under a `<method>:<provider>:<label>` key may not.
    records_method: bool,
    /// The run's credential and the account it was on.
    credential: &'static str,
    account: &'static str,
    frozen: broca_provider::wire::FrozenRenderConfig,
    /// What routing offers: a credential the vault lists, for another account.
    offered_credential: &'static str,
    offered_account: &'static str,
}

/// `run` hits its weekly wall and routing offers its other account, which the vault serves for
/// `vault_account`, taking `answer_after` to answer. What the executor's side of the move
/// answers the loop, and what it asked routing and the vault.
async fn a_wall_offered(
    label: &str,
    run: WalledRun,
    vault_account: &'static str,
    answer_after: Duration,
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
    let mut executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        listed_credentials([(run.offered_credential, run.offered_credential)]),
        supervised_broca_identity(),
    )
    .await;
    // A Claude subscription run's persona is derived again for the new account from it.
    executor.claude_code_install_id = Some("install".into());
    let routing = Arc::new(OffersAccount {
        credential_id: run.offered_credential,
        account: run.offered_account,
        answer_after,
        requests: Default::default(),
    });
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("walled-run".into());
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: run.model.0.into(),
        model_id: run.model.1.into(),
    };
    let selection: broca_core::AuthSelection = serde_json::from_value(serde_json::json!({
        "credential_id": run.credential,
        "account_ref": run.account,
        "resolved_credential_id": run.credential,
        "auth_method": run.records_method.then_some(run.method),
        "account_decision": {
            "path": "routed",
            "admission_id": "walled-run",
            "decision_id": "decision-a",
            "conversation_key": "broca-session:walled",
        },
    }))
    .unwrap();
    let config: broca_wal::RunConfig = serde_json::from_value(serde_json::json!({
        "model": req.model,
        "tools": [],
        "tool_choice": broca_protocol::request::ToolChoice::default(),
        "generation": req.generation,
        "response_format": broca_protocol::request::ResponseFormat::default(),
        "context_epoch": 0,
        "renderer_version": run.frozen.renderer_version,
        "frozen_render_config": run.frozen,
        "auth_selection": selection,
    }))
    .unwrap();
    let refused = vec![run.account.to_owned()];
    let now = epoch_millis().max(0) as u64;
    let wall = broca_session_loop::account_move::Wall {
        run_id: "walled-run",
        step_id: 2,
        selection: &selection,
        predecessor: "decision-a",
        credential_id: &selection.credential_id,
        account: run.account,
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
        head_turn: false,
    };
    let moves = AccountMoves::new(&executor, &req, None, None);
    let answer =
        broca_session_loop::account_move::AccountContinue::ask(&moves, &wall).await;
    let asked = routing.requests.lock().unwrap().clone();
    let vault = daemon.requests();
    drop(moves);
    drop(executor);
    daemon.assert_healthy();
    (answer, asked, vault)
}

/// A Claude subscription run of `model`, whose admission froze the refusal fallback (the Opus
/// backstop every such run carries), hits its weekly wall and is offered another Claude account.
async fn a_claude_run_with_a_refusal_fallback_offered_b(
    label: &str,
    model: &'static str,
    records_method: bool,
) -> broca_session_loop::account_move::Answer<Box<dyn broca_core::provider::Provider>> {
    let session = broca_protocol::BindIdentity::new("/project", "harness", "s1");
    let persona = broca_module_serve::claude_code_identity::fresh_persona(
        "install",
        Some("00000000-0000-4000-8000-00000000000a"),
        "oauth:anthropic:first",
        &session,
        Some("abc".into()),
        2,
    )
    .unwrap();
    let claude_code = |model: &str| {
        let mut frozen = broca_provider::families::anthropic_frozen_config(model);
        frozen.system_prompt = broca_provider::policy::SystemPromptPolicy::RequiredIdentityLead {
            identity: "You are Claude Code, Anthropic's official CLI for Claude.".into(),
        };
        frozen.claude_code_persona = Some(persona.clone());
        frozen
    };
    let mut frozen = claude_code(model);
    // Anthropic's own fallback field, which a live Claude subscription request carries too.
    frozen.server_side_fallback = true;
    frozen.refusal_fallback = Some(Box::new(broca_provider::wire::RefusalFallbackConfig {
        model: broca_protocol::request::ModelHandle::new("anthropic", "claude-opus-4-8"),
        context_limit: None,
        frozen: claude_code("claude-opus-4-8"),
    }));
    let (answer, asked, _) = a_wall_offered(
        label,
        WalledRun {
            model: ("anthropic", model),
            method: "oauth",
            records_method,
            credential: "oauth:anthropic:first",
            account: "claude-a",
            frozen,
            offered_credential: "oauth:anthropic:second",
            offered_account: "claude-b",
        },
        "claude-b",
        Duration::ZERO,
    )
    .await;
    assert_eq!(asked.len(), 1, "{asked:#?}");
    answer
}

/// Through the executor: a Claude subscription run with a refusal fallback, with and without
/// Anthropic's server-side fallback, moves to the offered Claude account rather than refusing
/// the offer as a credential its recorded fallback can't use.
#[tokio::test]
async fn a_claude_run_with_a_refusal_fallback_moves_to_the_offered_account() {
    for (label, model, records_method) in [
        ("move-claude-test", "claude-test", true),
        ("move-claude-opus", "claude-opus-5-5", true),
        ("move-claude-unrecorded", "claude-opus-5-5", false),
    ] {
        let answer = a_claude_run_with_a_refusal_fallback_offered_b(label, model, records_method).await;
        let broca_session_loop::account_move::Answer::Move(moved) = answer else {
            panic!("{label}: the offer is taken");
        };
        assert_eq!(moved.account, "claude-b", "{label}");
        assert_eq!(moved.selection.credential_id, "oauth:anthropic:second", "{label}");
    }
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
    assert_eq!(asked[0]["params"]["admission_id"], "walled-run");
    assert_eq!(asked[0]["params"]["previous_decision_id"], "decision-a");
    assert_eq!(asked[0]["params"]["exhausted"]["account"], "chatgpt-a");
    assert!(vault
        .iter()
        .any(|(method, params)| method == "credential.get_scoped"
            && params["credential_id"] == "chatgpt:openai:second"));
}

/// Through the executor: an offer routing takes 3 s to make is still taken. A run stopped at a
/// wall waits for routing longer than an admission does (on 16 Nov a 3.2 s offer arrived after
/// the admission budget and the run ended with it unused).
#[tokio::test]
async fn the_executor_waits_for_an_offer_slower_than_an_admission_budget() {
    let (answer, asked, _) = a_chatgpt_wall_offered_b_after(
        "move-offer-slow",
        "chatgpt-b",
        Duration::from_secs(3),
    )
    .await;
    let broca_session_loop::account_move::Answer::Move(moved) = answer else {
        panic!("the slow offer is taken");
    };
    assert_eq!(moved.account, "chatgpt-b");
    assert_eq!(asked.len(), 1, "{asked:#?}");
}

/// Through the executor: an offer whose credential the vault serves for another account than
/// the offer names is refused, so the run ends at its wall as before.
#[tokio::test]
async fn the_executor_refuses_an_offer_whose_token_names_another_account() {
    let (answer, _, _) = a_chatgpt_wall_offered_b("move-offer-wrong", "chatgpt-wrong").await;
    assert!(matches!(
        answer,
        broca_session_loop::account_move::Answer::Ignored
    ));
}

/// Routing answering a resumed run's repeated admission with `reply`, and counting those
/// questions (`route.account_for`); every other request is acknowledged.
struct AnswersAdmission {
    reply: Result<serde_json::Value, serde_json::Value>,
    asked: std::sync::atomic::AtomicUsize,
    /// The params of each `route.account_for` asked.
    params: std::sync::Mutex<Vec<serde_json::Value>>,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for AnswersAdmission {
    async fn capability(&self, _name: &str) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        broca_module_serve::routing_capabilities::CapabilitySupport::Declared
    }

    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if request["method"] != "route.account_for" {
            // A report, such as the scoped not_found broca sends for a routed credential the
            // vault doesn't serve.
            return Ok(Ok(serde_json::to_vec(&serde_json::json!({"result": {}})).unwrap()));
        }
        self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.params.lock().unwrap().push(request["params"].clone());
        Ok(match &self.reply {
            Ok(body) => Ok(serde_json::to_vec(body).unwrap()),
            Err(body) => Err(serde_json::to_vec(body).unwrap()),
        })
    }
}

/// Admits the resume of a ChatGPT run that moved to `chatgpt-a` at a wall and is paused for a
/// restart (or, with `records_change` false, resumes where it can't record a change), against
/// routing that declares `account_continue` and `account_continue_resume` and answers `reply`. The vault serves
/// tokens for `vault_account`. The admitted selection and how often routing was asked.
async fn a_moved_chatgpt_resume(
    label: &str,
    vault_account: &'static str,
    records_change: bool,
    reply: Result<serde_json::Value, serde_json::Value>,
) -> (Result<broca_core::AuthSelection, String>, usize) {
    a_moved_chatgpt_resume_with(label, vault_account, records_change, reply, None, true).await
}

/// [`a_moved_chatgpt_resume`], with the vault answering `not_found` for `missing`, and listing
/// only the recorded account's credential unless `both_listed`.
async fn a_moved_chatgpt_resume_with(
    label: &str,
    vault_account: &'static str,
    records_change: bool,
    reply: Result<serde_json::Value, serde_json::Value>,
    missing: Option<&'static str>,
    both_listed: bool,
) -> (Result<broca_core::AuthSelection, String>, usize) {
    let (admitted, asked) = admit_chatgpt_resume(
        label,
        vault_account,
        true,
        records_change,
        reply,
        missing,
        both_listed,
    )
    .await;
    let selection = admitted.and_then(|admitted| {
        assert!(!admitted.fresh_episode, "a resume");
        admitted
            .resolution
            .selection
            .ok_or_else(|| "no selection".to_owned())
    });
    (selection, asked)
}

/// Admits the resume of a ChatGPT run recorded on `chatgpt-a` under routing's decision (see
/// [`a_moved_chatgpt_resume`]); `moved` says whether it moved there at a quota wall.
#[allow(clippy::too_many_arguments)]
async fn admit_chatgpt_resume(
    label: &str,
    vault_account: &'static str,
    moved: bool,
    records_change: bool,
    reply: Result<serde_json::Value, serde_json::Value>,
    missing: Option<&'static str>,
    both_listed: bool,
) -> (Result<EpisodeAdmission, String>, usize) {
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_expiring(
        &root,
        ExpiryScript {
            account_id: Some(vault_account),
            missing_credential: missing,
            ..ExpiryScript::default()
        },
    )
    .await;
    let mut executor = executor_with_capturing_vault(&root, &daemon).await;
    let mut credentials = vec![serde_json::json!(
        {"id": "chatgpt:openai", "type": "chatgpt", "categories": ["openai"], "serves": ["openai"],
         "state": "active", "record_version": 1, "operations": ["read"], "account_id": "chatgpt-a",
         "auth_method": "chatgpt", "provider_ids": ["openai"]})];
    if both_listed {
        credentials.push(serde_json::json!(
            {"id": "chatgpt:openai:second", "type": "chatgpt", "categories": ["openai"], "serves": ["openai"],
             "state": "active", "record_version": 1, "operations": ["read"], "account_id": "chatgpt-b",
             "auth_method": "chatgpt", "provider_ids": ["openai"]}));
    }
    let listing = serde_json::from_value(serde_json::json!({
        "credentials": credentials,
        "grants": 1, "grant_tuples": [], "view": "moved-resume",
    }))
    .unwrap();
    executor.vault.as_ref().unwrap().listing.store(listing);
    let routing = Arc::new(AnswersAdmission {
        reply,
        asked: Default::default(),
        params: Default::default(),
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
            moved,
            records_account_change: records_change,
        }),
        last_config: Some(&config),
        head_seq: 1,
        persona: PersonaHistory {
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
                continue_offer: None,
                routing_serves_continuation: true,
                moved_here: false,
            },
            &history,
        )
        .await
        .map_err(|error| format!("{error}"));
    let asked = routing.asked.load(std::sync::atomic::Ordering::SeqCst);
    drop(executor);
    if missing.is_some() {
        // A routed credential the vault doesn't serve has broca list the vault again, which
        // this stand-in doesn't serve; nothing else may have gone wrong.
        let errors = daemon.errors.lock().unwrap().clone();
        assert!(
            errors
                .iter()
                .all(|error| error == "unexpected route request: credential.list_scoped"),
            "{errors:?}"
        );
    } else {
        daemon.assert_healthy();
    }
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

/// The recorded account a moved run's resume keeps: chatgpt-a under the run's own routed
/// decision, which a later wall continues from.
fn kept_the_recorded_account(selection: &broca_core::AuthSelection) {
    assert_eq!(selection.credential_id, "chatgpt:openai");
    assert_eq!(selection.account_ref.as_deref(), Some("chatgpt-a"));
    let decision = selection.account_decision.as_ref().unwrap();
    assert_eq!(decision.path, broca_wal::ACCOUNT_DECISION_ROUTED);
    assert_eq!(decision.admission_id.as_deref(), Some("chatgpt-run"));
    assert_eq!(decision.decision_id.as_deref(), Some("decision-a"));
}

/// Through the executor's admission: routing answering a moved run's resume that every
/// candidate needs a sign-in is not an account; the resume keeps the one it recorded.
#[tokio::test]
async fn a_moved_runs_resume_keeps_its_account_when_routing_says_sign_in() {
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-auth-required",
        "chatgpt-a",
        true,
        Err(serde_json::json!({"code": "auth_required", "message": "sign in", "detail": {}})),
    )
    .await;
    assert_eq!(asked, 1);
    kept_the_recorded_account(&selection.unwrap());
}

/// Through the executor's admission: a routed account whose credential the vault doesn't
/// serve here, or serves for another account than routing named, can't be admitted; the
/// moved run's resume keeps the account it recorded.
#[tokio::test]
async fn a_moved_runs_resume_keeps_its_account_when_the_routed_one_cant_be_admitted() {
    let (selection, asked) = a_moved_chatgpt_resume_with(
        "moved-resume-not-found",
        "chatgpt-a",
        true,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
        Some("chatgpt:openai:second"),
        true,
    )
    .await;
    assert_eq!(asked, 1);
    kept_the_recorded_account(&selection.unwrap());
    // The vault serves every token for chatgpt-a, so routing's chatgpt-b doesn't match.
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-other-account",
        "chatgpt-a",
        true,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
    )
    .await;
    assert_eq!(asked, 1);
    kept_the_recorded_account(&selection.unwrap());
}

/// Through the executor's admission: with only the recorded account's credential listed,
/// routing isn't asked and the pick is static. A moved run's resume keeps its routed decision
/// rather than taking the static one, which names no admission.
#[tokio::test]
async fn a_moved_runs_resume_keeps_its_routed_decision_on_a_static_pick() {
    let (selection, _) = a_moved_chatgpt_resume_with(
        "moved-resume-static",
        "chatgpt-a",
        true,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
        None,
        false,
    )
    .await;
    kept_the_recorded_account(&selection.unwrap());
    // Routing's account selection switched off: also a static pick.
    let (selection, asked) = a_moved_chatgpt_resume(
        "moved-resume-disabled",
        "chatgpt-a",
        true,
        Err(serde_json::json!({"code": "account_selection_disabled", "message": "off", "detail": {}})),
    )
    .await;
    assert_eq!(asked, 1);
    kept_the_recorded_account(&selection.unwrap());
}

/// Through the executor: a run that never moved resumes (a held call's resume, or any other)
/// after a re-login in which the vault now serves its recorded credential for another account.
/// The admission names that account, but the provider built for the resume keeps the run's
/// frozen account, so its first request is refused before anything is sent: an Auth error with
/// `account_changed`, which the loop turns into a pause for a sign-in. Nothing goes out on, or
/// is recorded for, the other account.
#[tokio::test]
async fn a_resume_after_a_relogin_to_another_account_pauses_as_account_changed() {
    let (admitted, asked) = admit_chatgpt_resume(
        "relogin-other-account",
        "chatgpt-c",
        false,
        false,
        Ok(chosen("chatgpt-b", "chatgpt:openai:second")),
        None,
        true,
    )
    .await;
    assert_eq!(asked, 0, "a run that never moved doesn't ask routing");
    let admitted = admitted.unwrap();
    assert_eq!(
        admitted
            .resolution
            .selection
            .as_ref()
            .and_then(|selection| selection.account_ref.as_deref()),
        Some("chatgpt-c"),
        "the vault served the recorded credential for another account"
    );
    let opts = broca_protocol::request::CallOptions::new(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "hello".into(),
        }],
    )]);
    let mut events = admitted
        .resolved
        .provider
        .step(broca_protocol::StepId(2), &opts)
        .await;
    let mut seen = Vec::new();
    while let Some(event) = events.recv().await {
        seen.push(event);
    }
    assert!(
        !seen.iter().any(|event| matches!(
            event,
            broca_protocol::NormalizedEvent::Control(
                broca_protocol::provider::ProviderControl::AttemptFinished { .. }
            )
        )),
        "no attempt was made: {seen:?}"
    );
    let error = seen
        .iter()
        .find_map(|event| match event {
            broca_protocol::NormalizedEvent::Control(
                broca_protocol::provider::ProviderControl::Error { error, .. },
            ) => Some(error.clone()),
            _ => None,
        })
        .expect("the step ends with an error");
    assert_eq!(error.provider_code.as_deref(), Some("account_changed"));
    assert_eq!(error.class, broca_protocol::ErrorClass::Auth);
}

/// A model call that never answers, so the run it belongs to is cut off mid-step.
struct NeverAnswers;

#[async_trait::async_trait]
impl broca_core::provider::Provider for NeverAnswers {
    async fn step(
        &self,
        _step_id: broca_protocol::StepId,
        _opts: &broca_protocol::request::CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        std::future::pending().await
    }
}

/// Routing that keeps every observation it is sent.
#[derive(Default)]
struct KeepsObservations(std::sync::Mutex<Vec<serde_json::Value>>);

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for KeepsObservations {
    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        Ok(Ok(serde_json::to_vec(&serde_json::json!({"result": {}})).unwrap()))
    }
}

/// A routed run a crash cut off mid-step is sealed when its session opens, and its end reaches
/// routing as `run_finished` under the run's admission, so routing's lease on the account
/// ends as it does for a run that ends any other way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sealed_routed_run_reports_its_end_to_routing() {
    let root = fresh_vault_root("sealed-run-finished");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let mut executor = executor_with_capturing_vault(&root, &daemon).await;
    let routing = Arc::new(KeepsObservations::default());
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut session = store_session().await;
    let mut parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("sealed-run".into());
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "hello".into(),
        }],
    )];
    // The run starts under routing's decision and is cut off inside its first model call.
    let control = broca_core::RunControl {
        auth_selection: Some(
            serde_json::from_value(serde_json::json!({
                "credential_id": "oauth:anthropic",
                "account_ref": "acct-a",
                "account_decision": {
                    "path": "routed",
                    "admission_id": "sealed-run",
                    "decision_id": "decision-a",
                    "conversation_key": "broca-session:sealed",
                },
            }))
            .unwrap(),
        ),
        ..Default::default()
    };
    {
        let provider = NeverAnswers;
        let tools = broca_core::StubToolPlane::new();
        let now = || 1_000;
        let seams = broca_session_loop::Seams {
            person_steers: None,
            provider: &provider,
            tools: &tools,
            bind: req.session.clone(),
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
            model_call_ready: None,
            pause_for_auth_at_start: None,
            account_continue: None,
        };
        let mut started = req.clone();
        let admission = broca_session_loop::admit::admission(
            &mut started,
            &control,
            &provider,
            broca_session_loop::writer::Writer::operations(&session),
            Some(1_000),
        )
        .unwrap();
        let cut = tokio::time::timeout(
            Duration::from_millis(200),
            broca_session_loop::run(&mut session, &mut parts, admission, &seams),
        )
        .await;
        assert!(cut.is_err(), "the run is cut off inside its model call");
    }
    let end = serve_store_run(
        &executor,
        &mut session,
        &parts,
        req,
        broca_module::executor::StoreRunMode::Seal(broca_core::seal::SealCause::CrashOrphan),
    )
    .await;
    let finished = end.result.expect("the run is sealed");
    assert_eq!(finished.reason, RunFinishReason::Interrupted);
    let observed = broca_test_support::wait_until("routing hears the run end", || {
        let routing = Arc::clone(&routing);
        async move {
        let observed = routing.0.lock().unwrap().clone();
        if observed
            .iter()
            .any(|request| request["params"]["kind"] == "run_finished")
        {
            Ok(observed)
        } else {
            Err(format!("{observed:?}"))
        }
        }
    })
    .await;
    let finished: Vec<_> = observed
        .iter()
        .filter(|request| request["params"]["kind"] == "run_finished")
        .collect();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0]["method"], "route.account_observe");
    assert_eq!(finished[0]["params"]["admission_id"], "sealed-run");
    assert_eq!(finished[0]["params"]["run_id"], "sealed-run");
    drop(executor);
    drop(daemon);
}

/// Through the executor's admission: a session's next run after its idle run on
/// `chatgpt:openai`, on a session that moved here when `moved_here`. The `route.account_for`
/// params it asked routing (which declares every capability) with.
async fn a_fresh_run_after_chatgpt_a(label: &str, moved_here: bool) -> serde_json::Value {
    let _catalog = install_test_catalog();
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_expiring(
        &root,
        ExpiryScript {
            account_id: Some("chatgpt-a"),
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
        "grants": 1, "grant_tuples": [], "view": "moved-here",
    }))
    .unwrap();
    executor.vault.as_ref().unwrap().listing.store(listing);
    let routing = Arc::new(AnswersAdmission {
        reply: Ok(chosen("chatgpt-a", "chatgpt:openai")),
        asked: Default::default(),
        params: Default::default(),
    });
    executor.routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            broca_module_serve::account_routing::AccountSelectionConfig::default(),
        ),
    ));
    let mut req = drift_request(&root);
    req.run_id = broca_protocol::RunId("next-run".into());
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-5.6-sol".into(),
    };
    let frozen = broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-5.6-sol");
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
                "admission_id": "earlier-run",
                "decision_id": "decision-a",
                "conversation_key": "broca-session:chatgpt",
            },
        },
    }))
    .unwrap();
    let history = EpisodeHistory {
        prior_run: Some(PriorRunView {
            run_id: broca_protocol::RunId("earlier-run".into()),
            config: config.clone(),
            idle: true,
            moved: false,
            records_account_change: false,
        }),
        last_config: Some(&config),
        head_seq: 1,
        persona: PersonaHistory {
            first_user: None,
            first_user_sealed: false,
            human_prompts: 0,
        },
    };
    let mut queued_service_tier = None;
    executor
        .admit_episode(
            EpisodeJob {
                req: &req,
                kind: RunKind::FreshOrResume,
                auth: &None,
                conversation_key: &None,
                queued_service_tier: &mut queued_service_tier,
                media_profile: &None,
                clock: None,
                continue_offer: None,
                routing_serves_continuation: true,
                moved_here,
            },
            &history,
        )
        .await
        .map_err(|error| format!("{error}"))
        .unwrap();
    drop(executor);
    daemon.assert_healthy();
    let params = routing.params.lock().unwrap().clone();
    assert_eq!(params.len(), 1, "{params:?}");
    params[0].clone()
}

/// A session that moved here names the credential its last run used when its next run asks
/// routing for an account; a session that didn't move doesn't.
#[tokio::test]
async fn a_moved_sessions_next_run_names_the_credential_its_last_run_used() {
    let moved = a_fresh_run_after_chatgpt_a("moved-here-warm", true).await;
    assert_eq!(moved["warm_credential"], "chatgpt:openai", "{moved}");
    let stayed = a_fresh_run_after_chatgpt_a("not-moved-warm", false).await;
    assert!(stayed.get("warm_credential").is_none(), "{stayed}");
}
