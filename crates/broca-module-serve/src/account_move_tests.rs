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
