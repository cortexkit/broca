// Moving a run to another account: the executor's decisions that don't need a vault.

/// A resumed run asks routing for its account again only when it moved and routing serves
/// continuation (it declares both `account_continue` and `account_continue_resume`).
#[test]
fn only_a_moved_run_resumed_against_a_router_that_answers_moved_runs_asks_again() {
    assert!(!resume_asks_routing(false, false));
    assert!(!resume_asks_routing(false, true));
    assert!(!resume_asks_routing(true, false));
    assert!(resume_asks_routing(true, true));
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
