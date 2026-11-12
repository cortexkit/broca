use super::*;

#[test]
fn fork_sessions_are_named_after_their_head() {
    let head = BindIdentity::new("/p", "opencode", "ses_head");
    let fork = fork_session(&head, "fork-0123456789abcdef0123456789abcdef");
    assert_eq!(
        fork.session,
        "ses_head:fork:fork-0123456789abcdef0123456789abcdef"
    );
    assert_eq!(
        (fork.project_root.clone(), fork.harness.clone()),
        (head.project_root.clone(), head.harness.clone())
    );
    assert_eq!(
        head_of(&fork, "fork-0123456789abcdef0123456789abcdef"),
        head
    );
}

#[test]
fn only_minted_fork_ids_name_a_session() {
    let minted = mint_fork_id().unwrap();
    assert!(minted_fork_id(&minted), "{minted}");
    assert_ne!(minted, mint_fork_id().unwrap());
    for foreign in [
        "",
        "fork-",
        "fork-0123456789ABCDEF0123456789abcdef",
        "fork-0123456789abcdef0123456789abcde",
        "fork-0123456789abcdef0123456789abcdef0",
        "../fork-0123456789abcdef0123456789abcdef",
        "x-0123456789abcdef0123456789abcdef",
    ] {
        assert!(!minted_fork_id(foreign), "{foreign}");
    }
}

#[test]
fn account_refusals_carry_the_contract_detail() {
    let held = ForkError::AccountHeld {
        routing_code: None,
        mismatch: Some("credential".into()),
    };
    assert_eq!(held.code(), "fork_account_held");
    assert_eq!(held.detail(), Some(json!({"mismatch": "credential"})));
    let blocked = ForkError::AccountBlocked {
        routing_code: "no_account".into(),
    };
    assert_eq!(
        blocked.detail(),
        Some(json!({"routing_code": "no_account"}))
    );
    assert_eq!(ForkError::UnknownFork.detail(), None);
}
