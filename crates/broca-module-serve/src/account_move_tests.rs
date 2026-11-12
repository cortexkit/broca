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
