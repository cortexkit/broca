use super::*;

#[tokio::test]
async fn scope_gone_withdrawals_do_not_degrade_health_but_other_failures_do() {
    /// No session store is installed, so recovery can't read a cancellation and asks nothing.
    /// The provider side is covered with a store in broca-module's withdrawal tests.
    struct NeverAsked;
    #[async_trait::async_trait]
    impl broca_module::held_withdrawals::WithdrawTransport for NeverAsked {
        async fn withdraw(
            &self,
            _: &broca_core::tool_plane::LateResultRegistration,
            _: bool,
        ) -> (
            cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
            serde_json::Value,
        ) {
            panic!("nothing is asked without a session store");
        }
    }
    // A failure whose scope is gone is never asked again and doesn't degrade health. Any other
    // failure is asked again at a start, so it waits for a session store and stays failed, and
    // health stays degraded until the provider answers.
    for (code, waits, failed, scope_gone, expected) in [
        (
            subc_protocol::error_codes::SCOPE_NOT_LIVE,
            false,
            0,
            1,
            subc_protocol::session::HealthStatus::Ok,
        ),
        (
            subc_protocol::error_codes::SCOPE_ENDED,
            false,
            0,
            1,
            subc_protocol::session::HealthStatus::Ok,
        ),
        (
            subc_protocol::error_codes::SCOPE_NOT_CARRIER,
            true,
            1,
            0,
            subc_protocol::session::HealthStatus::Degraded,
        ),
    ] {
        let root = broca_test_support::scratch_root().join(format!("withdrawal-health-{code}"));
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(
            broca_module::late_results_store::LateResultsStore::open(&root.join("late-results.db"))
                .unwrap(),
        );
        let call = broca_core::tool_plane::LateResultRegistration {
            lineage: "lineage".into(),
            call_key: "lineage:4".into(),
            module: "tools".into(),
            bind: BrocaBindIdentity::new("/project", "broca", code),
            scope: None,
        };
        store.register_calls(std::slice::from_ref(&call)).unwrap();
        // Existing outbox rows keep their terminal state; only health accounting changes.
        store
            .record_withdrawal(&call, "failed", 1, &serde_json::json!({"code":code}))
            .unwrap();
        let service =
            broca_module::held_withdrawals::HeldWithdrawals::new(store, Arc::new(NeverAsked));
        service.bind_stopping_signal(CancellationToken::new());
        let recovered = service.recover().await;
        assert_eq!(recovered.is_err(), waits, "{code}: {recovered:?}");
        let withdrawals = service.health();
        assert_eq!(withdrawals["failed"], failed, "{code}: {withdrawals}");
        assert_eq!(
            withdrawals["scope_gone"], scope_gone,
            "{code}: {withdrawals}"
        );
        let details: Vec<_> = held_withdrawal_health_detail(&withdrawals)
            .into_iter()
            .collect();
        assert_eq!(health_status(&details), expected, "{withdrawals}");
    }
    let error = serde_json::json!({"failed": 0, "error": "unreadable"});
    assert!(held_withdrawal_health_detail(&error).is_some());
}
