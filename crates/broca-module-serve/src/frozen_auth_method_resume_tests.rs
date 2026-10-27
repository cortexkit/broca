/// A RUN THE ACCOUNT-CHOOSER BUILD ADMITTED RESUMES HERE ON ITS FROZEN CREDENTIAL.
///
/// The `RunStarted` is the exact bytes that build writes for a routed run: the vault's opaque
/// credential id `work-claude` with `auth_method: "oauth"` beside it. It is framed and decoded
/// as replay reads it, folded into the admission checkpoint, and resumed through the same
/// prior-run view `execute` uses. The resume reads `work-claude` verbatim and authenticates as
/// OAuth. An alias exists for the broca key `oauth:anthropic`, so a build that turned the id
/// into a broca key, or ignored the method and fell back to configuration, would read
/// something else. The selection re-frozen on resume keeps the method, so a later resume of the
/// same run, here or on the writing build, reads it the same way.
#[tokio::test]
async fn a_run_started_with_a_frozen_auth_method_resumes_on_its_vault_id_verbatim() {
    const RUN_STARTED: &str = r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"work-claude","account_ref":"acct-1","account_decision":{"path":"routed","admission_id":"r","decision_id":"acd-1"},"auth_method":"oauth"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
    let record: broca_wal::WalRecord = serde_json::from_str(RUN_STARTED).unwrap();
    let bytes = broca_wal::framing::encode(1, 1, &record).unwrap();
    let framed = match broca_wal::framing::decode_next(&bytes) {
        broca_wal::framing::DecodeStep::Record { framed, .. } => framed,
        _ => panic!("this build reads a selection frozen with its method"),
    };
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&[*framed]).unwrap();
    let prior = prior_run_view_from_checkpoint(&checkpoint).expect("the admitted run");
    assert!(!prior.idle, "the run never finished, so its send resumes it");
    let frozen = prior
        .config
        .auth_selection
        .clone()
        .expect("the run froze its selection");

    // Every reader of a frozen selection takes the method from the field and the provider from
    // the run, and names the vault id verbatim.
    assert_eq!(
        broca_module_serve::auth_routing::frozen_selection_credential(&frozen, "anthropic"),
        Some((AuthMethod::Oauth, "anthropic".to_owned()))
    );

    let root = fresh_vault_root("branch-shape-resume");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        broca_module_serve::VaultCredentialIds::from_pairs([("oauth:anthropic", "aliased-key")]),
        supervised_broca_identity(),
    )
    .await;
    assert_eq!(
        executor
            .vault
            .as_ref()
            .unwrap()
            .addressing
            .vault_id_for_selection(&frozen)
            .as_deref(),
        Some("work-claude")
    );
    let resolved = executor
        .decide_auth_with_account(
            prior.config.model.provider_module_id.as_str(),
            Some(frozen.credential_id.as_str()),
            frozen.auth_method.as_deref(),
            false,
            None,
            None,
            &AccountChoice::NotApplicable,
            frozen.account_decision.as_ref(),
        )
        .await
        .unwrap_or_else(|error| {
            daemon.assert_healthy();
            panic!("the resume is admitted: {error:?}")
        });
    let requests = daemon.requests();
    assert_eq!(methods(&requests), ["credential.get_scoped"]);
    assert_eq!(requests[0].1["credential_id"], "work-claude");
    assert_eq!(resolved.method, Some(AuthMethod::Oauth));
    let selection = resolved.selection.expect("a resume keeps its selection");
    assert_eq!(selection.credential_id, "work-claude");
    assert_eq!(selection.auth_method.as_deref(), Some("oauth"));
    assert_eq!(selection.account_decision, frozen.account_decision);
    let refrozen = broca_wal::WalRecord::RunAuthRefrozen {
        ts_ms: None,
        auth_selection: selection,
    };
    assert_eq!(
        refrozen.required_features(),
        &[broca_wal::framing::AUTH_SELECTION_METHOD_FEATURE]
    );
    daemon.assert_healthy();
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// NOTHING THIS BUILD ADMITS FREEZES AN AUTH METHOD.
///
/// A fresh admission, whether resolved from configuration or routed to an account, freezes a
/// broca key with no `auth_method`, so the `RunStarted` it writes needs no reader feature and
/// any older build can still read and resume it.
#[tokio::test]
async fn a_fresh_admission_freezes_no_auth_method_and_requires_no_feature() {
    let root = fresh_vault_root("fresh-no-auth-method");
    let daemon = CapturingVaultDaemon::start_expiring(
        &root,
        ExpiryScript {
            account_id: Some("chatgpt-b"),
            ..ExpiryScript::default()
        },
    )
    .await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let configured = executor
        .decide_auth_with_account(
            "anthropic",
            None,
            None,
            true,
            Some(AuthMethod::Oauth),
            None,
            &AccountChoice::NotApplicable,
            None,
        )
        .await
        .unwrap();
    let routed = executor
        .decide_auth_with_account(
            "openai",
            None,
            None,
            true,
            Some(AuthMethod::Chatgpt),
            None,
            &routed_chatgpt_choice().await,
            None,
        )
        .await
        .unwrap();
    for resolved in [configured, routed] {
        let selection = resolved.selection.expect("a fresh run freezes its selection");
        assert_eq!(selection.auth_method, None, "{}", selection.credential_id);
        let mut record: broca_wal::WalRecord = serde_json::from_str(
            r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        )
        .unwrap();
        let broca_wal::WalRecord::RunStarted { config, .. } = &mut record else {
            unreachable!()
        };
        config.auth_selection = Some(selection);
        assert!(record.required_features().is_empty());
        assert!(!serde_json::to_string(&record)
            .unwrap()
            .contains("auth_method"));
    }
    daemon.assert_healthy();
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}
