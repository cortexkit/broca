/// A parked sign-in pause can be cancelled even when the vault would refuse its credential.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_store_cancel_needs_no_credentials_or_provider() {
    use broca_core::provider::StubProvider;
    use broca_session_loop::writer::Writer as _;
    use broca_session_store::record::{CancelPoint, CancelRequest, Record};
    use tokio_util::sync::CancellationToken;

    let server = resume_http_fixture().await;
    let root = fresh_vault_root("store-parked-cancel");
    install_alias_catalog();
    let daemon = CapturingVaultDaemon::start_refusing_scoped_reads(&root).await;
    let executor = executor_with_capturing_vault(&root, &daemon)
        .await
        .with_provider_overrides(runtime_overrides(&server.url));
    assert!(executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .is_err());
    assert_eq!(
        daemon.scoped_reads().len(),
        1,
        "the fixture credential really is unavailable"
    );
    daemon.requests.lock().unwrap().clear();

    let mut session = store_session().await;
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let mut req = drift_request(&root);
    req.model = broca_protocol::request::ModelHandle {
        provider_module_id: "deepseek".into(),
        model_id: "alias-model".into(),
    };
    req.context_limit = Some(8192);
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "hello".into(),
        }],
    )];
    // Seed the existing run with the real store loop, but no live credential or model call.
    let provider = StubProvider::new(vec![vec![broca_protocol::NormalizedEvent::Control(
        broca_protocol::ProviderControl::Error {
            error: serde_json::from_value(serde_json::json!({
                "class": "auth", "message": "sign in again", "status": 401,
            }))
            .unwrap(),
            retries_used: 0,
        },
    )]]);
    let control = broca_core::run::RunControl {
        admitted_context_limit: req.context_limit,
        ..Default::default()
    };
    let admission = broca_session_loop::admit::admission(
        &mut req,
        &control,
        &provider,
        session.operations(),
        Some(1),
    )
    .unwrap();
    let (out, _events) = mpsc::channel(256);
    let now = || 1;
    let seams = store_seams(
        &provider,
        executor.plane.as_ref(),
        &req,
        &out,
        &control,
        None,
        None,
        &now,
        None,
        None,
        executor.max_same_account_wait,
    );
    let mut seed_parts = parts.clone();
    let paused = broca_session_loop::run(&mut session, &mut seed_parts, admission, &seams)
        .await
        .unwrap();
    assert_eq!(
        paused.paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    session
        .append(
            &[Record::CancelRequest(CancelRequest {
                run_id: req.run_id.0.clone(),
                at: CancelPoint::Now,
            })],
            true,
        )
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let end = serve_store_run_with_control(
        &executor,
        &mut session,
        &parts,
        req,
        broca_module::executor::StoreRunMode::Resume,
        broca_core::run::RunControl {
            cancel: Some(cancel),
            ..Default::default()
        },
    )
    .await;
    let finished = end.result.expect("cancellation does not require admission");
    assert_eq!(finished.reason, RunFinishReason::Cancelled);
    assert!(finished.paused.is_none());
    let records = session.open_log_records().await.unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|(_, record)| matches!(record, Record::RunStart(_)))
            .count(),
        1
    );
    let ends: Vec<_> = records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end.reason),
            _ => None,
        })
        .collect();
    assert_eq!(ends, [RunFinishReason::Cancelled]);
    assert!(
        daemon.requests().is_empty(),
        "cancel must not ask the vault"
    );
    assert!(
        server.requests.lock().unwrap().is_empty(),
        "cancel must not call the provider"
    );
    daemon.assert_healthy();
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}
