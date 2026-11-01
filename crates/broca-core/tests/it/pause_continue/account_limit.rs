use super::*;
use broca_core::{run_loop_with_control, Provider, RunControl};
use broca_protocol::request::CallOptions;
use sha2::Digest;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

struct Limited {
    reject: AtomicBool,
    sent: Mutex<Vec<Vec<u8>>>,
    tool: bool,
    reset_ms: Option<u64>,
    drift: AtomicBool,
}

impl Limited {
    fn new(tool: bool, reject: bool) -> Self {
        Self {
            reject: AtomicBool::new(reject),
            sent: Mutex::new(Vec::new()),
            tool,
            reset_ms: Some(61_000),
            drift: AtomicBool::new(false),
        }
    }

    async fn send(
        &self,
        step: StepId,
        opts: &CallOptions,
        ordinal: u32,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(opts);
        self.sent.lock().unwrap().push(bytes.clone());
        let is_tool = self.tool && step.0 == 1;
        let rejected = !is_tool && self.reject.swap(false, Ordering::SeqCst);
        let mut attempt: broca_protocol::ModelAttempt = serde_json::from_value(json!({
            "attempt": ordinal, "request_body_len": bytes.len(),
            "request_body_sha256": format!("{:x}", sha2::Sha256::digest(&bytes)),
            "response_body_len": 0, "response_body_sha256": "0".repeat(64),
            "outcome": if rejected { "http_error" } else { "complete" }, "ts_ms": 1000
        }))
        .unwrap();
        if rejected {
            attempt.account_observation.limit = Some(broca_protocol::AttemptAccountLimit {
                disposition: broca_protocol::provider::AccountLimitDisposition::ACCOUNT,
                observed_at_ms: 1000,
                resets_at_ms: self.reset_ms,
                resets_at_source: Some("body".into()),
                limit_window: None,
            });
        }
        let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
            attempt,
        })];
        events.extend(if is_tool {
            let mut events = tool_reply("tools", "once");
            for event in &mut events {
                if let NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) = event {
                    *usage = Usage {
                        input_tokens: Some(100),
                        cached_input_tokens: Some(0),
                        cache_write_tokens: Some(0),
                        output_tokens: Some(0),
                        ..Default::default()
                    };
                }
            }
            events
        } else if rejected {
            let error: ProviderError = serde_json::from_value(json!({"class":"transient",
                "message":"account window exhausted", "status":429,
                    "provider_code":"account_limit_exhausted", "resets_at_ms":self.reset_ms}))
            .unwrap();
            vec![NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used: 0,
            })]
        } else {
            text_reply("done", "done")
        });
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            for event in events {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

#[async_trait::async_trait]
impl Provider for Limited {
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        let mut bytes = broca_core::render::canonical_request_bytes(opts);
        if self.drift.load(Ordering::SeqCst) {
            bytes.extend_from_slice(b"changed");
        }
        bytes
    }
    async fn step(&self, step: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.send(step, opts, 0).await
    }
    async fn step_with_runtime(
        &self,
        step: StepId,
        opts: Arc<CallOptions>,
        runtime: broca_core::provider::StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.send(step, &opts, runtime.first_attempt).await
    }
    fn reports_attempt_evidence(&self) -> bool {
        true
    }
}

async fn drive(
    request: RunRequest,
    provider: &impl Provider,
    plane: &StubToolPlane,
    leases: &FileLeaseStore,
    wal: &FileWalStore,
    control: RunControl,
) -> (broca_core::RunSummary, Vec<OutputEvent>) {
    let (tx, mut rx) = mpsc::channel(256);
    let output = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });
    let summary = run_loop_with_control(
        request, provider, plane, leases, &AllowAll, wal, None, control, &tx,
    )
    .await
    .unwrap();
    drop(tx);
    (summary, output.await.unwrap())
}

#[tokio::test]
async fn timed_account_limit_pause_follows_tool_results_and_retries_identical_bytes() {
    let root = broca_test_support::scratch_root().join("account-limit-core-tool-boundary");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "timed");
    let request = tool_req("timed-run", &session, "edit once");
    let provider = Limited::new(true, true);
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "edited".into(),
        })
    });
    let selection =
        serde_json::from_value(json!({"credential_id":"oauth:stub:work","account_ref":"acct-a"}))
            .unwrap();
    let control = RunControl {
        account_limit_pause: true,
        clock: Some(clock.clone()),
        auth_selection: Some(selection),
        ..Default::default()
    };
    let (paused, output) = drive(request.clone(), &provider, &plane, &leases, &wal, control).await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    let records = records_of(&wal, &session);
    let pause = records
        .iter()
        .position(|r| {
            matches!(
                r,
                WalRecord::RunPaused {
                    reason: PauseReason::AccountLimit,
                    ..
                }
            )
        })
        .unwrap();
    let result = records
        .iter()
        .position(|r| matches!(r, WalRecord::ToolResult { .. }))
        .unwrap();
    assert!(result < pause);
    assert!(output.iter().any(|event| matches!(
        event,
        OutputEvent::Control(ControlUnit::Paused {
            reason: PauseReason::AccountLimit,
            ..
        })
    )));
    assert!(!output.iter().any(|event| matches!(
        event,
        OutputEvent::Control(ControlUnit::RunFinished { .. } | ControlUnit::Error { .. })
    )));
    let (still_paused, early_output) = drive(
        request.clone(),
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            clock: Some(clock.clone()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(still_paused.paused, Some(PauseReason::AccountLimit));
    assert_eq!(provider.sent.lock().unwrap().len(), 2);
    assert!(early_output.iter().any(|event| matches!(
        event,
        OutputEvent::Control(ControlUnit::Paused {
            reason: PauseReason::AccountLimit,
            resume_not_before_ms: Some(61_000),
            error: Some(_),
            ..
        })
    )));
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (resumed, _) = drive(
        request,
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            clock: Some(clock),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(resumed.reason, RunFinishReason::Completed);
    let sent = provider.sent.lock().unwrap();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[1], sent[2]);
    let records = records_of(&wal, &session);
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }))
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn timed_account_limit_run_edges_and_stale_health_counter_are_pinned() {
    let before = broca_core::account_limit_stale_resets();
    for (name, reset, pauses) in [
        ("lower", 1000, true),
        ("stale", 999, false),
        ("upper", 3_601_000, true),
        ("distant", 3_601_001, false),
    ] {
        let root = broca_test_support::scratch_root().join(format!("account-limit-edge-{name}"));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
        let clock = broca_core::Clock::for_state_root(&root).unwrap();
        let wal = FileWalStore::new(root.join("wal"));
        let leases = FileLeaseStore::new(root.join("leases"));
        let session = BindIdentity::new(root.join("project"), "h", name);
        let mut provider = Limited::new(false, true);
        provider.reset_ms = Some(reset);
        let control = RunControl {
            account_limit_pause: true,
            clock: Some(clock),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap(),
            ),
            ..Default::default()
        };
        let (summary, _) = drive(
            req(&session, "hello"),
            &provider,
            &StubToolPlane::new(),
            &leases,
            &wal,
            control,
        )
        .await;
        assert_eq!(
            summary.paused,
            pauses.then_some(PauseReason::AccountLimit),
            "{name}"
        );
        if !pauses {
            assert_eq!(summary.reason, RunFinishReason::Error, "{name}");
        }
    }
    assert_eq!(broca_core::account_limit_stale_resets(), before + 1);
}

#[tokio::test]
async fn disabled_account_pause_keeps_legacy_account_error_bytes_and_no_feature() {
    let root = broca_test_support::scratch_root().join("account-limit-disabled-bytes");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "off");
    let provider = Limited::new(false, true);
    let (summary, _) = drive(
        req(&session, "hello"),
        &provider,
        &StubToolPlane::new(),
        &leases,
        &wal,
        RunControl {
            clock: Some(clock),
            auth_selection: Some(
                serde_json::from_value(json!({
            "credential_id":"oauth:stub:work", "account_ref":"acct-a"}))
                .unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(summary.paused, None);
    assert_eq!(summary.reason, RunFinishReason::Error);
    let records = records_of(&wal, &session);
    for record in &records {
        assert!(!record
            .required_features()
            .contains(&"account-limit-pause/v1"));
    }
    let terminal = records
        .iter()
        .find(|record| matches!(record, WalRecord::RunFinished { .. }))
        .unwrap();
    assert_eq!(
        serde_json::to_string(terminal).unwrap(),
        r#"{"type":"run_finished","reason":"error","ts_ms":1000,"usage":{},"error":{"class":"transient","message":"account window exhausted","status":429,"provider_code":"account_limit_exhausted","resets_at_ms":61000,"account_ref":"acct-a","credential_id":"oauth:stub:work"},"retries_used":0}"#
    );
    assert!(records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ModelAttemptFinished {
                account_limit_disposition,
                ..
            } => Some(account_limit_disposition),
            _ => None,
        })
        .all(Option::is_none));
}

#[tokio::test]
async fn an_exhausted_session_input_budget_stops_account_resume_before_sending() {
    let root = broca_test_support::scratch_root().join("account-limit-budget");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "budget");
    let mut request = tool_req("budget-run", &session, "edit once");
    let provider = Limited::new(true, true);
    let plane = StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "edited".into(),
        })
    });
    let (paused, _) = drive(
        request.clone(),
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            account_limit_pause: true,
            clock: Some(clock.clone()),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    request.budget = Some(broca_protocol::request::BudgetConfig {
        max_input_tokens: Some(10),
        nudges: Vec::new(),
        finalize_at_tool_calls: 99,
        finalize_text: "finish".into(),
    });
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (summary, _) = drive(
        request,
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            clock: Some(clock),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::BudgetExhausted);
    assert_eq!(provider.sent.lock().unwrap().len(), 2);
    assert!(records_of(&wal, &session).iter().any(|record| matches!(record, WalRecord::InputTokenBudgetExhausted { detail } if detail.spent == 100)));
}

#[tokio::test]
async fn account_resume_refuses_a_changed_selection_without_sending_or_refreezing() {
    let root = broca_test_support::scratch_root().join("account-limit-selection-guard");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "selection");
    let request = req(&session, "hello");
    let provider = Limited::new(false, true);
    let plane = StubToolPlane::new();
    let (paused, _) = drive(
        request.clone(),
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            account_limit_pause: true,
            clock: Some(clock.clone()),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (summary, _) = drive(
        request,
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            clock: Some(clock),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:other"})).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(provider.sent.lock().unwrap().len(), 1);
    let records = records_of(&wal, &session);
    assert!(!records
        .iter()
        .any(|record| matches!(record, WalRecord::RunAuthRefrozen { .. })));
    assert!(records.iter().any(
        |record| matches!(record, WalRecord::RunFinished { error:Some(error), .. }
        if error.provider_code.as_deref() == Some("account_limit_selection_changed"))
    ));
}

#[tokio::test]
async fn account_resume_checks_the_recorded_request_digest_before_provider_io() {
    let root = broca_test_support::scratch_root().join("account-limit-request-guard");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "request");
    let request = req(&session, "hello");
    let provider = Limited::new(false, true);
    let plane = StubToolPlane::new();
    let (paused, _) = drive(
        request.clone(),
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            account_limit_pause: true,
            clock: Some(clock.clone()),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    provider.drift.store(true, Ordering::SeqCst);
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (summary, _) = drive(
        request,
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            clock: Some(clock),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert_eq!(provider.sent.lock().unwrap().len(), 1);
    assert!(records_of(&wal, &session).iter().any(
        |record| matches!(record, WalRecord::RunFinished { error:Some(error), .. }
        if error.provider_code.as_deref() == Some("account_limit_request_changed"))
    ));
}

#[tokio::test]
async fn account_resume_vault_reauth_keeps_the_existing_auth_pause_path() {
    let root = broca_test_support::scratch_root().join("account-limit-vault-reauth");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "reauth");
    let request = req(&session, "hello");
    let provider = Limited::new(false, true);
    let plane = StubToolPlane::new();
    let (paused, _) = drive(request.clone(), &provider, &plane, &leases, &wal, RunControl {
        account_limit_pause:true, clock:Some(clock.clone()), auth_selection:Some(serde_json::from_value(json!({
            "credential_id":"oauth:stub:work", "account_ref":"acct-a", "resolved_credential_id":"cred-a"
        })).unwrap()), ..Default::default()
    }).await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (summary, _) = drive(request, &provider, &plane, &leases, &wal, RunControl {
        clock:Some(clock), auth_selection:Some(serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap()),
        pause_for_auth_at_start:Some(serde_json::from_value(json!({"class":"auth_required", "message":"credential needs reauthentication", "provider_code":"needs_reauth"})).unwrap()),
        ..Default::default()
    }).await;
    assert_eq!(summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(provider.sent.lock().unwrap().len(), 1);
    assert!(!records_of(&wal, &session)
        .iter()
        .any(|record| matches!(record, WalRecord::RunAuthRefrozen { .. })));
}

#[tokio::test]
async fn account_resume_does_not_mistake_selection_diagnostics_for_an_account_switch() {
    let root = broca_test_support::scratch_root().join("account-limit-selection-diagnostics");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), br#"{"now_ms":1000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let leases = FileLeaseStore::new(root.join("leases"));
    let session = BindIdentity::new(root.join("project"), "h", "diagnostics");
    let request = req(&session, "hello");
    let provider = Limited::new(false, true);
    let plane = StubToolPlane::new();
    let (paused, _) = drive(
        request.clone(),
        &provider,
        &plane,
        &leases,
        &wal,
        RunControl {
            account_limit_pause: true,
            clock: Some(clock.clone()),
            auth_selection: Some(
                serde_json::from_value(json!({"credential_id":"oauth:stub:work"})).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(paused.paused, Some(PauseReason::AccountLimit));
    std::fs::write(root.join("test-clock"), br#"{"now_ms":61000}"#).unwrap();
    clock.refresh().unwrap();
    let (summary, _) = drive(request, &provider, &plane, &leases, &wal, RunControl {
        clock:Some(clock), auth_selection:Some(serde_json::from_value(json!({"credential_id":"oauth:stub:work", "account_decision":{"path":"disabled"}})).unwrap()), ..Default::default()
    }).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(provider.sent.lock().unwrap().len(), 2);
    assert!(!records_of(&wal, &session)
        .iter()
        .any(|record| matches!(record, WalRecord::RunAuthRefrozen { .. })));
}
