// Fork calls use no run loop: they read a frozen provider, make one step, and never dispatch tools.
impl<W: ExecutorWalStore + 'static> LiveLoopExecutor<W> {
    async fn call_fork(
        &self,
        job: broca_module::executor::ForkCallJob,
    ) -> broca_module::executor::ForkCallOutcome {
        use broca_module::executor::{ForkCallOutcome, ForkCallRefusal};
        use broca_module_serve::account_routing::{AccountForOutcome, ADMISSION_BUDGET};
        let routing = self
            .routing
            .as_ref()
            .filter(|_| fork_is_routed(&job.base.config));
        let mut admitted_account = None;
        if let Some(routing) = routing {
            let selection = job
                .base
                .config
                .auth_selection
                .as_ref()
                .expect("routed selection");
            let account = selection
                .account_ref
                .as_deref()
                .unwrap_or(&selection.credential_id);
            let outcome = routing
                .account_for_pinned(
                    fork_account_params(&job),
                    tokio::time::Instant::now() + ADMISSION_BUDGET,
                )
                .await;
            let refusal = match outcome {
                AccountForOutcome::Chosen {
                    account_id,
                    credential_id,
                    service_tier_chosen,
                    ..
                } => {
                    let mismatch = if account_id != account {
                        Some("account")
                    } else if credential_id != selection.credential_id {
                        Some("credential")
                    } else if service_tier_chosen.as_deref().unwrap_or("standard")
                        != job
                            .base
                            .config
                            .service_tier
                            .map(|tier| tier.as_str())
                            .unwrap_or("standard")
                    {
                        Some("service_tier")
                    } else {
                        None
                    };
                    admitted_account = Some((account_id, credential_id));
                    mismatch.map(|mismatch| ForkCallRefusal::AccountHeld {
                        routing_code: None,
                        mismatch: Some(mismatch.into()),
                    })
                }
                AccountForOutcome::Refused(refusal) => {
                    Some(fork_routing_refusal(refusal.routing_code))
                }
                AccountForOutcome::AuthRequired { .. } => {
                    Some(fork_routing_refusal("auth_required".into()))
                }
                AccountForOutcome::SelectionDisabled { .. } => {
                    Some(fork_routing_refusal("account_selection_disabled".into()))
                }
                AccountForOutcome::Unreachable(_) | AccountForOutcome::NoDecisionWithinBudget => {
                    Some(ForkCallRefusal::AccountHeld {
                        routing_code: None,
                        mismatch: None,
                    })
                }
            };
            if let Some(refusal) = refusal {
                if let Some((account, credential)) = &admitted_account {
                    let _ = routing
                        .finish_fork(&job.admission_id, &job.run_id, Some((account, credential)))
                        .await;
                }
                return ForkCallOutcome::Refused(refusal);
            }
        }

        // Keep all post-admission failures inside this boundary so even a render or vault
        // failure releases the fork's admission before returning to its manager.
        let outcome = self.send_fork(&job).await;
        if let (Some(routing), Some((account, credential))) = (routing, &admitted_account) {
            let _ = routing
                .finish_fork(&job.admission_id, &job.run_id, Some((account, credential)))
                .await;
        }
        outcome
    }

    async fn release_fork(
        &self,
        job: broca_module::executor::ForkReleaseJob,
    ) -> Result<(), String> {
        if !fork_is_routed(&job.config) {
            return Ok(());
        }
        let routing = self
            .routing
            .as_ref()
            .ok_or_else(|| "routing is unavailable for the fork admission release".to_string())?;
        // Recovery knows the requested pin, not an unrecorded choice routing may have made.
        routing
            .finish_fork(&job.admission_id, &job.run_id, None)
            .await
    }

    async fn send_fork(
        &self,
        job: &broca_module::executor::ForkCallJob,
    ) -> broca_module::executor::ForkCallOutcome {
        use broca_module::executor::{ForkCallOutcome, ForkCallRefusal};
        let config = &job.base.config;
        let provider_id = config.model.provider_module_id.as_str();
        let mut frozen: broca_provider::wire::FrozenRenderConfig =
            match serde_json::from_value(config.frozen_render_config.clone()) {
                Ok(frozen) => frozen,
                Err(error) => {
                    return ForkCallOutcome::Refused(ForkCallRefusal::Unsupported(format!(
                        "unreadable frozen provider config: {error}"
                    )))
                }
            };
        if provider_id == "opencode-go" {
            frozen.opencode_go_session_id = Some(
                broca_module_serve::opencode_go_identity::for_session(&job.head),
            );
        }
        let selection = config.auth_selection.as_ref();
        let resolution = match self
            .decide_auth_with_account(
                provider_id,
                selection.map(|selection| selection.credential_id.as_str()),
                selection.and_then(|selection| selection.auth_method.as_deref()),
                selection.and_then(|selection| selection.resolved_credential_id.as_deref()),
                false,
                None,
                None,
                &AccountChoice::NotApplicable,
                selection.and_then(|selection| selection.account_decision.as_ref()),
            )
            .await
        {
            Ok(resolution) => resolution,
            Err(error) => {
                return ForkCallOutcome::ModelFailed {
                    usage: Default::default(),
                    error: error.to_string(),
                }
            }
        };
        if let Some(pause) = resolution.pause {
            return ForkCallOutcome::ModelFailed {
                usage: Default::default(),
                error: pause.message,
            };
        }
        let provider = match broca_catalog::live::provider_for_recorded_config_with_quota_observer(
            provider_id,
            &config.model.model_id,
            frozen,
            &resolution.mode,
            resolution.method,
            selection.and_then(|selection| selection.account_ref.as_deref()),
            resolution.account_id.as_deref(),
            self.provider_client.clone(),
            &self.clock,
            broca_module_serve::usage_observe::observer(
                provider_id,
                resolution.method.map(|method| method.as_token()),
                selection,
            ),
        ) {
            Ok(provider) => provider,
            Err(error) => return ForkCallOutcome::Refused(ForkCallRefusal::Unsupported(error)),
        };
        provider.set_step_route(if job.base.route.is_some() {
            broca_core::refusal_fallback::StepRoute::Fallback
        } else {
            broca_core::refusal_fallback::StepRoute::Source
        });
        let rendered = match provider.try_render_request(&job.base.opts) {
            Ok(bytes) => bytes,
            Err(error) => {
                return ForkCallOutcome::Refused(ForkCallRefusal::BaseUnavailable(error.message))
            }
        };
        if broca_core::render::request_digest_hex(&rendered) != job.base.request_body_sha256 {
            return ForkCallOutcome::Refused(ForkCallRefusal::BaseUnavailable(
                "the frozen base no longer renders to its recorded digest".into(),
            ));
        }
        fork_step(provider.as_ref(), job).await
    }
}

fn fork_is_routed(config: &broca_wal::RunConfig) -> bool {
    config
        .auth_selection
        .as_ref()
        .and_then(|selection| selection.account_decision.as_ref())
        .is_some_and(|decision| decision.path == broca_wal::ACCOUNT_DECISION_ROUTED)
}

fn fork_account_params(job: &broca_module::executor::ForkCallJob) -> serde_json::Value {
    use broca_module_serve::account_routing::{
        account_for_params, AccountForRequest, AccountHoldCap, Candidates,
    };
    let config = &job.base.config;
    let selection = config.auth_selection.as_ref().expect("routed selection");
    // No live credential listing or settings may change the head's frozen admission inputs.
    // This config format stores no pool or candidate-view snapshot.
    let candidates = Candidates {
        view_digest: String::new(),
        accounts: Vec::new(),
    };
    let mut params = account_for_params(&AccountForRequest {
        account_route: config.account_route.as_ref(),
        decision_id: None,
        service_tier: config.service_tier.map(|tier| tier.as_str()),
        pool: None,
        credential_id: None,
        provider: &config.model.provider_module_id,
        model: &config.model.model_id,
        conversation_key: job.conversation_key.as_deref().unwrap_or_default(),
        admission_id: &job.admission_id,
        initial_input_bytes: 0,
        candidates: &candidates,
        hold_cap: AccountHoldCap {
            max_s: 0,
            evidence_unknown: false,
        },
    });
    params.as_object_mut().unwrap().remove("candidates");
    params["requested_account"] = serde_json::json!({
        "account_id":selection.account_ref.as_deref().unwrap_or(&selection.credential_id),
        "credential_id":selection.credential_id,
    });
    params
}

fn fork_routing_refusal(code: String) -> broca_module::executor::ForkCallRefusal {
    use broca_module::executor::ForkCallRefusal;
    match code.as_str() {
        "account_hold" | "account_pool_exhausted" | "account_selection_unavailable" => {
            ForkCallRefusal::AccountHeld {
                routing_code: Some(code),
                mismatch: None,
            }
        }
        _ => ForkCallRefusal::AccountBlocked { routing_code: code },
    }
}

async fn fork_step(
    provider: &dyn broca_core::Provider,
    job: &broca_module::executor::ForkCallJob,
) -> broca_module::executor::ForkCallOutcome {
    use broca_module::executor::ForkCallOutcome;
    use broca_protocol::{FinishReason, NormalizedEvent, ProviderControl};
    use cortexkit_role_llm_runner::usage::RunUsage;
    let mut events = provider.step(job.base.step_id, &job.request).await;
    let mut message = None;
    let mut usage = RunUsage::default();
    let mut step_usage = None;
    let report_usage = |value: &broca_protocol::Usage| {
        let model = job
            .base
            .route
            .as_ref()
            .map(|route| &route.model)
            .unwrap_or(&job.base.config.model);
        let mut usage = RunUsage::default();
        usage.record_step(
            &model.provider_module_id,
            &model.model_id,
            job.base.config.variant.as_deref(),
            broca_core::step_usage::reported_step_usage(value),
        );
        if let Some(report) = &job.on_usage {
            report(usage.clone());
        }
        usage
    };
    while let Some(event) = events.recv().await {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message: reply }) => {
                message = Some(reply)
            }
            NormalizedEvent::Control(ProviderControl::StepFinished { usage: value, .. }) => {
                usage = report_usage(&value);
                step_usage = Some(value);
            }
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: value,
                finish_reason,
                ..
            }) => {
                if step_usage
                    .as_ref()
                    .is_none_or(|usage| *usage == broca_protocol::Usage::default())
                {
                    usage = report_usage(&value);
                }
                let Some(message) = message else {
                    break;
                };
                let mut answer = String::new();
                let mut tool = false;
                for block in message.content {
                    match block {
                        broca_protocol::content::ContentBlock::Text { text } => {
                            answer.push_str(&text)
                        }
                        broca_protocol::content::ContentBlock::ToolCall(_) => {
                            tool = true;
                            break;
                        }
                        _ => {}
                    }
                }
                let finish = if tool {
                    "tool_call_refused"
                } else {
                    match finish_reason {
                        FinishReason::Stop => "end_turn",
                        FinishReason::Length => "max_tokens",
                        _ => {
                            return ForkCallOutcome::ModelFailed {
                                usage,
                                error: format!(
                                    "provider ended the fork step with {finish_reason:?}"
                                ),
                            }
                        }
                    }
                };
                return ForkCallOutcome::Answered {
                    answer,
                    finish: finish.into(),
                    usage,
                };
            }
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                return ForkCallOutcome::ModelFailed {
                    usage,
                    error: error.message,
                }
            }
            _ => {}
        }
    }
    ForkCallOutcome::ModelFailed {
        usage,
        error: "provider stream ended without an assistant message and Finished control".into(),
    }
}
