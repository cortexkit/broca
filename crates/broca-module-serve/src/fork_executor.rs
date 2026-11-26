// Fork calls use no run loop: they read a frozen provider, make one step, and never dispatch tools.
impl LiveLoopExecutor {
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
            // A router declaring `account_fork_of` places the fork on the head admission's own
            // account, and on the account the head moved to if it moved. One that doesn't is
            // asked for the frozen account by name, which it may not honour.
            let fork_of = routing
                .capability(cortexkit_role_routing::capabilities::ACCOUNT_FORK_OF)
                .await
                == broca_module_serve::routing_capabilities::CapabilitySupport::Declared;
            let outcome = routing
                .account_for_pinned(
                    fork_account_params(&job, fork_of),
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
                    Some(if refusal.class == ErrorClass::Transient {
                        ForkCallRefusal::AccountHeld {
                            routing_code: Some(refusal.routing_code),
                            mismatch: None,
                        }
                    } else {
                        ForkCallRefusal::AccountBlocked { routing_code: refusal.routing_code }
                    })
                }
                AccountForOutcome::AuthRequired { .. } => {
                    Some(ForkCallRefusal::AccountBlocked { routing_code: "auth_required".into() })
                }
                AccountForOutcome::SelectionDisabled { .. } => {
                    Some(ForkCallRefusal::AccountBlocked { routing_code: "account_selection_disabled".into() })
                }
                AccountForOutcome::Unreachable(_) | AccountForOutcome::NoDecisionWithinBudget => {
                    // Routing may have committed the admission before the reply was lost.
                    // The manager keeps it owed so it can release by admission id on reopen.
                    self.note_fork_release(
                        &job.admission_id,
                        Err("routing's admission decision is unknown".into()),
                    );
                    Some(ForkCallRefusal::AccountHeld {
                        routing_code: None,
                        mismatch: None,
                    })
                }
            };
            if let Some(refusal) = refusal {
                if let Some((account, credential)) = &admitted_account {
                    let released = routing
                        .finish_fork(&job.admission_id, &job.run_id, Some((account, credential)))
                        .await;
                    self.note_fork_release(&job.admission_id, released);
                }
                return ForkCallOutcome::Refused(refusal);
            }
        }

        // Keep all post-admission failures inside this boundary so even a render or vault
        // failure releases the fork's admission before returning to its manager.
        let outcome = self.send_fork(&job).await;
        if let (Some(routing), Some((account, credential))) = (routing, &admitted_account) {
            let released = routing
                .finish_fork(&job.admission_id, &job.run_id, Some((account, credential)))
                .await;
            self.note_fork_release(&job.admission_id, released);
        }
        outcome
    }

    /// Keeps a possibly granted admission owed until routing confirms its release.
    fn note_fork_release(&self, admission_id: &str, released: Result<(), String>) {
        if let Err(error) = released {
            tracing::warn!(target: "fork", admission_id, %error,
                "could not confirm a fork admission's release; release is retried when the fork next opens");
            self.unreleased_forks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(admission_id.to_owned());
        }
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
            &resolution.credential_hosts,
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
        if let Some(media) = &job.media {
            media.install(provider.as_ref());
        }
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

fn fork_is_routed(config: &broca_records::record::RunConfig) -> bool {
    config
        .auth_selection
        .as_ref()
        .and_then(|selection| selection.account_decision.as_ref())
        .is_some_and(|decision| decision.path == broca_records::record::ACCOUNT_DECISION_ROUTED)
}

/// The fork's `route.account_for` admission. With `fork_of` (the router declares
/// `account_fork_of`) and the head's admission id recorded, it names that admission instead of
/// an account, so routing gives the fork the head's exact account and never another; the
/// contract refuses `requested_account` beside it.
fn fork_account_params(
    job: &broca_module::executor::ForkCallJob,
    fork_of: bool,
) -> serde_json::Value {
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
        // A fork's question isn't the head's turn: the head's run is the turn.
        head_turn: false,
        warm_credential: None,
    });
    params.as_object_mut().unwrap().remove("candidates");
    let head_admission = selection
        .account_decision
        .as_ref()
        .and_then(|decision| decision.admission_id.as_deref())
        .filter(|_| fork_of);
    match head_admission {
        Some(admission_id) => {
            params["fork_of"] = serde_json::json!({ "admission_id": admission_id });
        }
        None => {
            params["requested_account"] = serde_json::json!({
                "account_id":selection.account_ref.as_deref().unwrap_or(&selection.credential_id),
                "credential_id":selection.credential_id,
            });
        }
    }
    params
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
    let mut step_tier: Option<String> = None;
    let mut raw_finish: Option<String> = None;
    let mut streamed = StreamedText::default();
    let report_usage = |value: &broca_protocol::Usage,
                        service_tier_used: Option<&String>,
                        finish_reason: Option<FinishReason>| {
        if let Some(report) = &job.on_step {
            report(broca_module::executor::ForkStepSpend {
                usage: *value,
                service_tier_used: service_tier_used.cloned(),
                finish_reason,
            });
        }
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
            NormalizedEvent::Display(display) => streamed.observe(&display),
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message: reply }) => {
                message = Some(reply)
            }
            NormalizedEvent::Control(ProviderControl::StepFinished {
                usage: value,
                finish_reason,
                service_tier_used,
                raw_finish_reason,
                ..
            }) => {
                usage = report_usage(&value, service_tier_used.as_ref(), Some(finish_reason));
                step_tier = service_tier_used;
                step_usage = Some(value);
                raw_finish = raw_finish_reason;
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
                    usage = report_usage(&value, step_tier.as_ref(), Some(finish_reason));
                }
                let Some(message) = message else {
                    break;
                };
                return fork_reply(
                    &message,
                    finish_reason,
                    raw_finish.as_deref(),
                    &streamed,
                    usage,
                );
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

/// The assistant text of a fork's reply as it streamed, split at its first tool call.
///
/// Some wire formats assemble a reply's text into one block ahead of its tool calls: OpenAI
/// Chat streams `content` and `tool_calls` side by side, so text the model wrote after a call
/// lands in the same block as text before it. The stream's display events keep the order the
/// model wrote in, which is what "the text before the tool call" means.
#[derive(Default)]
struct StreamedText {
    /// Text streamed before the first tool call's input.
    before_tool: String,
    /// Every text the reply streamed, in order.
    all: String,
    tool_started: bool,
    /// Display frames were dropped, so the order above may be incomplete.
    gap: bool,
}

impl StreamedText {
    fn observe(&mut self, event: &broca_protocol::display::DisplayEvent) {
        use broca_protocol::display::DisplayEvent;
        match event {
            DisplayEvent::TextDelta {
                delta,
                tool_call_id: None,
                ..
            } => {
                self.all.push_str(delta);
                if !self.tool_started {
                    self.before_tool.push_str(delta);
                }
            }
            DisplayEvent::ToolInputDelta { .. } => self.tool_started = true,
            DisplayEvent::DisplayGap { .. } => self.gap = true,
            _ => {}
        }
    }
}

/// A fork's answer to a reply that ended with `finish_reason` (and the provider's own token
/// `raw_finish`), as llm-runner `fork_answer` finishes it.
fn fork_reply(
    message: &broca_protocol::content::AssistantMessage,
    finish_reason: broca_protocol::FinishReason,
    raw_finish: Option<&str>,
    streamed: &StreamedText,
    usage: cortexkit_role_llm_runner::usage::RunUsage,
) -> broca_module::executor::ForkCallOutcome {
    use broca_module::executor::ForkCallOutcome;
    use broca_protocol::content::ContentBlock;
    let text = |blocks: &mut dyn Iterator<Item = &ContentBlock>| {
        blocks
            .filter_map(|block| match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect::<String>()
    };
    let tool = message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall(_)));
    if tool {
        // The per-call gate refuses the call; the answer is what the model wrote before it.
        // The streamed order is used when the stream carried every text of the reply in order;
        // otherwise the assembled message's text ahead of its first call.
        let assembled_before = text(
            &mut message
                .content
                .iter()
                .take_while(|block| !matches!(block, ContentBlock::ToolCall(_))),
        );
        let ordered = !streamed.gap
            && streamed.tool_started
            && streamed.all == text(&mut message.content.iter());
        return ForkCallOutcome::Answered {
            answer: if ordered {
                streamed.before_tool.clone()
            } else {
                assembled_before
            },
            finish: "tool_call_refused".into(),
            usage,
        };
    }
    match fork_finish(finish_reason, raw_finish) {
        Some(finish) => ForkCallOutcome::Answered {
            answer: text(&mut message.content.iter()),
            finish: finish.into(),
            usage,
        },
        None => ForkCallOutcome::ModelFailed {
            usage,
            error: format!(
                "provider ended the fork step with {finish_reason:?} ({})",
                raw_finish.unwrap_or("no reason given")
            ),
        },
    }
}

/// The `finish` of a fork answer that holds no tool call, or `None` when the reply is a failed
/// model call (`model_error`).
fn fork_finish(finish_reason: broca_protocol::FinishReason, raw: Option<&str>) -> Option<&'static str> {
    use broca_protocol::FinishReason;
    match finish_reason {
        FinishReason::Stop => Some("end_turn"),
        FinishReason::Length => Some("max_tokens"),
        // `Other` is a provider reason none of the named ones match, or no reason at all. A
        // head's run takes such a reply as a completed answer (its loop keys on the message,
        // not the reason), so a fork does too. The few provider tokens that land in `Other`
        // although the call failed or was refused are named here instead: a Responses
        // response that failed or was cancelled, and a Gemini image safety stop.
        FinishReason::Other => match raw {
            Some(
                "failed" | "cancelled" | "IMAGE_SAFETY" | "IMAGE_PROHIBITED_CONTENT"
                | "IMAGE_RECITATION",
            ) => None,
            _ => Some("end_turn"),
        },
        // A refusal, a provider error, or a tool-call finish without a call is a failed call.
        FinishReason::ContentFilter | FinishReason::Error | FinishReason::ToolCalls => None,
    }
}
