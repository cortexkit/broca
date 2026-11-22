//! What a fork attempt's model call spent, written to the usage export (llm-runner 0.8.42 §9.20
//! `fork_answer.spend`).
//!
//! Ordinary run spend facts come from run records, handed to
//! `broca_core::billing::compute_segments_for_run` through the export writer's `emit_run`. A fork
//! attempt is billed by the same builder and writer, using the records a one-step run would
//! write, under the attempt's own run identity (`ForkCallJob::run_id`) and the fork's own
//! session. The head's runs and session are not used for billing. The run's config is the
//! head's config at the fork's base, so the charge basis and account come from the credential
//! the fork is pinned to. Neither the fact nor the attempt's step event names the fork; its
//! own session and run keep its spend apart.

use broca_core::billing::{compute_segments_for_run, BillingEvent, BillingRecord, SpendSegment};
use broca_core::ChargeBasisResolver;
use broca_protocol::{BindIdentity, RunFinishReason, RunId, StepId};
use broca_records::{record::RunConfig, record::StepFallback, record::StepRoute};

use crate::executor::ForkStepSpend;

/// One fork attempt's model call, as its spend facts are built from it.
pub struct AttemptSpend<'a> {
    /// The fork's own session.
    pub session: &'a BindIdentity,
    /// The attempt's own run identity.
    pub run_id: &'a str,
    /// The head's config at the fork's base: model, account and credential.
    pub config: &'a RunConfig,
    /// The refusal-fallback route the base was sent on, when it was.
    pub route: Option<&'a StepRoute>,
    pub spend: &'a ForkStepSpend,
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    /// Whether the attempt ended in `model_error`.
    pub failed: bool,
}

impl AttemptSpend<'_> {
    /// The spend facts the usage export stores for the attempt, as its writer computes them.
    pub fn segments(
        &self,
        source_instance_id: &str,
        resolver: &dyn ChargeBasisResolver,
    ) -> Vec<SpendSegment> {
        struct Resolver<'a>(&'a dyn ChargeBasisResolver);
        impl ChargeBasisResolver for Resolver<'_> {
            fn resolve_charge_basis(
                &self,
                provider_id: &str,
                method: &str,
            ) -> (broca_core::ChargeBasis, u32) {
                self.0.resolve_charge_basis(provider_id, method)
            }

            fn charge_basis_overrides(&self) -> String {
                self.0.charge_basis_overrides()
            }
        }
        compute_segments_for_run(
            &self.records(),
            &RunId(self.run_id.to_owned()),
            source_instance_id,
            &Resolver(resolver),
        )
    }

    /// The records of a one-step run that made this attempt's model call: its start, its step and
    /// its end.
    pub fn records(&self) -> Vec<BillingRecord> {
        let started = BillingEvent::RunStarted {
            run_id: RunId(self.run_id.to_owned()),
            session: self.session.clone(),
            config: Box::new(self.config.clone()),
            ts_ms: Some(self.started_at_ms),
            scope: None,
        };
        let step = BillingEvent::StepFinished {
            step_id: StepId(1),
            written_at_ms: Some(self.finished_at_ms),
            usage: self.spend.usage,
            retries_used: 0,
            service_tier_used: self.spend.service_tier_used.clone(),
            speed_used: None,
            fallback: Box::new(StepFallback {
                route: self.route.cloned(),
                ..StepFallback::default()
            }),
        };
        let finished = BillingEvent::RunFinished {
            reason: if self.failed {
                RunFinishReason::Error
            } else {
                RunFinishReason::Completed
            },
            ts_ms: Some(self.finished_at_ms),
            retries_used: 0,
        };
        [started, step, finished]
            .into_iter()
            .zip(1..)
            .map(|(event, seq)| BillingRecord { seq, event })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::ChargeBasis;
    use broca_protocol::content::FinishReason;
    use broca_protocol::request::{GenerationConfig, ResponseFormat, ToolChoice};
    use broca_protocol::{ModelHandle, Usage};
    use broca_records::record::AuthSelection;

    fn head_config() -> RunConfig {
        RunConfig {
            global_system: None,
            account_route: None,
            route_decision_id: None,
            served_provider: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("anthropic", "claude-opus-4-8"),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: Some(AuthSelection {
                billing_vault_facts: None,
                auth_method: Some("oauth".into()),
                credential_id: "oauth:anthropic:synthetic-work".into(),
                account_ref: Some("synthetic-account".into()),
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }
    }

    fn spend() -> ForkStepSpend {
        ForkStepSpend {
            usage: Usage {
                input_tokens: Some(1200),
                cached_input_tokens: Some(1000),
                output_tokens: Some(40),
                ..Usage::default()
            },
            service_tier_used: None,
            finish_reason: Some(FinishReason::Stop),
        }
    }

    /// Subscription spend for OAuth credentials, metered spend otherwise: the charge basis a
    /// fact names follows the credential's method.
    fn resolver(_provider: &str, method: &str) -> (ChargeBasis, u32) {
        if method == "oauth" {
            (ChargeBasis::SubscriptionIncluded, 7)
        } else {
            (ChargeBasis::MeteredApi, 7)
        }
    }

    #[test]
    fn a_fork_attempt_is_billed_under_its_own_run_and_session_on_the_head_credential() {
        let head = BindIdentity::new("/synthetic/project", "runner", "head");
        let fork = BindIdentity::new(
            "/synthetic/project",
            "runner",
            "head:fork:fork-72f6bdfa7eb7fb4cdf456d38c4d99623",
        );
        let config = head_config();
        let spend = spend();
        let attempt = |session, run_id, failed| AttemptSpend {
            session,
            run_id,
            config: &config,
            route: None,
            spend: &spend,
            started_at_ms: 1_790_000_000_000,
            finished_at_ms: 1_790_000_001_500,
            failed,
        };
        let resolver = resolver as fn(&str, &str) -> (ChargeBasis, u32);
        let segments = attempt(
            &fork,
            "fork:1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0",
            false,
        )
        .segments("source", &resolver);
        let [segment] = segments.as_slice() else {
            panic!("one fact per attempt: {segments:?}");
        };
        assert_eq!(
            segment.run_id,
            RunId("fork:1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0".into())
        );
        assert_eq!(segment.session, fork);
        assert_eq!(segment.charge_basis, ChargeBasis::SubscriptionIncluded);
        assert_eq!(segment.account_ref.as_deref(), Some("synthetic-account"));
        assert_eq!(
            (segment.provider.as_str(), segment.model.as_str()),
            ("anthropic", "claude-opus-4-8")
        );
        assert_eq!(segment.usage.input_tokens, Some(1200));
        assert_eq!(segment.usage.output_tokens, Some(40));
        // The fact names no fork id: the fork's session and the attempt's run identify it.
        let encoded = serde_json::to_value(segment).unwrap();
        assert!(encoded.get("fork_id").is_none(), "{encoded}");

        // The same one-step run of the head's own would bill the same fields, under its own run
        // and session: nothing but those, and the id they make, tells the two apart.
        let ordinary = attempt(&head, "head-run", false).segments("source", &resolver);
        let mut ordinary = serde_json::to_value(&ordinary[0]).unwrap();
        let mut forked = encoded;
        for value in [&mut ordinary, &mut forked] {
            for member in ["run_id", "session", "fact_id"] {
                value.as_object_mut().unwrap().remove(member);
            }
        }
        assert_eq!(forked, ordinary);

        // `BROCA_FORK_SPEND_SAMPLES` names a folder: the fork attempt's fact and the ordinary
        // run's, as the usage export stores them.
        if let Some(dir) = std::env::var_os("BROCA_FORK_SPEND_SAMPLES") {
            let dir = std::path::PathBuf::from(dir).join("usage-export");
            std::fs::create_dir_all(&dir).unwrap();
            let ordinary = attempt(&head, "head-run", false).segments("source", &resolver);
            for (name, segment) in [
                ("segment.fork.json", &segments[0]),
                ("segment.ordinary.json", &ordinary[0]),
            ] {
                std::fs::write(dir.join(name), serde_json::to_vec(segment).unwrap()).unwrap();
            }
        }

        // A failed model call is still billed, as a run that ended in an error.
        let failed = attempt(&fork, "fork-failed", true).segments("source", &resolver);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].usage.input_tokens, Some(1200));
    }
}
