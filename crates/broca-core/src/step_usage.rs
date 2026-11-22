//! Completed-step usage, generation timing and normalized provider finish reasons.

use broca_protocol::Usage;

pub use generation::StepServed;

/// One model's share of a step's usage, as `refusal_fallback::usage_by_model` splits it: the
/// provider, the model, its variant, and the usage counted under it.
pub type ModelShare<'a> = (&'a str, &'a str, Option<&'a str>, Usage);

/// The usage arithmetic of one run's completed steps, apart from how its records are read: what
/// `run.status` and `run.result` serve as the run's `usage`. The session store's projection
/// feeds it from its records, counting a run's steps, service tiers and generation timing.
#[derive(Debug, Clone, Default)]
pub struct RunUsageSteps {
    final_attempt: Option<generation::AttemptTiming>,
    tier_usage: cortexkit_role_llm_runner::service_tier::RunServiceTierUsage,
    tier_reported: bool,
    final_tier_report: Option<String>,
}

impl RunUsageSteps {
    /// The run's own start, with the render config it froze.
    pub fn run_started(
        &mut self,
        usage: &mut broca_protocol::usage::RunUsage,
        frozen_render_config: &serde_json::Value,
    ) {
        if broca_protocol::service_tier_enabled() {
            let level = broca_protocol::frozen_service_tier(frozen_render_config);
            usage.service_tier_applied = (level != "standard").then(|| level.to_owned());
            self.tier_reported = matches!(
                frozen_render_config
                    .get("family_id")
                    .and_then(serde_json::Value::as_str),
                Some(
                    "openai_chat"
                        | "openai_responses"
                        | "openai_responses_chatgpt"
                        | "anthropic_messages"
                        | "gemini"
                )
            );
        }
    }

    /// A boundary no attempt is carried across: any run's start, and each of this run's steps'
    /// starts.
    pub fn boundary(&mut self) {
        self.final_attempt = None;
        self.final_tier_report = None;
    }

    /// One of the run's HTTP attempts finished: its step, its send and content stamps, and the
    /// service tier or speed the provider reported serving it at, as the attempt's record keeps
    /// it.
    pub fn attempt(
        &mut self,
        step_id: broca_protocol::StepId,
        sent_at_ms: Option<u64>,
        first_content_at_ms: Option<u64>,
        last_content_at_ms: Option<u64>,
        served_tier: Option<&str>,
    ) {
        // Replace even an unmeasured attempt: a retry's timing never belongs to
        // the output produced by the step's final attempt.
        self.final_attempt = Some(generation::AttemptTiming {
            step_id,
            sent: sent_at_ms,
            first: first_content_at_ms,
            last: last_content_at_ms,
        });
        self.final_tier_report = if broca_protocol::service_tier_enabled() && self.tier_reported {
            served_tier.map(abstract_served_tier)
        } else {
            None
        };
    }

    /// One of the run's steps ended: finished (`served`) or refused (`None`). `by_model` is its
    /// usage split by the model each part counts under; `None` when the run's model isn't known,
    /// which counts nothing.
    pub fn step_ended(
        &mut self,
        usage: &mut broca_protocol::usage::RunUsage,
        served: Option<StepServed<'_>>,
        by_model: Option<&[ModelShare<'_>]>,
    ) {
        if broca_protocol::service_tier_enabled() {
            let mut step = cortexkit_role_llm_runner::service_tier::ServiceTierUsage::default();
            step.service_tier_applied
                .clone_from(&usage.service_tier_applied);
            if self.tier_reported {
                if let Some(served) = served {
                    step.service_tier_served = served
                        .speed_used
                        .or(served.service_tier_used)
                        .map(abstract_served_tier);
                } else {
                    // Refused calls are billed too; only a recorded attempt envelope can
                    // supply their service-tier report because older refusal records have no tier.
                    step.service_tier_served = self.final_tier_report.take();
                }
            }
            self.tier_usage
                .record_step(&step)
                .expect("applied tier is a known non-standard level or absent");
            usage.service_tier_served = self.tier_usage.usage().service_tier_served;
        }
        self.final_tier_report = None;
        // A refused call was billed, so it counts as a call of the model that refused.
        if let Some(by_model) = by_model {
            generation::record(
                &mut usage.generation,
                self.final_attempt.take(),
                served,
                by_model,
            );
            for (provider, model, variant, step) in by_model {
                let run_split =
                    extend_cache_write_split(usage.steps, usage.cache_write_split(), step);
                let model_index = usage.by_model.iter().position(|entry| {
                    entry.provider == *provider
                        && entry.model == *model
                        && entry.variant.as_deref() == *variant
                });
                let model_split = match model_index {
                    Some(index) => {
                        let entry = &usage.by_model[index];
                        extend_cache_write_split(entry.steps, entry.cache_write_split(), step)
                    }
                    None => extend_cache_write_split(0, None, step),
                };
                usage.record_step(provider, model, *variant, reported_step_usage(step));
                // The run and each model carry the 5-minute and 1-hour split only while every
                // counted step reported a complete one that sums to its cache-write total.
                let run_split = summing_cache_write_split(run_split, usage.cache_write_tokens);
                usage.cache_write_5m_tokens = run_split.map(|split| split.0);
                usage.cache_write_1h_tokens = run_split.map(|split| split.1);
                let index = model_index.unwrap_or(usage.by_model.len() - 1);
                let entry = &mut usage.by_model[index];
                let model_split = summing_cache_write_split(model_split, entry.cache_write_tokens);
                entry.cache_write_5m_tokens = model_split.map(|split| split.0);
                entry.cache_write_1h_tokens = model_split.map(|split| split.1);
            }
        }
    }
}

fn extend_cache_write_split(
    steps: u64,
    previous: Option<(u64, u64)>,
    step: &Usage,
) -> Option<(u64, u64)> {
    let step = step.with_complete_cache_write_lifetimes();
    let pair = step.cache_write_5m_tokens.zip(step.cache_write_1h_tokens)?;
    // An absent total is unmeasured, not a zero contribution. Once a counted step lacked a
    // complete pair, later reports cannot turn the aggregate's partial sum into a breakdown.
    let previous = if steps == 0 { (0, 0) } else { previous? };
    Some((
        previous.0.checked_add(pair.0)?,
        previous.1.checked_add(pair.1)?,
    ))
}

fn summing_cache_write_split(split: Option<(u64, u64)>, total: Option<u64>) -> Option<(u64, u64)> {
    split.filter(|(five, hour)| {
        five.checked_add(*hour)
            .is_some_and(|sum| Some(sum) == total)
    })
}

fn abstract_served_tier(tier: &str) -> String {
    match tier {
        "default" | "standard_only" | "STANDARD" | "DEFAULT" => "standard",
        "priority" | "PRIORITY" | "FAST" => "fast",
        "ULTRAFAST" => "ultrafast",
        other => other,
    }
    .to_owned()
}

mod generation;
pub use generation::content_timing_intervals;

pub fn reported_step_usage(usage: &Usage) -> cortexkit_role_llm_runner::usage::StepUsage {
    let mut step = cortexkit_role_llm_runner::usage::StepUsage::unreported();
    step.input_tokens = usage.input_tokens;
    step.cached_input_tokens = usage.cached_input_tokens;
    step.cache_write_tokens = usage.cache_write_tokens;
    step.output_tokens = usage.output_tokens;
    step.reasoning_tokens = usage.reasoning_tokens;
    step
}

/// A finished step's normalized finish, as `run.status` and `run.result` serve it: the
/// provider's own terminal token (`raw_finish_reason`) read by the shared finish source of the
/// family that rendered the step. That family is the run's frozen render config's, or, for a step
/// routed to the refusal-fallback model (`refusal_fallback`), the fallback config frozen inside
/// it. `made_calls` is whether the step's message asked for tools. `None` when the config names
/// no family with a shared finish source.
pub fn step_finish(
    frozen_render_config: &serde_json::Value,
    refusal_fallback: bool,
    raw_finish_reason: Option<&str>,
    made_calls: bool,
) -> Option<cortexkit_role_llm_runner::finish::Finish> {
    let config = if refusal_fallback {
        frozen_render_config.pointer("/refusal_fallback/frozen")
    } else {
        Some(frozen_render_config)
    };
    config
        .and_then(|config| config.get("family_id"))
        .and_then(serde_json::Value::as_str)
        .and_then(|family| match family {
            "anthropic_messages" => Some("anthropic.messages"),
            "openai_chat" => Some("openai.chat_completions"),
            "openai_responses" => Some("openai.responses"),
            "openai_responses_chatgpt" => Some("openai.chatgpt_responses"),
            "gemini" => Some("google.generate_content"),
            "google_codeassist" => Some("google.code_assist"),
            "bedrock_converse" => Some("aws.bedrock_converse"),
            _ => None,
        })
        .and_then(|source| {
            cortexkit_role_llm_runner::finish::normalize(source, raw_finish_reason, made_calls)
                .expect("every frozen family has a shared finish source")
        })
}
