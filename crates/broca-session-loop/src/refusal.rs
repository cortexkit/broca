//! The client-side refusal fallback follows the schedule in `broca_core::refusal_fallback`.
//! A run whose provider froze a fallback model sends a step there while a refusal's schedule
//! says so. When the run's own model refuses a step with a content filter, the run re-sends
//! that same step there once. A run without a frozen fallback never takes any of these
//! paths, and its records stay unchanged.

use broca_core::provider::Provider;
use broca_core::refusal_fallback::{self, CommittedStep, StepRoute};
use broca_protocol::content::{AssistantMessage, ContentBlock};
use broca_protocol::request::{CallOptions, ModelHandle};
use broca_provider::wire::FrozenRenderConfig;
use broca_records::{record::RefusalFallbackSchedule, record::StepRouteReason};
use serde::Deserialize;

use crate::run::LoopError;
use crate::writer::Writer;

/// Where one step goes.
pub(crate) struct Route {
    /// The model the run froze for refusals. `None` when it froze none.
    pub fallback: Option<ModelHandle>,
    pub route: StepRoute,
    pub reason: StepRouteReason,
    /// The step was refused and re-sent already, live or before a crash: it goes to the
    /// fallback, and a refusal there passes through rather than being re-sent again.
    pub resent: bool,
    /// The newest step's prompt fits the fallback model's window, so it may go there.
    pub fits: bool,
}

impl Route {
    pub fn fallback_target(&self) -> Option<ModelHandle> {
        (self.route == StepRoute::Fallback)
            .then(|| self.fallback.clone())
            .flatten()
    }

    /// Decides the step's route from the session's schedule before each step and tells the
    /// provider which model it renders for.
    pub fn decide<W: Writer, P: Provider>(
        writer: &W,
        provider: &P,
        step_id: u64,
        fallback_allowed: bool,
    ) -> Result<Self, LoopError> {
        let configured = provider.refusal_fallback_model();
        let fallback = configured.clone().filter(|_| fallback_allowed);
        let operations = writer.operations();
        let (route, resent, fits) = decide_at(
            operations,
            step_id,
            fallback.is_some(),
            provider.refusal_fallback_context_limit(),
        )?;
        if configured.is_some() {
            provider.set_step_route(route);
        }
        Ok(Self {
            fallback,
            route,
            reason: if resent {
                StepRouteReason::Refusal
            } else {
                StepRouteReason::Schedule
            },
            resent,
            fits,
        })
    }

    /// The fallback model to re-send this step to, when the run's own model just refused it:
    /// a content filter on a step without tool calls, sent to the run's model, not re-sent
    /// before, and small enough for the fallback's window. `None` otherwise, and the answer
    /// commits as it is.
    pub fn resend_to(&self, message: &AssistantMessage, refused: bool) -> Option<&ModelHandle> {
        let fallback = self.fallback.as_ref()?;
        (refused
            && self.route == StepRoute::Source
            && message.tool_calls().next().is_none()
            && !self.resent
            && self.fits)
            .then_some(fallback)
    }

    /// Sends the rest of this step to the fallback, after its refusal is durable.
    pub fn reroute<P: Provider>(&mut self, provider: &P) {
        self.route = StepRoute::Fallback;
        self.reason = StepRouteReason::Refusal;
        self.resent = true;
        provider.set_step_route(StepRoute::Fallback);
    }

    /// The model that answers the step: the fallback on a fallback step, else `model`.
    pub fn model<'a>(&'a self, model: &'a ModelHandle) -> &'a ModelHandle {
        match (&self.fallback, self.route) {
            (Some(fallback), StepRoute::Fallback) => fallback,
            _ => model,
        }
    }

    /// What the committed step records about the fallback, and whether it is a refusal after
    /// a completed tool call: that one commits as a tool step and arms the schedule, so the
    /// next step goes to the fallback. `now` is the time the schedule records.
    pub fn committed<W: Writer>(
        &self,
        writer: &W,
        model: &ModelHandle,
        message: &AssistantMessage,
        refused: bool,
        served_model: Option<&str>,
        now: Option<u64>,
    ) -> Result<Committed, LoopError> {
        let Some(fallback) = &self.fallback else {
            return Ok(Committed::default());
        };
        let current = schedule(writer)?;
        let after_tool_use =
            self.route == StepRoute::Source && refused && message.tool_calls().next().is_some();
        let next = if after_tool_use {
            Some(refusal_fallback::on_source_refusal(
                current.as_ref(),
                model,
                fallback,
                now,
            ))
        } else {
            // A handoff is not a clean reply from the run's own model, even when the model it
            // handed to has the requested model's name.
            let served_by_server_side_fallback = served_model.is_some()
                || message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ProviderFallback { .. }));
            refusal_fallback::on_step_committed(
                current.as_ref(),
                CommittedStep {
                    route: self.route,
                    refused,
                    served_by_server_side_fallback,
                },
                now,
            )
        };
        Ok(Committed {
            route: (self.route == StepRoute::Fallback).then(|| broca_records::record::StepRoute {
                model: fallback.clone(),
                reason: self.reason,
            }),
            schedule: next,
            after_tool_use,
        })
    }
}

fn decide_at(
    operations: &broca_session_store::fold::operations::Operations,
    step_id: u64,
    available: bool,
    limit: Option<u64>,
) -> Result<(StepRoute, bool, bool), LoopError> {
    let resent = operations
        .run
        .as_ref()
        .is_some_and(|run| run.refused_step == Some(step_id));
    let fill = operations
        .estimate
        .as_ref()
        .map(crate::compaction::history)
        .and_then(|history| history.previous_native_usage)
        .and_then(|usage| usage.fill_tokens());
    let fits = limit.zip(fill).is_none_or(|(limit, fill)| fill < limit);
    let scheduled = resent
        || refusal_fallback::route(schedule_at(operations)?.as_ref(), true) == StepRoute::Fallback;
    let route = if available && scheduled && fits {
        StepRoute::Fallback
    } else {
        StepRoute::Source
    };
    Ok((route, resent, fits))
}

/// The route at a durable request cut, without changing a live provider's state.
pub(crate) fn recorded_route(
    operations: &broca_session_store::fold::operations::Operations,
    config: &serde_json::Value,
    step: u64,
    opts: &CallOptions,
) -> Result<Option<broca_records::record::StepRoute>, LoopError> {
    if !carries_notes(config, opts) {
        return Ok(None);
    }
    let Some(fallback) = config
        .get("refusal_fallback")
        .filter(|fallback| !fallback.is_null())
    else {
        return Ok(None);
    };
    let limit = fallback
        .get("context_limit")
        .and_then(serde_json::Value::as_u64);
    let (route, resent, _) = decide_at(operations, step, true, limit)?;
    if route == StepRoute::Source {
        return Ok(None);
    }
    let model = serde_json::from_value(fallback["model"].clone())
        .map_err(|error| LoopError::Parts(format!("the frozen fallback model: {error}")))?;
    Ok(Some(broca_records::record::StepRoute {
        model,
        reason: if resent {
            StepRouteReason::Refusal
        } else {
            StepRouteReason::Schedule
        },
    }))
}

pub(crate) fn carries_notes(config: &serde_json::Value, opts: &CallOptions) -> bool {
    if !opts
        .prompt
        .iter()
        .any(|message| message.tags.plan_note || message.tool_addition.is_some())
    {
        return true;
    }
    let Ok(frozen) = FrozenRenderConfig::deserialize(config) else {
        return false;
    };
    frozen.refusal_fallback.as_deref().is_some_and(|fallback| {
        fallback.frozen.family_id == frozen.family_id
            && broca_provider::turn_system::tool_addition::route_carries(
                &fallback.frozen,
                &fallback.model.provider_module_id,
                opts,
            )
    })
}

/// What a committed step records about the refusal fallback.
#[derive(Default)]
pub(crate) struct Committed {
    /// The fallback model and why the step went there, on a fallback step.
    pub route: Option<broca_records::record::StepRoute>,
    /// The session's schedule after the step, while one exists.
    pub schedule: Option<RefusalFallbackSchedule>,
    pub after_tool_use: bool,
}

/// The session's refusal-fallback schedule.
pub(crate) fn schedule<W: Writer>(
    writer: &W,
) -> Result<Option<RefusalFallbackSchedule>, LoopError> {
    schedule_at(writer.operations())
}

fn schedule_at(
    operations: &broca_session_store::fold::operations::Operations,
) -> Result<Option<RefusalFallbackSchedule>, LoopError> {
    operations
        .refusal_schedule
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(format!("refusal-fallback schedule: {e}")))
}
