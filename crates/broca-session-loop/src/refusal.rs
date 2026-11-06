//! The client-side refusal fallback, run as today's loop runs it, with today's own schedule
//! (`broca_core::refusal_fallback`). A run whose provider froze a fallback model sends a step
//! there while a refusal's schedule says so. When the run's own model refuses a step with a
//! content filter, the run re-sends that same step there once. A run without a frozen fallback
//! never takes any of these paths, and its records stay exactly as before.

use broca_core::provider::Provider;
use broca_core::refusal_fallback::{self, CommittedStep, StepRoute};
use broca_protocol::content::{AssistantMessage, ContentBlock};
use broca_protocol::request::ModelHandle;
use broca_wal::{RefusalFallbackSchedule, StepRouteReason};

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
    /// Decides the step's route from the session's schedule, as today's loop does before each
    /// step, and tells the provider which model it renders for.
    pub fn decide<W: Writer, P: Provider>(
        writer: &W,
        provider: &P,
        step_id: u64,
    ) -> Result<Self, LoopError> {
        let fallback = provider.refusal_fallback_model();
        let operations = writer.operations();
        let resent = operations
            .run
            .as_ref()
            .is_some_and(|run| run.refused_step == Some(step_id));
        // A prompt that already fills the fallback model's window would only overflow it. The
        // prompt is measured by the newest step's own model, as today's compaction history
        // measures it.
        let fill = operations
            .estimate
            .as_ref()
            .map(crate::compaction::history)
            .and_then(|history| history.previous_native_usage)
            .and_then(|usage| usage.fill_tokens());
        let fits = match (provider.refusal_fallback_context_limit(), fill) {
            (Some(limit), Some(fill)) => fill < limit,
            _ => true,
        };
        let scheduled = resent
            || refusal_fallback::route(schedule(writer)?.as_ref(), true) == StepRoute::Fallback;
        let route = if fallback.is_some() && scheduled && fits {
            StepRoute::Fallback
        } else {
            StepRoute::Source
        };
        if fallback.is_some() {
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
            route: (self.route == StepRoute::Fallback).then(|| broca_wal::StepRoute {
                model: fallback.clone(),
                reason: self.reason,
            }),
            schedule: next,
            after_tool_use,
        })
    }
}

/// What a committed step records about the refusal fallback.
#[derive(Default)]
pub(crate) struct Committed {
    /// The fallback model and why the step went there, on a fallback step.
    pub route: Option<broca_wal::StepRoute>,
    /// The session's schedule after the step, while one exists.
    pub schedule: Option<RefusalFallbackSchedule>,
    pub after_tool_use: bool,
}

/// The session's refusal-fallback schedule.
pub(crate) fn schedule<W: Writer>(
    writer: &W,
) -> Result<Option<RefusalFallbackSchedule>, LoopError> {
    writer
        .operations()
        .refusal_schedule
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(format!("refusal-fallback schedule: {e}")))
}
