//! A step's output reservation: today's `OutputReservationHistory`, rebuilt from the facts the
//! operational fold keeps, so the new loop lowers a step's output cap exactly where today's
//! loop does, with today's own estimate and decision.

use broca_core::output_reservation::{OutputReservationHistory, Reservation};
use broca_core::provider::Provider;
use broca_protocol::request::CallOptions;
use broca_protocol::{ProviderError, StepId};
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{OutputCapLowered, Record};

use crate::run::LoopError;
use crate::writer::Writer;

/// What a step sends after its output reservation is decided.
pub enum Reserved {
    /// Send these options: the step's cap is the frozen one, or a lowered one now durable.
    Send(Box<CallOptions>),
    /// Not even the provider's smallest cap fits beside the estimated input, so the request
    /// isn't sent and the run ends with this error.
    Overflow(Box<ProviderError>),
}

/// Decides step `step_id`'s output cap before its request is sent, as today's loop does: the
/// estimated input plus the frozen cap must stay inside the step's context limit, or the cap is
/// lowered, or the run ends without sending when even the provider's floor doesn't fit. The
/// input is estimated only from a measured earlier step on the step's model. A cap the step
/// already recorded (an earlier attempt, before a crash) is what the provider saw, so it is sent
/// again, and only a fresh estimate that needs less lowers it further. A lowered cap is durable
/// before the request goes out.
///
/// `model` and `context_limit` are the step's: the run's own, or the refusal-fallback model's
/// on a step sent there.
///
/// `renders` is false when the step's request can't be rendered, because what its rendering
/// waits for failed (`Seams::model_call_ready`). The input is then not estimated, so the cap is
/// the frozen one or the one already recorded; the step's model call fails before sending.
pub async fn reserve<W: Writer, P: Provider>(
    writer: &mut W,
    provider: &P,
    step_id: u64,
    mut opts: CallOptions,
    model: &broca_protocol::request::ModelHandle,
    context_limit: Option<u64>,
    renders: bool,
) -> Result<Reserved, LoopError> {
    let history = history(writer.operations());
    let recorded = history.recorded_for(StepId(step_id));
    let mut fresh = None;
    if let (Some(limits), Some(context_limit)) = (provider.output_limits(), context_limit) {
        // The estimate sizes the whole request, so it is made only when a measured earlier step
        // on this model makes one possible.
        let estimate = if renders && history.has_measurement(model) {
            let bytes = provider
                .request_len(&opts)
                .map_err(|error| LoopError::Parts(error.message))?;
            history.estimate(bytes, model)
        } else {
            None
        };
        if let Some(estimate) = estimate {
            match broca_core::output_reservation::decide(limits, context_limit, estimate) {
                Reservation::Overflow { available, floor } => {
                    return Ok(Reserved::Overflow(Box::new(
                        broca_core::output_reservation::output_reservation_overflow(
                            estimate,
                            available,
                            floor,
                            context_limit,
                        ),
                    )));
                }
                Reservation::Clamped { max_output_tokens } => {
                    fresh = Some((max_output_tokens, estimate, context_limit));
                }
                Reservation::Unchanged => {}
            }
        }
    }
    let cap = match (recorded, fresh) {
        (Some(recorded), Some((fresh, ..))) => Some(recorded.min(fresh)),
        (Some(recorded), None) => Some(recorded),
        (None, Some((fresh, ..))) => Some(fresh),
        (None, None) => None,
    };
    if let Some(cap) = cap {
        if recorded != Some(cap) {
            let (_, estimated_input_tokens, context_limit) =
                fresh.expect("a cap below the recorded one comes from a fresh estimate");
            writer
                .append(
                    &[Record::OutputCapLowered(OutputCapLowered {
                        step_id,
                        max_output_tokens: cap,
                        estimated_input_tokens,
                        context_limit,
                    })],
                    true,
                )
                .await?;
        }
        if let Some(step_context) = opts.step_context.as_mut() {
            step_context.max_output_tokens = Some(cap);
        }
    }
    Ok(Reserved::Send(Box::new(opts)))
}

/// Today's history from the session's facts: the run's model, the newest measured step, the
/// body length of an attempt no step's end has closed yet, and a cap the current run recorded
/// for a step that hasn't ended.
pub fn history(operations: &Operations) -> OutputReservationHistory {
    let mut history = OutputReservationHistory::default();
    // A measured step is noted the way a step's end notes it: after the attempt that sent it.
    if let Some(measure) = &operations.output_measure {
        history.note_attempt(measure.request_bytes);
        history.note_step(
            broca_protocol::Usage {
                input_tokens: Some(measure.input_tokens),
                ..Default::default()
            },
            Some(&measure.model),
        );
    }
    if let Some(model) = operations
        .estimate
        .as_ref()
        .and_then(|estimate| estimate.run_model.as_ref())
    {
        history.start_run(model);
    }
    if let Some(bytes) = operations.output_attempt_bytes {
        history.note_attempt(bytes);
    }
    if let Some(cap) = operations
        .run
        .as_ref()
        .and_then(|run| run.output_cap.as_ref())
    {
        history.note_recorded(broca_protocol::StepId(cap.step_id), cap.max_output_tokens);
    }
    history
}
