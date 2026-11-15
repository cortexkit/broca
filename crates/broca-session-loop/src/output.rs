//! A step's output cap: the frozen one, or the one the step already recorded. [`history`]
//! rebuilds the old runner's `OutputReservationHistory` from the facts the session's
//! operations fold keeps; the storage converter compares it with the one the old log gives.

use broca_core::output_reservation::OutputReservationHistory;
use broca_protocol::request::CallOptions;
use broca_protocol::StepId;
use broca_session_store::fold::operations::Operations;

/// Step `step_id`'s request with its output cap: the frozen one, or the cap the step recorded
/// before a crash, which is what the provider saw then and so is sent again.
///
/// The cap is never lowered from a guess at the request's size. A byte count can't say what the
/// provider counts, and Claude 4.5 and newer accept a prompt and cap past the window anyway,
/// stopping generation at the window instead.
pub fn reserve(operations: &Operations, step_id: u64, mut opts: CallOptions) -> CallOptions {
    if let Some(cap) = history(operations).recorded_for(StepId(step_id)) {
        if let Some(step_context) = opts.step_context.as_mut() {
            step_context.max_output_tokens = Some(cap);
        }
    }
    opts
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
