//! A step's output reservation: today's `OutputReservationHistory`, rebuilt from the facts the
//! operational fold keeps, so the new loop lowers a step's output cap exactly where today's
//! loop does, with today's own estimate and decision.

use broca_core::output_reservation::OutputReservationHistory;
use broca_session_store::fold::operations::Operations;

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
    if let Some(cap) = operations.run.as_ref().and_then(|run| run.output_cap.as_ref()) {
        history.note_recorded(broca_protocol::StepId(cap.step_id), cap.max_output_tokens);
    }
    history
}
