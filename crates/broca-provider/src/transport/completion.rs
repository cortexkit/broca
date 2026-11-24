//! Whether a stream's events hold a whole step: the assembled message and its `Finished`
//! control, and whether the step's last record already arrived.

use super::*;

/// Whether the stream has closed its step, so a body that then fails or stalls lost nothing:
/// either its last record is the one the grammar ends a step with, after which no record can
/// change it (Anthropic's `message_stop`, a Responses `response.completed` or
/// `response.incomplete`), and the step's message and `Finished` have arrived; or the grammar
/// reports its own end-of-step record arrived ([`StreamNormalizer::step_ended`]: OpenAI Chat's
/// `[DONE]`, Bedrock's `messageStop` and `metadata`), and `finish` closes the step from what it
/// holds. Gemini names no such record, so its steps close only at the body's clean end.
pub(super) fn step_closed(
    normalizer: &dyn StreamNormalizer,
    completion: &[NormalizedEvent],
) -> bool {
    normalizer.step_ended()
        || (matches!(
            normalizer.last_event_type(),
            Some("message_stop" | "response.completed" | "response.incomplete")
        ) && completion_holds_step(completion))
}

/// Whether `completion` holds an assembled assistant message and a `Finished` control.
pub(super) fn completion_holds_step(completion: &[NormalizedEvent]) -> bool {
    completion_parts(completion) == (true, true)
}

/// Whether `completion` holds an assembled assistant message, and whether it holds a
/// `Finished` control.
pub(super) fn completion_parts(completion: &[NormalizedEvent]) -> (bool, bool) {
    let has_message = completion.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::AssistantMessage { .. })
        )
    });
    let has_finished = completion.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::Finished { .. })
        )
    });
    (has_message, has_finished)
}

pub(super) fn validate_attempt_completion(
    completion: Vec<NormalizedEvent>,
) -> Result<Vec<NormalizedEvent>, AttemptFailure> {
    let (has_message, has_finished) = completion_parts(&completion);
    if has_message && has_finished {
        return Ok(completion);
    }

    let missing = match (has_message, has_finished) {
        (false, false) => "an assembled assistant message and Finished control",
        (false, true) => "an assembled assistant message",
        (true, false) => "a Finished control",
        (true, true) => unreachable!(),
    };
    Err(AttemptFailure::transport(
        transport_error(&format!("provider stream ended without {missing}")),
        AttemptOutcome::EofBeforeCompletion,
    ))
}

pub(super) fn is_step_completion_event(event: &NormalizedEvent) -> bool {
    matches!(
        event,
        NormalizedEvent::Control(
            ProviderControl::AssistantMessage { .. }
                | ProviderControl::ToolArgsTrailingGarbage { .. }
                | ProviderControl::StepFinished { .. }
                | ProviderControl::Finished { .. }
        )
    )
}

pub(super) fn tool_args_cut(events: &[NormalizedEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { .. })
        )
    })
}
