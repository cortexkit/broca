//! Keeps a request's output reservation inside the run's context limit.
//!
//! A provider checks `input + max_output_tokens` against the model's context window, and
//! some (Meta's Responses endpoint, for one) refuse a request that crosses it with a generic
//! `400 invalid parameters` rather than a context error, which the runner classes `permanent`.
//! A long session that creeps up on its window therefore fails with an error nobody compacts
//! for. Before each step the run loop estimates the request's input, and when the frozen output
//! cap would push the request past the context limit it lowers the step's cap to what fits. When
//! not even [`OutputLimits::floor`] fits, the run ends `context_overflow` without sending, so the
//! caller compacts instead of retrying into the same refusal.
//!
//! The estimate is the previous step's measured input (exact, as the provider counted it) plus
//! the bytes the request grew by since, converted at
//! [`crate::compaction::ESTIMATE_BYTES_PER_TOKEN`] (two bytes per token). That rate overstates
//! tokens for ordinary text, code and JSON (which run at three to eight bytes per token), so the
//! estimate errs high. [`margin`] covers what it does not see: content denser than two bytes per
//! token and any provider-side accounting the reported usage leaves out.
//!
//! A lowered cap is recorded in the session's log (`output_reservation_clamped`, must-understand
//! feature `output-reservation/v1`) before the request is sent. A resume of the step reuses it,
//! and every rebuild of the step's request from the log (a cache warm, a cache dump) reads it
//! back, so the clamp never depends on state a rebuild cannot see. A step that keeps the frozen
//! cap writes nothing and renders exactly as before.

use broca_protocol::{ModelHandle, StepId};

/// The smallest output cap the loop lowers a request to when the request did not ask for less.
/// Below this a reasoning model typically spends the whole reservation thinking and returns a
/// `length` finish with no usable answer, so the run is better compacted than sent.
pub const MIN_USEFUL_OUTPUT_TOKENS: u32 = 4_096;

/// The output cap a provider's requests carry and the smallest it may be lowered to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLimits {
    /// The run's frozen cap: what every request carries unless a step lowers it.
    pub max: u32,
    /// The smallest cap a step may be lowered to; never above `max`.
    pub floor: u32,
}

impl OutputLimits {
    /// Limits for a frozen cap `max` whose provider additionally needs at least `required`
    /// (Anthropic's `max_tokens` must exceed the thinking budget, for one). The floor is
    /// [`MIN_USEFUL_OUTPUT_TOKENS`] or `required`, whichever is larger, and never above `max`:
    /// a caller who asked for fewer tokens than the useful minimum keeps exactly what it asked
    /// for or nothing.
    pub fn new(max: u32, required: u32) -> Self {
        Self {
            max,
            floor: MIN_USEFUL_OUTPUT_TOKENS.max(required).min(max),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Measured {
    /// The step's full input occupancy as the provider reported it.
    input_tokens: u64,
    /// The rendered body length of the attempt that produced that usage.
    request_bytes: u64,
    model: ModelHandle,
}

/// What the loop needs from the session's log to estimate the next request's input and to reuse
/// a cap a step already recorded. Folded from replayed records at run start and from the
/// records the loop writes as it runs, so the live value and the replayed value agree.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutputReservationHistory {
    current_model: Option<ModelHandle>,
    /// The body length of the latest attempt not yet closed by a `ModelStepFinished`.
    attempt_bytes: Option<u64>,
    /// The latest completed step whose input and body length are both known.
    measured: Option<Measured>,
    /// The cap the current run recorded for one of its steps, until that step finishes.
    recorded: Option<(StepId, u32)>,
}

impl OutputReservationHistory {
    /// A run's `RunStarted`: the run loop calls this for the record it writes itself.
    pub fn start_run(&mut self, model: &ModelHandle) {
        self.current_model = Some(model.clone());
        self.attempt_bytes = None;
        // Step ids restart with each run, so a cap another run recorded never applies.
        self.recorded = None;
    }

    /// A `ModelAttemptFinished` record with this rendered body length.
    pub fn note_attempt(&mut self, request_body_len: u64) {
        self.attempt_bytes = Some(request_body_len);
    }

    /// A `ModelStepFinished` record with usage measured on `model`, or `None` when the
    /// serving model is unknown. A different model's tokenizer cannot supply an estimate.
    pub fn note_step(&mut self, usage: broca_protocol::Usage, model: Option<&ModelHandle>) {
        // A step without measured input (or without an attempt record) leaves the older
        // measurement in place: its input plus the growth since is still an estimate.
        if let (Some(input_tokens), Some(request_bytes), Some(model)) =
            (usage.fill_tokens(), self.attempt_bytes, model.cloned())
        {
            self.measured = Some(Measured {
                input_tokens,
                request_bytes,
                model,
            });
        }
        self.attempt_bytes = None;
        self.recorded = None;
    }

    /// An `OutputReservationClamped` record the run loop writes itself.
    pub fn note_recorded(&mut self, step_id: StepId, max_output_tokens: u32) {
        self.recorded = Some((step_id, max_output_tokens));
    }

    /// The cap this run already recorded for `step_id`, when a resume re-runs that step.
    pub fn recorded_for(&self, step_id: StepId) -> Option<u32> {
        self.recorded
            .filter(|(recorded, _)| *recorded == step_id)
            .map(|(_, cap)| cap)
    }
}

/// The `provider_code` of [`output_reservation_overflow`].
pub const OUTPUT_RESERVATION_OVERFLOW_CODE: &str = "output_reservation_exceeds_context";
