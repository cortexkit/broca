//! Keeps a request's output reservation inside the run's context limit.
//!
//! A provider checks `input + max_output_tokens` against the model's context window, and some
//! (Meta's Responses endpoint, for one) refuse a request that crosses it with a generic
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
//! A lowered cap is recorded in the WAL (`output_reservation_clamped`, must-understand feature
//! `output-reservation/v1`) before the request is sent. A resume of the step reuses it, and
//! every rebuild of the step's request from the WAL (a cache warm, a cache dump) reads it back,
//! so the clamp never depends on state a rebuild cannot see. A step that keeps the frozen cap
//! writes nothing and renders exactly as before.

use broca_protocol::{ModelHandle, StepId};
use broca_wal::WalRecord;

/// The smallest output cap the loop lowers a request to when the request did not ask for less.
/// Below this a reasoning model typically spends the whole reservation thinking and returns a
/// `length` finish with no usable answer, so the run is better compacted than sent.
pub const MIN_USEFUL_OUTPUT_TOKENS: u32 = 4_096;

/// The fixed part of [`margin`].
pub const MARGIN_FLOOR_TOKENS: u64 = 1_024;

/// [`margin`] grows with the window: one token in this many.
pub const MARGIN_WINDOW_DIVISOR: u64 = 256;

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

/// Tokens kept free between the estimated request and the context limit: 1,024 or 1/256 of the
/// window (4,096 for a 1M window), whichever is larger. The previous step's input is measured, so
/// only the growth since then is estimated, and that estimate already errs high; the margin
/// absorbs content that tokenizes denser than two bytes per token (CJK text, for one) and
/// framing a provider counts against the window but does not report as input.
pub fn margin(context_limit: u64) -> u64 {
    MARGIN_FLOOR_TOKENS.max(context_limit / MARGIN_WINDOW_DIVISOR)
}

/// What a step's output cap is, given its estimated input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reservation {
    /// The frozen cap fits: render the request unchanged.
    Unchanged,
    /// The frozen cap does not fit and this smaller cap does.
    Clamped { max_output_tokens: u32 },
    /// Not even the floor fits: the request must not be sent.
    Overflow { available: u64, floor: u32 },
}

/// Decide a step's output cap from its estimated input tokens.
pub fn decide(limits: OutputLimits, context_limit: u64, estimated_input: u64) -> Reservation {
    let margin = margin(context_limit);
    let available = context_limit
        .saturating_sub(estimated_input)
        .saturating_sub(margin);
    if available >= u64::from(limits.max) {
        return Reservation::Unchanged;
    }
    if available >= u64::from(limits.floor) && available > 0 {
        return Reservation::Clamped {
            // `available < max`, so it fits a u32.
            max_output_tokens: available as u32,
        };
    }
    Reservation::Overflow {
        available,
        floor: limits.floor,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Measured {
    /// The step's full input occupancy as the provider reported it.
    input_tokens: u64,
    /// The rendered body length of the attempt that produced that usage.
    request_bytes: u64,
    model: ModelHandle,
}

/// What the loop needs from the WAL to estimate the next request's input and to reuse a cap a
/// step already recorded. Folded from replayed records at run start and from the records the
/// loop writes as it runs, so the live value and the replayed value agree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub fn fold(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { config, .. } => self.start_run(&config.model),
            WalRecord::ModelAttemptFinished {
                request_body_len, ..
            } => self.note_attempt(*request_body_len),
            WalRecord::ModelStepFinished { usage, .. } => self.note_step(*usage),
            WalRecord::OutputReservationClamped {
                step_id,
                max_output_tokens,
                ..
            } => self.note_recorded(*step_id, *max_output_tokens),
            _ => {}
        }
    }

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

    /// A `ModelStepFinished` record with this usage.
    pub fn note_step(&mut self, usage: broca_protocol::Usage) {
        // A step without measured input (or without an attempt record) leaves the older
        // measurement in place: its input plus the growth since is still an estimate.
        if let (Some(input_tokens), Some(request_bytes), Some(model)) = (
            usage.fill_tokens(),
            self.attempt_bytes,
            self.current_model.clone(),
        ) {
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

    /// Use a compaction session's checkpointed history when the replayed records held no
    /// measured step (a resume whose earlier steps were materialized into a store snapshot).
    pub fn seed_from_compaction(&mut self, history: &crate::compaction::CompactionHistory) {
        if self.measured.is_some() {
            return;
        }
        if let (Some(input_tokens), Some(request_bytes), Some(model)) = (
            history.usage().and_then(|usage| usage.fill_tokens()),
            history.previous_request_bytes,
            history.previous_model.clone(),
        ) {
            self.measured = Some(Measured {
                input_tokens,
                request_bytes: request_bytes as u64,
                model,
            });
        }
    }

    /// Whether [`Self::estimate`] can estimate a request for `model`.
    pub fn has_measurement(&self, model: &ModelHandle) -> bool {
        self.measured.as_ref().is_some_and(|m| &m.model == model)
    }

    /// The estimated input tokens of a request rendered to `request_bytes` bytes for `model`.
    /// `None` without a measured earlier step on the same model: a whole-body byte conversion
    /// overstates tokens two- to three-fold, and a different model's tokenizer counts the same
    /// prefix differently, so neither is a basis for lowering a cap or refusing a request.
    pub fn estimate(&self, request_bytes: usize, model: &ModelHandle) -> Option<u64> {
        let measured = self.measured.as_ref().filter(|m| &m.model == model)?;
        Some(crate::compaction::estimate_tokens(
            request_bytes,
            Some((
                measured.input_tokens,
                usize::try_from(measured.request_bytes).unwrap_or(usize::MAX),
            )),
        ))
    }

    /// The cap this run already recorded for `step_id`, when a resume re-runs that step.
    pub fn recorded_for(&self, step_id: StepId) -> Option<u32> {
        self.recorded
            .filter(|(recorded, _)| *recorded == step_id)
            .map(|(_, cap)| cap)
    }
}

/// The cap recorded for `step_id` among `records`, searching back from the end to the current
/// run's `RunStarted` (step ids restart with each run). A rebuild of a step's request from the
/// WAL passes the records up to the attempt it reproduces.
pub fn recorded_cap(records: &[WalRecord], step_id: StepId) -> Option<u32> {
    for record in records.iter().rev() {
        match record {
            WalRecord::OutputReservationClamped {
                step_id: recorded,
                max_output_tokens,
                ..
            } if *recorded == step_id => return Some(*max_output_tokens),
            WalRecord::RunStarted { .. } => return None,
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: u64 = 1_048_576;
    const LIMITS: OutputLimits = OutputLimits {
        max: 131_072,
        floor: MIN_USEFUL_OUTPUT_TOKENS,
    };

    #[test]
    fn a_request_far_from_the_window_keeps_its_cap() {
        assert_eq!(decide(LIMITS, WINDOW, 500_000), Reservation::Unchanged);
        // Exactly fitting, margin included, is still unchanged.
        let fits = WINDOW - margin(WINDOW) - u64::from(LIMITS.max);
        assert_eq!(decide(LIMITS, WINDOW, fits), Reservation::Unchanged);
    }

    /// A step with 916,594 measured input tokens on the step before it, plus a
    /// 6,856-byte growth (a short reply and a 5 KB tool result), against a 1,048,576 window with
    /// a 131,072 cap. The frozen cap crosses the window; the clamped one does not.
    #[test]
    fn the_refused_step_gets_a_cap_that_fits_the_window() {
        let estimate = crate::compaction::estimate_tokens(5_560_631, Some((916_594, 5_553_775)));
        assert_eq!(estimate, 916_594 + 3_428);
        assert!(estimate + u64::from(LIMITS.max) > WINDOW);
        let Reservation::Clamped { max_output_tokens } = decide(LIMITS, WINDOW, estimate) else {
            panic!("expected a clamp");
        };
        assert_eq!(
            u64::from(max_output_tokens),
            WINDOW - estimate - margin(WINDOW)
        );
        assert!(estimate + u64::from(max_output_tokens) + margin(WINDOW) <= WINDOW);
    }

    #[test]
    fn a_request_that_cannot_fit_the_floor_overflows() {
        let estimate = WINDOW - margin(WINDOW) - u64::from(MIN_USEFUL_OUTPUT_TOKENS) + 1;
        assert_eq!(
            decide(LIMITS, WINDOW, estimate),
            Reservation::Overflow {
                available: u64::from(MIN_USEFUL_OUTPUT_TOKENS) - 1,
                floor: MIN_USEFUL_OUTPUT_TOKENS,
            }
        );
        assert!(matches!(
            decide(LIMITS, WINDOW, WINDOW + 10),
            Reservation::Overflow { available: 0, .. }
        ));
    }

    #[test]
    fn the_floor_never_exceeds_the_frozen_cap_and_covers_a_required_minimum() {
        assert_eq!(OutputLimits::new(1_000, 0).floor, 1_000);
        assert_eq!(OutputLimits::new(64_000, 0).floor, MIN_USEFUL_OUTPUT_TOKENS);
        assert_eq!(OutputLimits::new(64_000, 32_001).floor, 32_001);
        // A caller's explicit cap below the useful minimum is its own floor, so a request that
        // cannot fit the whole cap overflows instead of being lowered further.
        let small = OutputLimits::new(1_000, 0);
        assert!(matches!(
            decide(small, 10_000, 10_000 - 1_024 - 999),
            Reservation::Overflow { .. }
        ));
    }

    fn model(id: &str) -> ModelHandle {
        ModelHandle::new("p", id)
    }

    fn records(json: &[serde_json::Value]) -> Vec<WalRecord> {
        json.iter()
            .map(|value| serde_json::from_value(value.clone()).unwrap())
            .collect()
    }

    fn run_started(model: &str) -> serde_json::Value {
        serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":model},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}})
    }

    fn attempt(step: u64, len: u64) -> serde_json::Value {
        serde_json::json!({"type":"model_attempt_finished","step_id":step,"attempt":0,"request_body_len":len,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"bb","outcome":"complete","ts_ms":1})
    }

    fn step(step: u64, input: Option<u64>) -> serde_json::Value {
        let usage = match input {
            Some(input) => serde_json::json!({"input_tokens": input, "output_tokens": 5}),
            None => serde_json::json!({}),
        };
        serde_json::json!({"type":"model_step_finished","step_id":step,"assistant_message":{"message_id":"m","content":[]},"usage":usage,"retries_used":0})
    }

    #[test]
    fn the_estimate_needs_a_measured_step_on_the_same_model() {
        let mut history = OutputReservationHistory::default();
        assert_eq!(history.estimate(1_000, &model("m")), None);
        for record in records(&[run_started("m"), attempt(1, 400), step(1, Some(100))]) {
            history.fold(&record);
        }
        assert_eq!(history.estimate(1_000, &model("m")), Some(100 + 300));
        assert_eq!(history.estimate(1_000, &model("other")), None);
        // An unmeasured step keeps the older measurement.
        for record in records(&[attempt(2, 900), step(2, None)]) {
            history.fold(&record);
        }
        assert_eq!(history.estimate(1_000, &model("m")), Some(100 + 300));
    }

    #[test]
    fn a_recorded_cap_applies_to_its_own_step_of_its_own_run() {
        let clamp = serde_json::json!({"type":"output_reservation_clamped","step_id":2,"max_output_tokens":9000,"estimated_input_tokens":1,"context_limit":2});
        let wal = records(&[run_started("m"), clamp.clone()]);
        let mut history = OutputReservationHistory::default();
        for record in &wal {
            history.fold(record);
        }
        assert_eq!(history.recorded_for(StepId(2)), Some(9_000));
        assert_eq!(history.recorded_for(StepId(3)), None);
        assert_eq!(recorded_cap(&wal, StepId(2)), Some(9_000));
        // Once the step finishes, or another run starts, the cap no longer applies.
        let finished = records(&[attempt(2, 1), step(2, Some(1))]);
        let mut done = history.clone();
        for record in &finished {
            done.fold(record);
        }
        assert_eq!(done.recorded_for(StepId(2)), None);
        let next_run = records(&[run_started("m")]);
        history.fold(&next_run[0]);
        assert_eq!(history.recorded_for(StepId(2)), None);
        let mut later = wal.clone();
        later.extend(next_run);
        assert_eq!(recorded_cap(&later, StepId(2)), None);
    }
}
