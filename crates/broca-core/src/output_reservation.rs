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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    pub fn fold(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { config, .. } => self.start_run(&config.model),
            WalRecord::ModelAttemptFinished {
                request_body_len, ..
            } => self.note_attempt(*request_body_len),
            WalRecord::ModelStepFinished {
                usage, fallback, ..
            } => {
                let model = fallback
                    .route
                    .as_ref()
                    .map(|route| &route.model)
                    .or(self.current_model.as_ref())
                    .cloned();
                self.note_served_step(
                    *usage,
                    model.as_ref(),
                    fallback.served_model.as_deref(),
                    fallback.usage_iterations.as_deref(),
                );
            }
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

    /// Use the last serving iteration's prompt occupancy, never the sum of prompts
    /// sent to different tokenizers. Bind the measurement to that iteration's model.
    pub fn note_served_step(
        &mut self,
        usage: broca_protocol::Usage,
        requested: Option<&ModelHandle>,
        served_model: Option<&str>,
        iterations: Option<&[broca_protocol::usage::UsageIteration]>,
    ) {
        let measured = requested.and_then(|requested| {
            broca_protocol::usage::model_usage(
                served_model.unwrap_or(&requested.model_id),
                usage,
                iterations,
            )
            .last()
            .map(|(model, usage)| {
                (
                    *usage,
                    ModelHandle::new(&requested.provider_module_id, *model),
                )
            })
        });
        match measured {
            Some((usage, model)) => self.note_step(usage, Some(&model)),
            None => self.note_step(broca_protocol::Usage::default(), None),
        }
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

/// The `provider_code` of [`output_reservation_overflow`].
pub const OUTPUT_RESERVATION_OVERFLOW_CODE: &str = "output_reservation_exceeds_context";

#[cfg(test)]
mod tests {
    use super::*;

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
