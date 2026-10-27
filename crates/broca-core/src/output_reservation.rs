//! A step's output cap lowered below the run's frozen cap, as the WAL records it.
//!
//! A provider checks `input + max_output_tokens` against the model's context window, and some
//! (Meta's Responses endpoint, for one) refuse a request that crosses it with a generic
//! `400 invalid parameters`. A step whose request would cross it is sent with a lower cap, and
//! that cap is recorded in the WAL (`output_reservation_clamped`, must-understand feature
//! `output-reservation/v1`) before the request is sent.
//!
//! The recorded cap is part of the request the step sent. A resume of the step renders it again,
//! and every rebuild of the step's request from the WAL (a cache warm, a cache dump) reads it
//! back, so the rebuilt bytes match the recorded digest. A step without a record renders the
//! frozen cap exactly as before.

use broca_protocol::StepId;
use broca_wal::WalRecord;

/// The cap the current run recorded for one of its steps, folded from replayed records at run
/// start and kept current by the run loop as it runs, so the live value and the replayed value
/// agree.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputReservationHistory {
    /// The cap the current run recorded for one of its steps, until that step finishes.
    recorded: Option<(StepId, u32)>,
}

impl OutputReservationHistory {
    pub fn fold(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { .. } => self.start_run(),
            WalRecord::ModelStepFinished { .. } => self.note_step(),
            WalRecord::OutputReservationClamped {
                step_id,
                max_output_tokens,
                ..
            } => self.note_recorded(*step_id, *max_output_tokens),
            _ => {}
        }
    }

    /// A run's `RunStarted`: the run loop calls this for the record it writes itself.
    pub fn start_run(&mut self) {
        // Step ids restart with each run, so a cap another run recorded never applies.
        self.recorded = None;
    }

    /// A `ModelStepFinished` record: the step a recorded cap belonged to is done.
    pub fn note_step(&mut self) {
        self.recorded = None;
    }

    /// An `OutputReservationClamped` record.
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
