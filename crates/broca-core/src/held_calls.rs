//! Ordered provider-held call status, independent of the run loop driving it.

use broca_protocol::PauseReason;
use broca_protocol::{BatchId, ToolCallId};
use broca_records::record::WalRecord;
use std::collections::HashMap;

/// One ordered held-call fold for status reads and durable subscribe status events.
/// Keys include the batch because a model may reuse a call id in a later step.
///
/// The WAL projection drives it with [`Self::observe`]; the session-store projection calls the
/// same steps one by one from its own records.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HeldCallsProjection {
    recoveries: Vec<broca_records::recovery::RecoveryRun>,
    tool_names: HashMap<ToolCallId, String>,
    calls: Vec<(
        BatchId,
        ToolCallId,
        cortexkit_role_llm_runner::run::HeldCall,
    )>,
    state: Option<String>,
}

impl HeldCallsProjection {
    /// A run started: nothing is held.
    pub fn run_started(&mut self) {
        *self = Self::default();
    }

    /// A step started: the run is active again.
    pub fn step_started(&mut self) {
        self.state = None;
    }

    /// The run paused for `reason`; a pause for a held call itself changes nothing.
    pub fn paused(&mut self, reason: PauseReason) {
        if reason != PauseReason::ToolHeld && !self.calls.is_empty() {
            self.state = Some("paused".into());
        }
    }

    /// The run ended: every held call goes, which is a change when any was held.
    pub fn run_finished(
        &mut self,
        reason: broca_protocol::caller::RunFinishReason,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        self.state = Some(reason.as_str().into());
        let changed = !self.calls.is_empty();
        self.calls.clear();
        let status = changed.then(|| self.status());
        *self = Self::default();
        status
    }

    /// A tool batch finished: its call names are no longer needed.
    pub fn batch_finished(&mut self) {
        self.tool_names.clear();
    }

    /// A model step finished with `calls`: their names, which a held call is listed under.
    pub fn step_finished<'a>(&mut self, calls: impl Iterator<Item = &'a broca_protocol::ToolCall>) {
        self.tool_names.clear();
        for call in calls {
            self.tool_names
                .insert(call.tool_call_id.clone(), call.tool_name.clone());
        }
    }

    /// A call was dispatched under `tool_name`, when the intent names one.
    pub fn intent(&mut self, tool_call_id: &ToolCallId, tool_name: Option<&str>) {
        if let Some(tool) = tool_name {
            self.tool_names
                .entry(tool_call_id.clone())
                .or_insert_with(|| tool.to_owned());
        }
    }

    /// A provider took call `tool_call_id` of `batch_id` to answer later.
    pub fn held(
        &mut self,
        batch_id: &BatchId,
        tool_call_id: &ToolCallId,
        call_key: &str,
        module: &str,
        reason: &str,
        held_at_ms: u64,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        let held = cortexkit_role_llm_runner::run::HeldCall::new(
            call_key.to_owned(),
            self.tool_names
                .get(tool_call_id)
                .cloned()
                .unwrap_or_default(),
            module.to_owned(),
            reason.to_owned(),
            held_at_ms,
        );
        if let Some((_, _, previous)) = self
            .calls
            .iter_mut()
            .find(|(batch, call, _)| batch == batch_id && call == tool_call_id)
        {
            if previous == &held {
                return None;
            }
            *previous = held;
        } else {
            self.calls
                .push((batch_id.clone(), tool_call_id.clone(), held));
        }
        Some(self.status())
    }

    /// Call `tool_call_id` of `batch_id` reached its terminal: it is no longer held.
    pub fn terminal(
        &mut self,
        batch_id: &BatchId,
        tool_call_id: &ToolCallId,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        self.remove(batch_id, tool_call_id)
    }

    /// Only list changes return a snapshot, including the change to an empty list.
    pub(crate) fn observe(
        &mut self,
        record: &WalRecord,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        match record {
            WalRecord::RunStarted { .. } => self.run_started(),
            WalRecord::StepStarted { .. } => self.step_started(),
            WalRecord::RunPaused { reason, .. } => self.paused(*reason),
            WalRecord::RunFinished { reason, .. } => return self.run_finished(*reason),
            WalRecord::ToolBatchFinished { .. } => self.batch_finished(),
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => self.step_finished(assistant_message.tool_calls()),
            WalRecord::ToolDispatchIntent {
                tool_call_id,
                tool_name,
                ..
            } => self.intent(tool_call_id, tool_name.as_deref()),
            WalRecord::ToolCallProviderHeld {
                batch_id,
                tool_call_id,
                call_key,
                module,
                reason,
                held_at_ms,
                ..
            } => {
                return self.held(
                    batch_id,
                    tool_call_id,
                    call_key,
                    module,
                    reason,
                    *held_at_ms,
                )
            }
            WalRecord::ToolResult {
                batch_id, result, ..
            } => return self.remove(batch_id, &result.tool_call_id),
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } => return self.remove(batch_id, tool_call_id),
            WalRecord::RecoveryCut { recovery } => self.recovery_cut(&recovery.runs),
            WalRecord::RecoveryCallClosed {
                run_id,
                call_key,
                tool_call_id,
                batch_id,
                step_id,
                ..
            } => {
                return self.recovery_call_closed(
                    run_id,
                    call_key.as_ref(),
                    tool_call_id,
                    batch_id.clone(),
                    *step_id,
                )
            }
            _ => {}
        }
        None
    }

    /// A recovery found `runs` with calls whose outcome is unknown.
    pub fn recovery_cut(&mut self, runs: &[broca_records::recovery::RecoveryRun]) {
        self.recoveries.extend(runs.iter().cloned());
    }

    /// A recovery closed call `tool_call_id` of `run_id`: any held call it names is no longer
    /// held.
    pub fn recovery_call_closed(
        &mut self,
        run_id: &broca_protocol::RunId,
        call_key: Option<&String>,
        tool_call_id: &ToolCallId,
        batch_id: Option<BatchId>,
        step_id: Option<broca_protocol::StepId>,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        let mut terminal = broca_records::recovery::RecoveryCall {
            tool_call_id: tool_call_id.clone(),
            batch_id,
            step_id,
            call_keys: call_key.into_iter().cloned().collect(),
        };
        for run in self.recoveries.iter().filter(|run| &run.run_id == run_id) {
            for call in &run.calls {
                if call.terminal_matches(&terminal) {
                    terminal.call_keys.extend(call.call_keys.iter().cloned());
                }
            }
        }
        let previous = self.calls.len();
        self.calls.retain(|(batch, id, held)| {
            let identity = broca_records::recovery::RecoveryCall {
                tool_call_id: id.clone(),
                batch_id: Some(batch.clone()),
                step_id: None,
                call_keys: vec![held.call_key.clone()],
            };
            !identity.terminal_matches(&terminal)
        });
        (previous != self.calls.len()).then(|| self.status())
    }

    fn remove(
        &mut self,
        batch_id: &BatchId,
        tool_call_id: &ToolCallId,
    ) -> Option<cortexkit_role_llm_runner::run::RunStatus> {
        let previous = self.calls.len();
        self.calls
            .retain(|(batch, call, _)| batch != batch_id || call != tool_call_id);
        let status = (previous != self.calls.len()).then(|| self.status());
        if self.calls.is_empty() {
            self.state = None;
        }
        status
    }

    pub(crate) fn status(&self) -> cortexkit_role_llm_runner::run::RunStatus {
        cortexkit_role_llm_runner::run::RunStatus::new(self.state.as_deref().unwrap_or("active"))
            .with_held_calls(self.calls.iter().map(|(_, _, held)| held.clone()).collect())
    }
}
