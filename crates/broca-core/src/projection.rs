//! The control-stream projection: durable WAL records → the ordered control units a
//! subscriber sees, each addressed by a durable cursor.
//!
//! This is a DIFFERENT output than [`crate::replay`]: replay produces the RESUME state
//! (one prompt + a resolution for the loop); projection produces the STREAM (an ordered
//! list of control units for subscribers). They read the same WAL but answer different
//! questions, so they're separate modules — and the two POSITIONS they use are kept
//! apart by type (see [`Cursor`] vs the store's materialization watermark).
//!
//! ## Determinism
//! `project_control` is a pure function of the records: the same records always produce
//! the same `(Cursor, ControlUnit)` sequence. A record can expand to SEVERAL ordered
//! units (e.g. a model step → an assistant message then a step-finished), disambiguated
//! by `sub_index`. `sub_index` is a pure function of the record's own content and a
//! fixed per-record-type ordering — it NEVER depends on map iteration order or anything
//! ambient. Where multiple units come from a collection (a batch's tool calls), they are
//! ordered by the collection's own order (the `planned_calls` Vec / the assistant
//! message's content order), never a map. The render golden pins this.
//!
//! ## Two positions, never conflated
//! A [`Cursor`] is `(wal_seq, sub_index)`: a position in the lineage-global control
//! stream, monotonic over fsynced seqs, valid at ANY fsynced unit (including
//! mid-episode). It is what a subscriber holds and resubscribes with. It is DISTINCT
//! from the store's materialization watermark (a turn-boundary-anchored seq for resume):
//! different type, no implicit conversion. The projection is computed from a record
//! RANGE addressed by cursors, never "from the materialization watermark forward".

use std::sync::Arc;

use broca_protocol::{
    caller::{ControlUnit, RunFinishReason, SharedProviderError},
    content::ContentBlock,
    FinishReason, RunId, ToolCallId,
};

/// A durable position in the control stream: the WAL seq of the source record plus the
/// 0-based index of this unit WITHIN that record's expansion (one record can project to
/// several ordered units). Lineage-global and monotonic over fsynced seqs; it never
/// resets at an episode boundary (WAL seq is monotonic across the whole lineage file).
///
/// This is NOT the materialization watermark (which is a turn-boundary-anchored seq the
/// store uses for resume). Distinct type on purpose, so a code path can't pass one where
/// the other belongs.
#[derive(Debug, Clone, Copy)]
pub struct Cursor {
    pub wal_seq: u64,
    pub sub_index: u32,
    frame: Option<FrameProof>,
}

/// What a cursor names besides its position, so a resume can tell the unit it names is still
/// the one the log holds at that position.
#[derive(Debug, Clone, Copy)]
enum FrameProof {
    /// A record of a session kept in the new storage: its head's incarnation and the record's
    /// keyed digest, both minted by that storage.
    Store { incarnation: [u8; 16], digest: u64 },
}

impl Cursor {
    pub fn new(wal_seq: u64, sub_index: u32) -> Self {
        Self {
            wal_seq,
            sub_index,
            frame: None,
        }
    }

    /// Binds the cursor to a record of a session kept in the new storage: the session head's
    /// `incarnation` and the record's keyed `digest`.
    pub fn with_store_identity(mut self, incarnation: [u8; 16], digest: u64) -> Self {
        self.frame = Some(FrameProof::Store {
            incarnation,
            digest,
        });
        self
    }

    /// The new storage's identity this cursor carries: the head's incarnation and the record's
    /// digest.
    pub fn store_identity(self) -> Option<([u8; 16], u64)> {
        match self.frame {
            Some(FrameProof::Store {
                incarnation,
                digest,
            }) => Some((incarnation, digest)),
            _ => None,
        }
    }

    /// The cursor's `incarnation` and `digest` as the wire spells them: the new storage's
    /// incarnation in lowercase hex as `session.read` serves it, and the digest as 16 lowercase
    /// hex digits.
    pub fn wire_identity(self) -> Option<(String, String)> {
        match self.frame? {
            FrameProof::Store {
                incarnation,
                digest,
            } => Some((
                incarnation
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                format!("{digest:016x}"),
            )),
        }
    }
}

// Position ordering is independent of whether a reader also retained the verified frame header.
impl PartialEq for Cursor {
    fn eq(&self, other: &Self) -> bool {
        (self.wal_seq, self.sub_index) == (other.wal_seq, other.sub_index)
    }
}
impl Eq for Cursor {}
impl PartialOrd for Cursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Cursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.wal_seq, self.sub_index).cmp(&(other.wal_seq, other.sub_index))
    }
}
impl std::hash::Hash for Cursor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&(self.wal_seq, self.sub_index), state);
    }
}

/// Episode-local tracking shared by control projection and run-status reconciliation. A dispatch
/// intent remains unresolved until a definite terminal arrives; an indeterminate terminal stays in
/// the reported set because the tool may already have applied its side effect. A session kept in
/// the new storage reports a run's indeterminate calls through the same tracking.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct IndeterminateToolCalls {
    unresolved_dispatches: Vec<ToolCallId>,
    indeterminate_outcomes: Vec<ToolCallId>,
}

impl IndeterminateToolCalls {
    pub fn reset_episode(&mut self) {
        self.unresolved_dispatches.clear();
        self.indeterminate_outcomes.clear();
    }

    pub fn reset_batch(&mut self) {
        self.unresolved_dispatches.clear();
    }

    pub fn record_dispatch_intent(&mut self, tool_call_id: &ToolCallId) {
        if !self.unresolved_dispatches.contains(tool_call_id) {
            self.unresolved_dispatches.push(tool_call_id.clone());
        }
    }

    pub fn record_dispatch_terminal(&mut self, tool_call_id: &ToolCallId, indeterminate: bool) {
        self.unresolved_dispatches
            .retain(|pending| pending != tool_call_id);
        if indeterminate && !self.indeterminate_outcomes.contains(tool_call_id) {
            self.indeterminate_outcomes.push(tool_call_id.clone());
        }
    }

    pub fn snapshot(&self) -> Vec<ToolCallId> {
        let mut calls = self.indeterminate_outcomes.clone();
        for tool_call_id in &self.unresolved_dispatches {
            if !calls.contains(tool_call_id) {
                calls.push(tool_call_id.clone());
            }
        }
        calls
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveCallIdentity {
    seq: u64,
    schema_pin: Option<String>,
    recorded_key: Option<String>,
    dispatched: bool,
}

/// Context for incremental projection. Placement and log identity survive episode
/// boundaries; call facts are bounded to the current tool batch.
///
/// Batch replay constructs this state internally, while the live projector keeps
/// an instance so later records retain their run, message and call identities.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ControlProjectionState {
    call_anchors: crate::call_identity::CallAnchors,
    current_call: Option<crate::call_identity::CallAnchor>,
    wal_lineage_id: Option<broca_wal::WalLineageId>,
    call_identities: std::collections::HashMap<ToolCallId, LiveCallIdentity>,
    notice_senders: std::collections::HashMap<String, String>,
    notice_sends: std::collections::HashMap<String, (String, String)>,
    notice_carriers: std::collections::HashMap<ToolCallId, (u64, u32, Option<String>)>,
    placement: crate::placement::Placement,
    placement_error: Option<String>,
    assistant_ordinal: Option<u64>,
    tool_result_ordinal: Option<u64>,
    assistant_message: Option<Arc<broca_protocol::AssistantMessage>>,
    assistant_started: bool,
    started_tools: std::collections::HashSet<broca_protocol::ToolCallId>,
    cancelled: bool,
    pending_inputs: Vec<Arc<Vec<broca_protocol::Message>>>,
    /// The recorded episode identity, including when restored from a store seed.
    /// `RunFinished` carries no run id in the WAL, so its control unit needs this context.
    run_id: Option<RunId>,
    final_step_finish_reason: Option<FinishReason>,
    has_completed_step: bool,
    indeterminate_tool_calls: IndeterminateToolCalls,
    held_calls: crate::replay::HeldCallsProjection,
}

impl ControlProjectionState {}

/// A notice fact and its source send identity, kept outside the serialized reader envelope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProjectedNotice {
    pub fact: cortexkit_role_llm_runner::read::NoticeFact,
    pub submission_id: String,
}

/// The lifecycle event of message `mid`: started, or finished. With no message, a reset.
/// Shared by today's projection and the projection of a session kept in the new storage.
pub fn message_lifecycle_event(
    mid: Option<String>,
    finished: bool,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use cortexkit_role_llm_runner::live::{LiveEvent, MessageEvent};
    match mid {
        Some(mid) if finished => LiveEvent::MessageFinished(MessageEvent::new(mid)),
        Some(mid) => LiveEvent::MessageStarted(MessageEvent::new(mid)),
        None => LiveEvent::DisplayReset,
    }
}

/// A call's start, in the message `mid` that made it: the dispatched name and arguments when
/// they differ from the call's, and when it was observed.
pub fn tool_started_event_at(
    mid: String,
    call: &broca_protocol::ToolCall,
    name: Option<&str>,
    arguments: Option<&serde_json::Value>,
    observed_at: Option<u64>,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use cortexkit_role_llm_runner::live::{LiveEvent, ToolStarted};
    LiveEvent::ToolStarted(ToolStarted::new(
        mid,
        call.tool_call_id.0.clone(),
        name.unwrap_or(&call.tool_name),
        arguments.unwrap_or(&(call.input).parse()),
        None,
        observed_at,
    ))
}

/// A call prepared for an approval before it was dispatched: its start, with the arguments as
/// prepared (canonical JSON) rather than as the model wrote them.
pub fn deferred_tool_started_event(
    mid: String,
    call: &broca_protocol::ToolCall,
    tool: &str,
    args_jcs: &str,
    deferred_at_ms: Option<u64>,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    let mut event = live::ToolStarted::new(
        mid,
        call.tool_call_id.0.clone(),
        tool,
        &serde_json::Value::Null,
        None,
        deferred_at_ms,
    );
    event.arguments = args_jcs.to_owned();
    LiveEvent::ToolStarted(event)
}

/// A call's end, in the message `mid` that made it: its outcome name and the size of what it
/// returned.
pub fn tool_finished_event_at(
    mid: String,
    call_id: &ToolCallId,
    outcome: &str,
    result: &broca_protocol::ToolResult,
    observed_at: Option<u64>,
) -> cortexkit_role_llm_runner::live::LiveEvent {
    use broca_protocol::ToolOutput;
    use cortexkit_role_llm_runner::live::{LiveEvent, ToolFinished};
    let bytes = match &result.output {
        ToolOutput::Text { text } => text.len(),
        ToolOutput::Json { value } => serde_json::to_vec(value)
            .expect("JSON values serialize")
            .len(),
        // A multi-part result counts its text and the stored size of each media object, so a
        // screenshot reads as the bytes the tool returned rather than its short marker text.
        ToolOutput::Parts { parts } => parts
            .iter()
            .map(|part| match part {
                broca_protocol::ToolOutputPart::Text { text } => text.len(),
                broca_protocol::ToolOutputPart::Media { media } => media.byte_size as usize,
            })
            .sum(),
        ToolOutput::Denied { reason } => reason.len(),
        ToolOutput::Error { message, .. } => message.len(),
    };
    LiveEvent::ToolFinished(ToolFinished::new(
        mid,
        call_id.0.clone(),
        outcome,
        bytes as u64,
        observed_at,
    ))
}

/// The outcome name a call's end carries, from its recorded terminal and result. An interrupted
/// call reads `cancelled` when its run was cancelled.
pub fn tool_outcome(
    outcome: broca_wal::DispatchOutcome,
    result: &broca_protocol::ToolResult,
    cancelled: bool,
) -> &'static str {
    use broca_wal::DispatchOutcome;
    match outcome {
        DispatchOutcome::Interrupted if cancelled => "cancelled",
        DispatchOutcome::Interrupted | DispatchOutcome::Unknown => "interrupted",
        DispatchOutcome::MaxSteps => "error",
        DispatchOutcome::Completed
            if matches!(result.output, broca_protocol::ToolOutput::Denied { .. }) =>
        {
            "denied"
        }
        DispatchOutcome::Completed if result.is_error => "error",
        DispatchOutcome::Completed => "ok",
    }
}

/// The outcome name of a call that was never dispatched, from why it wasn't.
pub fn no_dispatch_outcome(reason: &broca_wal::NoDispatchReason) -> &'static str {
    match reason {
        broca_wal::NoDispatchReason::Denied { .. }
        | broca_wal::NoDispatchReason::PolicyBlocked { .. } => "denied",
        _ => "error",
    }
}

/// One unit an input message adds to the stream, at the message (or, for a tool result, at the
/// message whose call it answers) named by position `P`.
pub enum InputUnit<'a, P> {
    Started(P),
    Call(P, &'a broca_protocol::ToolCall),
    Result(P, &'a broca_protocol::ToolResult),
    Finished(P),
}

/// The units an input message placed at `at` adds, in order: its start, each call it makes,
/// each result that answers a call an earlier message of the same placement made, and its end.
/// `owners` carries the calls across the placement's messages.
pub fn input_units<'a, P: Clone>(
    owners: &mut std::collections::HashMap<ToolCallId, P>,
    at: P,
    message: &'a broca_protocol::Message,
) -> Vec<InputUnit<'a, P>> {
    let mut units = vec![InputUnit::Started(at.clone())];
    for block in &message.content {
        match block {
            broca_protocol::ContentBlock::ToolCall(call) => {
                owners.insert(call.tool_call_id.clone(), at.clone());
                units.push(InputUnit::Call(at.clone(), call));
            }
            broca_protocol::ContentBlock::ToolResult(result) => {
                if let Some(owner) = owners.remove(&result.tool_call_id) {
                    units.push(InputUnit::Result(owner, result));
                }
            }
            _ => {}
        }
    }
    units.push(InputUnit::Finished(at));
    units
}

impl<P> InputUnit<'_, P> {
    /// The message the unit's boundary names.
    pub fn position(&self) -> &P {
        match self {
            Self::Started(at) | Self::Call(at, _) | Self::Result(at, _) | Self::Finished(at) => at,
        }
    }

    /// The unit's lifecycle event, naming each message by `mid`.
    pub fn event(&self, mid: impl Fn(&P) -> String) -> cortexkit_role_llm_runner::live::LiveEvent {
        match self {
            Self::Started(at) => message_lifecycle_event(Some(mid(at)), false),
            Self::Call(at, call) => tool_started_event_at(mid(at), call, None, None, None),
            Self::Result(owner, result) => tool_finished_event_at(
                mid(owner),
                &result.tool_call_id,
                tool_outcome(broca_wal::DispatchOutcome::Completed, result, false),
                result,
                None,
            ),
            Self::Finished(at) => message_lifecycle_event(Some(mid(at)), true),
        }
    }
}

impl<P> InputUnit<'_, P> {}

/// The unit a message boundary takes: the message's id, which its event names too.
pub fn boundary_unit(mid: String) -> ControlUnit {
    ControlUnit::MessageBoundary { mid }
}

/// A run's start, as a unit and its lifecycle event, which names the run's rich UI.
pub fn run_started_unit(
    run_id: &RunId,
    submission_id: Option<broca_protocol::SubmissionId>,
    rich_ui: Option<broca_protocol::RichUiStamp>,
) -> (ControlUnit, cortexkit_role_llm_runner::live::LiveEvent) {
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    let mut started = live::RunStarted::new(run_id.0.clone());
    started.rich_ui = rich_ui;
    (
        ControlUnit::RunStarted {
            run_id: run_id.clone(),
            submission_id,
        },
        LiveEvent::RunStarted(started),
    )
}

/// What a run's end tells about it besides its reason: what the run did before it ended.
pub struct RunEndFacts<'a> {
    /// The run, which today's end record doesn't name.
    pub run_id: Option<&'a RunId>,
    pub reason: RunFinishReason,
    pub usage: broca_protocol::Usage,
    /// Whether any step of the run completed: only then does the end report usage.
    pub has_completed_step: bool,
    pub final_step_finish_reason: Option<FinishReason>,
    /// The calls that may have run without a recorded result, reported on an interrupted run.
    pub indeterminate: &'a IndeterminateToolCalls,
    pub retries_used: u32,
}

/// A run's end, as a unit and its lifecycle event.
pub fn run_finished_unit(
    end: RunEndFacts<'_>,
) -> (ControlUnit, cortexkit_role_llm_runner::live::LiveEvent) {
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    (
        ControlUnit::RunFinished {
            awaits: None,
            run_id: end.run_id.cloned(),
            reason: end.reason,
            usage: end.has_completed_step.then_some(end.usage),
            final_step_finish_reason: end.final_step_finish_reason,
            indeterminate_tool_calls: (end.reason == RunFinishReason::Interrupted)
                .then(|| end.indeterminate.snapshot()),
            retries_used: end.retries_used,
        },
        end.run_id.map_or(LiveEvent::DisplayReset, |run_id| {
            LiveEvent::RunFinished(live::RunEnded::new(run_id.0.clone(), end.reason.as_str()))
        }),
    )
}

/// A run's pause (never a held-call park), as a unit and its lifecycle event.
pub fn paused_unit(
    run_id: &RunId,
    reason: broca_protocol::PauseReason,
    resume_not_before_ms: Option<u64>,
    error: Option<broca_protocol::ProviderError>,
) -> (ControlUnit, cortexkit_role_llm_runner::live::LiveEvent) {
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    (
        ControlUnit::Paused {
            awaits: None,
            run_id: run_id.clone(),
            reason,
            resume_not_before_ms,
            error: error.map(SharedProviderError::from),
        },
        LiveEvent::RunPaused(live::RunEnded::new(run_id.0.clone(), reason.as_str())),
    )
}

/// The notice for text a send delivered with call `part` of message `mid`, under the call's
/// key and the send's id and sender. Only text separated from the result by a blank line is a
/// notice; the separator belongs to rendering.
pub fn notice_delivered_event(
    mid: String,
    part: u32,
    key: Option<String>,
    send_id: String,
    sender: String,
    text: &str,
) -> Option<cortexkit_role_llm_runner::live::LiveEvent> {
    use cortexkit_role_llm_runner::live::{self, LiveEvent};
    let text = text.strip_prefix("\n\n")?;
    Some(key.map_or(LiveEvent::DisplayReset, |key| {
        LiveEvent::NoticeDelivered(
            live::NoticeDelivered::new(mid, part, key, send_id, sender).with_text(text),
        )
    }))
}

/// The schema pin a call's start and end carry: the pin it was dispatched under, encoded with
/// the name it was dispatched as.
pub fn schema_pin_text(tool_name: &str, pin: &broca_wal::DispatchSchemaPin) -> Option<String> {
    cortexkit_role_tool_provider::call::SchemaPin::new(tool_name, &pin.schema_digest, pin.semantics)
        .encode()
        .ok()
}

/// The finish reason implied by an assistant message's SHAPE, used only when the record
/// has no recorded provider finish_reason (older-shaped record). A message with tool
/// calls implies `ToolCalls`; otherwise `Stop`.
pub fn message_shape_finish_reason(
    msg: &broca_protocol::content::AssistantMessage,
) -> broca_protocol::content::FinishReason {
    use broca_protocol::content::FinishReason;
    if msg
        .content
        .iter()
        .any(|b| matches!(b, ContentBlock::ToolCall(_)))
    {
        FinishReason::ToolCalls
    } else {
        FinishReason::Stop
    }
}

// Episode-bounded reads from the offset index.
//
// On overhaul state roots the offset index records where each run episode starts. The model
// view and a live attach then decode only the frames of the episodes they show, folding at
// most one episode's records to rebuild state, instead of replaying the whole log. These
// helpers check everything they take from the index against the log before relying on it, and
// a read that fails any check replays the whole log instead.
