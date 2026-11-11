//! Reading a head's whole history, for `session.import` with a file source.
//!
//! A head is a long-lived agent session hosted by one of Thalamus's runners (OpenCode 2 through
//! `thalamus-harness`, or Pi through `thalamus-pi`). Moving it onto broca makes its history the
//! start of a broca session, so these readers keep what the transcript readers beside them drop:
//! each source message's id, its own times, the model's call ids and the call keys the runner
//! derived from them, the media the model saw, and the step usage the runner served.
//!
//! The readers are strict where the transcript readers are lenient. A source fact they can't map
//! refuses the whole history by name ([`HeadError`]) rather than being dropped, and a value the
//! source doesn't hold stays absent. Rows the host never sends to its model become no message,
//! and the move list ([`HeadHistory::rows`]) says so for each of them.
//!
//! What the model saw follows the host's own conversion of its rows to model messages: for
//! OpenCode 2, llm-runner's `opencode_v2` serializer profile (OpenCode 2's `to-llm-message.ts`);
//! for Pi, llm-runner's `pi` serializer profile (Pi's `convertToLlm`), which both runners serve.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use broca_protocol::{ContentBlock, MediaRef, Message, Role, ToolCallId};

mod calls;
pub mod opencode1;
pub mod opencode2;
pub mod pi;
mod sends;

pub use calls::{apply_calls, calls_path, read_calls, CallRow};
pub use sends::{apply_sends, read_sends, sends_path, SendRow, SendTarget};

/// The module that minted an OpenCode 2 head's call keys.
pub const OPENCODE2_CARRIER: &str = "thalamus-harness";
/// The module that minted a Pi head's call keys.
pub const PI_CARRIER: &str = "thalamus-pi";

/// Validates one medium's bytes against the type its row declares and stores nothing: the
/// caller's media rules decide what a stored medium may be. Returns the reference the message
/// carries, or why the medium can't be stored.
pub type MediaCheck<'a> = dyn FnMut(&[u8], &str) -> Result<MediaRef, String> + 'a;

/// One head's history as broca imports it.
#[derive(Debug, Clone, PartialEq)]
pub struct HeadHistory {
    /// The messages in conversation order.
    pub messages: Vec<HeadMessage>,
    /// One row per source row on the session's path, in source order: the move list.
    pub rows: Vec<SourceRow>,
    /// System rows left out. Broca builds a run's system text from its plan, and a system
    /// message in the middle of a history breaks providers that refuse one there.
    pub dropped_system_messages: usize,
}

/// One source row and the message it became, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRow {
    /// The source's id of the row.
    pub mid: String,
    /// The index in [`HeadHistory::messages`] of the message the row became; `None` when the
    /// host never sends the row to its model. A row that became two messages (an OpenCode 2
    /// assistant row whose calls have results) names the first, the assistant message.
    pub message: Option<usize>,
    /// The standalone plugin's assistant summary finished with `stop`, even if it errored.
    pub finished_summary: bool,
}

/// One message of a head's history.
#[derive(Debug, Clone, PartialEq)]
pub struct HeadMessage {
    /// The source row's id; `None` for a message broca adds, the tool results split out of an
    /// OpenCode 2 assistant row.
    pub mid: Option<String>,
    pub message: Message,
    /// When the source wrote the message, in Unix milliseconds.
    pub written_at_ms: Option<u64>,
    /// When the source accepted the message, in Unix milliseconds. A tool results message has
    /// none, as for broca's own.
    pub created_at_ms: Option<u64>,
    /// The step usage the host's runner serves for an assistant message.
    pub usage: Option<HeadUsage>,
    /// The bytes of each medium the message carries.
    pub media: Vec<HeadMedium>,
    /// For an assistant message, the identity of each tool call it made, in call order.
    pub calls: Vec<HeadCall>,
    /// What the runner recorded about the send that placed the message (the side file).
    pub send: Option<SendFacts>,
    /// Sends that rode on this tool message's results, in side-file order.
    pub rides: Vec<SendFacts>,
}

/// Token counts as the source runner serves them in `session.entries`: absent means not
/// reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeadUsage {
    pub prompt_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

/// The bytes of one medium, and where the message holds its reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadMedium {
    /// The content block holding the reference.
    pub block: u32,
    /// For a medium inside a tool result, its position in the result's parts.
    pub part: Option<u32>,
    pub bytes: Vec<u8>,
}

/// One imported tool call's identity and recorded facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallIdentity {
    /// The call id as the message holds it, after a reused id was made unique.
    pub tool_call_id: String,
    /// The id as the source wrote it, before reused ids were made unique for broca.
    pub source_tool_call_id: String,
    /// The key the source runner gave the call, verbatim.
    pub call_key: String,
    /// The module that minted the key.
    pub carrier: String,
    /// Facts recorded by the minting runner, never supplied by the importing runner.
    pub schema_pin: Option<String>,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub unanswered: Option<String>,
}

pub type HeadCall = CallIdentity;

/// What the source runner recorded about the send that placed a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendFacts {
    pub client_message_id: Option<String>,
    /// The attested module id of the sender, as the runner recorded it.
    pub sender: String,
    /// The plan template the send was rendered from, by its id.
    pub template: Option<String>,
    /// The send's marks (`speaks_for`, `notice`), as llm-runner's `MessageMarks` JSON.
    pub marks: Option<serde_json::Value>,
    /// How the send reached the conversation, as `session.read` names it: `steer`,
    /// `idle_start` or `tool_result`; absent for a plain queued send.
    pub delivery_mode: Option<String>,
    /// For a send that rode on a tool result: the call whose result carried it.
    pub delivery_tool_call_id: Option<String>,
    /// For a send that rode on a tool result: its text, exactly as delivered with the result.
    pub text: Option<String>,
    /// The runner's id of the send.
    pub send_id: String,
}

/// Why a head's history can't be imported. Every refusal names what it refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeadError {
    /// The file isn't the source's format.
    Unreadable(String),
    /// A source row holds something the import can't map.
    Row { mid: String, reason: String },
    /// A call has no result, in a history whose host sends no call without one (OpenCode 2).
    /// An import runs only while the head is idle, so an open call means it wasn't.
    OpenCall { mid: String, call_id: String },
    /// A tool result answers no call made before it. `skipped_turn` names the turn whose call it
    /// answers when that turn errored or was aborted, so the host never sent the call.
    OrphanResult {
        mid: String,
        call_id: String,
        skipped_turn: Option<String>,
    },
    /// A medium can't be stored.
    Media { mid: String, reason: String },
    /// The side file can't be applied.
    Sends(String),
    /// The call facts side file can't be applied.
    Calls(String),
}

impl HeadError {
    /// The refusal's stable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreadable(_) => "import_source_unreadable",
            Self::Row { .. } => "import_source_unmapped",
            Self::OpenCall { .. } => "import_open_call",
            Self::OrphanResult { .. } => "import_orphan_result",
            Self::Media { .. } => "import_media_refused",
            Self::Sends(_) => "import_sends_unreadable",
            Self::Calls(_) => "import_calls_unreadable",
        }
    }

    pub(crate) fn row(mid: &str, reason: impl Into<String>) -> Self {
        Self::Row {
            mid: mid.to_owned(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for HeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreadable(reason) => write!(f, "the source file is unreadable: {reason}"),
            Self::Row { mid, reason } => write!(f, "source row {mid}: {reason}"),
            Self::OpenCall { mid, call_id } => write!(
                f,
                "source row {mid}: call {call_id} has no result, so the head wasn't idle"
            ),
            Self::OrphanResult {
                mid,
                call_id,
                skipped_turn: None,
            } => write!(
                f,
                "source row {mid}: the result for call {call_id} answers no call before it"
            ),
            Self::OrphanResult {
                mid,
                call_id,
                skipped_turn: Some(turn),
            } => write!(
                f,
                "source row {mid}: the result for call {call_id} answers a call of turn {turn}, \
                 which errored or was aborted, so the host never sent that call to its model"
            ),
            Self::Media { mid, reason } => {
                write!(f, "source row {mid}: a medium can't be stored: {reason}")
            }
            Self::Sends(reason) => write!(f, "the side file can't be applied: {reason}"),
            Self::Calls(reason) => write!(f, "the calls side file can't be applied: {reason}"),
        }
    }
}

impl std::error::Error for HeadError {}

/// A message under construction, before its calls are paired and named.
pub(crate) struct Draft {
    pub mid: Option<String>,
    pub message: Message,
    pub written_at_ms: Option<u64>,
    pub created_at_ms: Option<u64>,
    pub usage: Option<HeadUsage>,
    pub media: Vec<HeadMedium>,
    /// For each tool call, in call order, the key the source runner gave it; `None` where it
    /// gave none.
    pub keys: Vec<Option<String>>,
    /// The source row the message came from, for refusals.
    pub row: String,
    /// A turn the host leaves out of what it sends its model, which still ends the turn before
    /// it: it becomes no message.
    pub boundary: bool,
}

impl Draft {
    /// A turn the host leaves out, from source row `row`.
    pub(crate) fn boundary(row: &str) -> Self {
        Self {
            mid: None,
            message: Message::new(Role::Assistant, Vec::new()),
            written_at_ms: None,
            created_at_ms: None,
            usage: None,
            media: Vec::new(),
            keys: Vec::new(),
            row: row.to_owned(),
            boundary: true,
        }
    }
}

/// What a history does with a call still without a result when the turn that made it ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenCalls {
    /// Refuse the import ([`HeadError::OpenCall`]).
    Refuse,
    /// Close the call with an error result of this text, in a tool message of its own, as the
    /// host does before it sends its model the conversation.
    Close(&'static str),
}

/// The messages of a history, and where each draft went.
pub(crate) struct Paired {
    pub messages: Vec<HeadMessage>,
    /// For each draft, in order, the index of the message it became; `None` for a boundary.
    pub at: Vec<Option<usize>>,
}

/// Pairs every call with its result and makes a reused call id unique.
///
/// - A call's result must come in the tool messages right after its assistant message, before
///   any other message. When the turn ends with a call still unanswered, `open_calls` decides:
///   the import refuses, or the call is closed with a synthetic error result placed where the
///   turn ends, under an id broca mints.
/// - A call its runner gave no key never ran, so a real result for it refuses the import. Only
///   a call with a key is listed with its identity.
/// - A result answering no call refuses it ([`HeadError::OrphanResult`]).
/// - A call id the history used before is renamed `<id>-<n>`, n counting the id's uses, with a
///   further `-<k>` while that collides with any call id in the history, as the shared
///   finalizer does; its result is renamed with it. The call keeps the key the source gave it.
pub(crate) fn pair_calls(
    drafts: Vec<Draft>,
    carrier: &str,
    open_calls: OpenCalls,
) -> Result<Paired, HeadError> {
    let reserved: HashSet<String> = drafts
        .iter()
        .flat_map(|draft| &draft.message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.clone()),
            _ => None,
        })
        .collect();
    let mut used: HashSet<String> = HashSet::new();
    let mut uses: HashMap<String, usize> = HashMap::new();
    // The open turn's calls by their source id, each with its assigned id, in call order.
    let mut pending: HashMap<String, VecDeque<String>> = HashMap::new();
    // The open turn, for refusals once it closes.
    let mut open: Option<OpenTurn> = None;
    let mut out = Vec::with_capacity(drafts.len());
    let mut at = Vec::with_capacity(drafts.len());
    for mut draft in drafts {
        if draft.boundary || draft.message.role != Role::Tool {
            if let Some(turn) = open.take() {
                out.extend(turn.close(open_calls)?);
            }
            pending.clear();
        }
        if draft.boundary {
            at.push(None);
            continue;
        }
        let mut calls = Vec::new();
        let mut keys = std::mem::take(&mut draft.keys).into_iter();
        let mut unanswered = Vec::new();
        let mut keyless = Vec::new();
        for block in &mut draft.message.content {
            match block {
                ContentBlock::ToolCall(call) => {
                    let source = call.tool_call_id.0.clone();
                    if source.is_empty() {
                        return Err(HeadError::row(&draft.row, "a tool call has no id"));
                    }
                    let count = uses.entry(source.clone()).or_default();
                    *count += 1;
                    let assigned = if used.insert(source.clone()) {
                        source.clone()
                    } else {
                        unique(&format!("{source}-{count}"), &reserved, &mut used)
                    };
                    call.tool_call_id = ToolCallId(assigned.clone());
                    match keys.next().flatten() {
                        Some(key) => calls.push(CallIdentity {
                            tool_call_id: assigned.clone(),
                            source_tool_call_id: source.clone(),
                            call_key: key,
                            carrier: carrier.to_owned(),
                            schema_pin: None,
                            started_at: None,
                            finished_at: None,
                            unanswered: None,
                        }),
                        None => keyless.push(assigned.clone()),
                    }
                    pending
                        .entry(source.clone())
                        .or_default()
                        .push_back(assigned.clone());
                    unanswered.push((source, assigned));
                }
                ContentBlock::ToolResult(result) => {
                    let source = result.tool_call_id.0.clone();
                    let Some(assigned) = pending.get_mut(&source).and_then(VecDeque::pop_front)
                    else {
                        return Err(HeadError::OrphanResult {
                            mid: draft.row.clone(),
                            call_id: source,
                            skipped_turn: None,
                        });
                    };
                    result.tool_call_id = ToolCallId(assigned.clone());
                    if let Some(turn) = open.as_mut() {
                        turn.answered(&assigned)?;
                    }
                }
                _ => {}
            }
        }
        if !unanswered.is_empty() {
            open = Some(OpenTurn {
                row: draft.row.clone(),
                unanswered,
                keyless,
            });
        }
        at.push(Some(out.len()));
        out.push(HeadMessage {
            mid: draft.mid,
            message: draft.message,
            written_at_ms: draft.written_at_ms,
            created_at_ms: draft.created_at_ms,
            usage: draft.usage,
            media: draft.media,
            calls,
            send: None,
            rides: Vec::new(),
        });
    }
    if let Some(turn) = open {
        out.extend(turn.close(open_calls)?);
    }
    Ok(Paired { messages: out, at })
}

/// An assistant message's calls while their results come in.
struct OpenTurn {
    row: String,
    /// Calls without a result yet, by source id and assigned id, in call order.
    unanswered: Vec<(String, String)>,
    /// Calls the source runner gave no key, by assigned id.
    keyless: Vec<String>,
}

impl OpenTurn {
    /// The call with assigned id `assigned` has its result. A call without a key never ran, so
    /// a result for it can't be the host's own.
    fn answered(&mut self, assigned: &str) -> Result<(), HeadError> {
        if let Some(at) = self.unanswered.iter().position(|(_, id)| id == assigned) {
            self.unanswered.remove(at);
        }
        if self.keyless.iter().any(|id| id == assigned) {
            return Err(HeadError::row(
                &self.row,
                format!("call {assigned} has a result but no key from its runner"),
            ));
        }
        Ok(())
    }

    /// Ends the turn: each call still without a result refuses the import, or gets the
    /// synthetic error result `open_calls` names, in call order.
    fn close(self, open_calls: OpenCalls) -> Result<Vec<HeadMessage>, HeadError> {
        match open_calls {
            OpenCalls::Refuse => match self.unanswered.into_iter().next() {
                Some((call_id, _)) => Err(HeadError::OpenCall {
                    mid: self.row,
                    call_id,
                }),
                None => Ok(Vec::new()),
            },
            OpenCalls::Close(text) => Ok(self
                .unanswered
                .into_iter()
                .map(|(_, assigned)| HeadMessage {
                    mid: None,
                    message: Message::new(
                        Role::Tool,
                        vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
                            tool_call_id: ToolCallId(assigned),
                            output: broca_protocol::ToolOutput::error(text),
                            is_error: true,
                        })],
                    ),
                    written_at_ms: None,
                    created_at_ms: None,
                    usage: None,
                    media: Vec::new(),
                    calls: Vec::new(),
                    send: None,
                    rides: Vec::new(),
                })
                .collect()),
        }
    }
}

fn unique(candidate: &str, reserved: &HashSet<String>, used: &mut HashSet<String>) -> String {
    let mut suffix = 1usize;
    let mut value = candidate.to_owned();
    while reserved.contains(&value) || !used.insert(value.clone()) {
        suffix += 1;
        value = format!("{candidate}-{suffix}");
    }
    value
}

/// Decodes standard base64, as both hosts store inline bytes.
pub(crate) fn base64_bytes(data: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|error| format!("its base64 bytes don't decode: {error}"))
}

#[cfg(test)]
mod opencode1_tests;
#[cfg(test)]
mod tests;
