//! The records a session log holds, split into two streams.
//!
//! - **Transcript:** the entries of the conversation, as the model sees them: inputs, assistant
//!   output, tool replies, text delivered with a reply, and the setup stub marking a setup change.
//!   A revert can cut the transcript.
//! - **Ledger:** what happened: runs, steps, model attempts, tool dispatches and their outcomes.
//!   The ledger is never reverted, because money spent and side effects done stay true.
//!
//! A *fold* is the code that rebuilds a session's state by reading its records in order: the
//! conversation fold reads the transcript, and the operational fold reads the ledger. Each record
//! carries what its own stream's fold needs, so the two folds never read each other.
//!
//! Kind numbers are stored in every frame header and must never be reused. Transcript kinds use
//! 1-63 and ledger kinds 64 and up, so a kind number alone names its stream.
//!
//! Some members are **owned payloads**: JSON values whose structure belongs to the component that
//! reads them (the provider's frozen render config, the vault's auth selection, billing
//! envelopes). The store carries them without interpreting them, as today's log already does for
//! the render config. Everything the store or its folds read is a typed field.
//!
//! This is the first slice of the record set: the kinds that carry nearly all of a session's bytes
//! and that loading and appending exercise. The remaining kinds (queue, approvals, held calls,
//! compaction, policy waits, recovery) are added as their folds are built.

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{FinishReason, Message, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;
use serde::Serialize;

use crate::codec::{
    put_varint, DecodeError, DecodeErrorKind, Digest, Message as CodecMessage, Raw, Value, Wire,
};
use crate::{message, values};

/// Which stream a record belongs to. Stored in every frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Stream {
    Transcript,
    Ledger,
}

impl Value for RunFinishReason {
    const WIRE: Wire = Wire::Varint;
    const NAME: &'static str = "RunFinishReason";
    fn write(&self, out: &mut Vec<u8>) {
        let n: u64 = match self {
            RunFinishReason::Completed => 1,
            RunFinishReason::MaxSteps => 2,
            RunFinishReason::BudgetExhausted => 3,
            RunFinishReason::ContextCeiling => 4,
            RunFinishReason::Cancelled => 5,
            RunFinishReason::Interrupted => 6,
            RunFinishReason::Error => 7,
            RunFinishReason::TransformUnavailable => 8,
        };
        put_varint(out, n);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let n = u64::read(raw).map_err(|e| DecodeError::new(Self::NAME, e.kind))?;
        Ok(match n {
            1 => RunFinishReason::Completed,
            2 => RunFinishReason::MaxSteps,
            3 => RunFinishReason::BudgetExhausted,
            4 => RunFinishReason::ContextCeiling,
            5 => RunFinishReason::Cancelled,
            6 => RunFinishReason::Interrupted,
            7 => RunFinishReason::Error,
            8 => RunFinishReason::TransformUnavailable,
            other => {
                return Err(DecodeError::new(
                    Self::NAME,
                    DecodeErrorKind::UnknownVariant(other),
                ))
            }
        })
    }
}

// Transcript records.

values! {
    /// How an input entry reached the conversation.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum Placement {
        /// The run's own input, from the send that started it.
        1 RunInput,
        /// A steer, placed at a step boundary of a running run.
        2 Steer,
        /// A queued send delivered into a running run as a user message (delivery mode `steer`).
        3 Delivered,
        /// The caller's text placed when a tool-call budget threshold fired.
        4 BudgetNudge,
        /// History imported from another harness's log.
        5 Imported,
        /// A queued send delivered as a turn-scoped system entry (delivery mode `turn_system`).
        6 DeliveredTurnSystem,
    }
}

message! {
    /// Who delivered an entry a person didn't type: a module or the runner, and the plan template
    /// it was rendered from.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Notice {
        1 req sender: String,
        2 opt template: String,
    }
}

message! {
    /// One message placed into the conversation that the model didn't write: user input,
    /// system or turn-scoped system text, a steer, a delivered send, a budget nudge.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Input {
        1 req run_id: String,
        2 req placement: Placement,
        3 req message: Message,
        4 opt written_at_ms: u64,
        5 opt submission_id: String,
        6 opt notice: Notice,
        /// The part numbers a run frozen under runner tags v1 stamped on this message. Runs
        /// frozen later compute their numbers from the transcript and leave this absent.
        7 opt tags_v1: serde_json::Value,
    }
}

message! {
    /// One model step's assistant message: text, reasoning and tool calls, with the model that
    /// produced it as the message's origin.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Assistant {
        1 req run_id: String,
        2 req step_id: u64,
        3 req provider_message_id: MessageId,
        4 req message: Message,
        /// The step's usage, which `session.entries` returns with the entry. The ledger's
        /// `StepEnd` carries it too, for run totals: each fold reads only its own stream.
        5 opt usage: Usage,
        6 opt written_at_ms: u64,
        /// When the last content arrived, from which the entry's `completed_at` comes.
        7 opt last_content_at_ms: u64,
        8 opt tags_v1: serde_json::Value,
    }
}

message! {
    /// The result the model sees for one tool call: real output, or a synthetic result for a
    /// call that was refused, never sent, or whose outcome is unknown. Every call of a step gets
    /// exactly one, so the conversation fold knows a step is complete from the transcript alone.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ToolReply {
        1 req run_id: String,
        /// The batch the call ran in. Absent only on a closing result that named no batch: an
        /// approval's closing result, or a recovery close-out written before batches were named.
        2 opt batch_id: String,
        3 req result: ToolResult,
        4 opt finished_at_ms: u64,
        5 opt tags_v1: serde_json::Value,
    }
}

message! {
    /// Text delivered with a tool result after the call returned, rendered after that result.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ResultAppend {
        1 req run_id: String,
        2 req tool_call_id: ToolCallId,
        3 req text: String,
        4 opt notice: Notice,
        5 opt written_at_ms: u64,
    }
}

values! {
    /// What a setup part holds. Setup parts are content-addressed and shared between sessions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
    pub enum PartKind {
        /// The tools offered to the model, in the order offered.
        1 Tools,
        /// One tool provider's catalog answer, with its declarations and schema pins.
        2 ProviderAnswer,
        3 SystemText,
        /// The plan as the caller sent it.
        4 Plan,
        /// The step-transform declarations, in plan order.
        5 Declarations,
        /// The structured-output response format.
        6 ResponseFormat,
        /// Global instructions of a run without a plan.
        7 GlobalSystem,
    }
}

message! {
    /// A setup part, named by the SHA-256 of its canonical bytes.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
    pub struct PartRef {
        1 req kind: PartKind,
        2 req digest: Digest,
    }
}

message! {
    /// Marks a setup change on the transcript: the parts now in force, the model, and the tool
    /// names, so history reads never need the parts themselves.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct SetupStub {
        1 rep parts: PartRef,
        2 req model: ModelHandle,
        3 rep tool_names: String,
    }
}

// Ledger records.

message! {
    /// A run started: its own choices, and the setup parts it was frozen with.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RunStart {
        1 req run_id: String,
        2 req model: ModelHandle,
        3 rep setup: PartRef,
        4 req renderer_version: u32,
        5 opt submission_id: String,
        6 opt send_id: String,
        7 opt request_identity_digest: String,
        8 opt ts_ms: u64,
        9 opt written_at_ms: u64,
        10 opt session_class: String,
        11 opt retention_secs: u64,
        12 opt title: String,
        13 opt context_limit: u64,
        14 opt service_tier: String,
        15 opt route_decision_id: String,
        16 opt compaction_plan_version: u32,
        /// Owned by the provider: the frozen render config the renderer is rebuilt from.
        17 opt render_config: serde_json::Value,
        /// Owned by the vault client: the credential and account chosen for the run.
        18 opt auth_selection: serde_json::Value,
        /// The run loop's remaining frozen choices (generation settings, budgets, stop
        /// conditions, tag rules), owned by the run loop.
        19 opt run_choices: serde_json::Value,
        /// Where the conversation came from: fresh, or seeded from an import or fork.
        20 opt origin: serde_json::Value,
        21 opt principal: serde_json::Value,
        22 opt scope: serde_json::Value,
        23 opt hooks: serde_json::Value,
        24 opt keep_warm: serde_json::Value,
        25 opt on_restart: serde_json::Value,
    }
}

message! {
    /// A run ended. `Interrupted` and `Cancelled` stay distinct: only a caller cancels.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RunEnd {
        1 req run_id: String,
        2 req reason: RunFinishReason,
        3 opt usage: Usage,
        4 opt ts_ms: u64,
        5 req retries_used: u32,
        6 opt interrupt_reason: String,
        /// Owned by the provider layer: the typed provider error, when the run failed.
        7 opt error: serde_json::Value,
    }
}

message! {
    /// A model step began. Steers wait for this boundary.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct StepStart {
        1 req run_id: String,
        2 req step_id: u64,
    }
}

message! {
    /// A model step finished: the ledger half of the assistant entry.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct StepEnd {
        1 req run_id: String,
        2 req step_id: u64,
        3 opt usage: Usage,
        4 opt finish_reason: FinishReason,
        5 req retries_used: u32,
        6 opt service_tier_used: String,
        7 opt speed_used: String,
        /// How many tool calls the step made.
        8 req call_count: u32,
        /// Owned by the run loop: the refusal-fallback route and schedule.
        9 opt fallback: serde_json::Value,
        10 opt hooks: serde_json::Value,
    }
}

message! {
    /// One HTTP attempt of a model step.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Attempt {
        1 req run_id: String,
        2 req step_id: u64,
        3 req attempt: u32,
        4 opt attempt_id: String,
        5 req outcome: String,
        6 opt http_status: u32,
        7 req request_body_len: u64,
        8 req request_body_sha256: Digest,
        9 req response_body_len: u64,
        10 req response_body_sha256: Digest,
        11 req ts_ms: u64,
        12 opt sent_at_ms: u64,
        13 opt first_content_at_ms: u64,
        14 opt last_content_at_ms: u64,
        15 opt transport: String,
        16 opt served_account_ref: String,
        /// Owned by billing: the reservation and envelope of a billed attempt.
        17 opt billing: serde_json::Value,
        /// Owned by diagnostics: stream event counts, cache evidence, WebSocket and input
        /// transformation details.
        18 opt telemetry: serde_json::Value,
    }
}

message! {
    /// A tool batch began: the calls planned for one step, in order.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct BatchStart {
        1 req run_id: String,
        2 req batch_id: String,
        3 req step_id: u64,
        4 rep planned: ToolCallId,
    }
}

message! {
    /// Every planned call of a batch has its outcome.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct BatchEnd {
        1 req run_id: String,
        2 req batch_id: String,
    }
}

message! {
    /// A tool call is about to be dispatched. Durable before the call leaves, so a crash after
    /// this record never re-sends the call.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ToolIntent {
        1 req run_id: String,
        2 req batch_id: String,
        3 req tool_call_id: ToolCallId,
        4 opt tool_name: String,
        5 opt module_id: String,
        /// SHA-256 of the dispatched arguments as canonical JSON (RFC 8785).
        6 req args_digest: Digest,
        /// The dispatched arguments, kept only when a hook or approval changed them from the
        /// assistant's call. Otherwise they are the call's own input and aren't stored twice.
        7 opt args: serde_json::Value,
        8 opt call_key: String,
        9 opt approval_digest: String,
        10 opt dispatched_at_ms: u64,
        /// Owned by the tool router, the code that sends calls to tool provider modules: the
        /// schema pin the call was dispatched under.
        11 opt schema_pin: serde_json::Value,
        12 opt hooks: serde_json::Value,
    }
}

values! {
    /// What kind of terminal a tool call reached.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum CallOutcome {
        /// A definite result: real output, or an error result for a call that never left.
        1 Completed,
        /// The call was sent and nothing came back: its side effect may have happened.
        2 Unknown,
        /// Abandoned by a cancel or interrupt before it finished.
        3 Interrupted,
        /// A step limit stopped the run with this call pending.
        4 MaxSteps,
        /// Never dispatched: denied, invalid, blocked by policy, or the tool provider was
        /// unreachable.
        5 NotDispatched,
    }
}

message! {
    /// A tool call's terminal: the ledger half of its `ToolReply`.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ToolOutcome {
        1 req run_id: String,
        2 req batch_id: String,
        3 req tool_call_id: ToolCallId,
        4 req outcome: CallOutcome,
        /// SHA-256 of the result as stored on the transcript, so budget and repetition rules can
        /// fold from the ledger without reading the result.
        5 req result_digest: Digest,
        6 req is_error: bool,
        7 opt reason: String,
        8 opt finished_at_ms: u64,
        /// Owned by the tool router: why a call wasn't dispatched, held or withdrawn.
        9 opt detail: serde_json::Value,
        10 opt hooks: serde_json::Value,
    }
}

message! {
    /// A compaction boundary on the transcript. It is either a compaction message from the first
    /// compaction protocol (found only on converted sessions), or a marker from the current
    /// compaction contract that records where the provider's cut falls. The body is the record as
    /// written; the compaction code is what reads it.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Compaction {
        /// 1 for a v1 compaction message, 2 for a mode-3 marker.
        1 req version: u32,
        2 req body: serde_json::Value,
    }
}

message! {
    /// A send accepted into the queue, durable before the caller is answered. Its input is
    /// written again as a transcript entry when it is placed.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct QueuedSend {
        1 req submission_id: String,
        2 opt send_id: String,
        3 rep input: Message,
        /// The tool surface the send carried, as a setup part.
        4 rep setup: PartRef,
        5 opt written_at_ms: u64,
        6 opt ts_ms: u64,
        /// Owned by admission: the rest of the request (model, generation, budget, stop
        /// conditions, cache and auth preferences).
        7 opt request: serde_json::Value,
        /// The receipt: sender, principal, delivery, retention, title and hooks.
        8 opt receipt: serde_json::Value,
    }
}

message! {
    /// A session refresh was accepted: the new setup, and when it applies.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct SessionRefresh {
        1 req generation: u64,
        2 req policy: String,
        3 req run_in_progress: bool,
        4 req applies_from_next_run: bool,
        5 rep setup: PartRef,
        /// The rest of the refreshed manifest, owned by the fetch-plan code.
        6 opt manifest: serde_json::Value,
    }
}

message! {
    /// A structured-output run's final result.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct StructuredResult {
        1 req run_id: String,
        2 req result: serde_json::Value,
    }
}

message! {
    /// A ledger event whose fields no fold reads individually yet: pauses and resumes, policy waits,
    /// approvals, held calls, late results, the rest of the queue, writer and recovery records,
    /// and the v1 compaction protocol. `name` is the old record's type name and `body` its
    /// fields. As each fold is built, the events it reads become typed kinds of their own.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Event {
        1 req name: String,
        2 opt run_id: String,
        3 opt body: serde_json::Value,
    }
}

/// The type a record variant holds: the record itself, or a box for the rare large kinds, so a
/// loaded log's records don't all pay for the largest one.
macro_rules! variant_type {
    ($record:ident) => { $record };
    ($record:ident boxed) => { Box<$record> };
}

/// Declares the record enum, its kind numbers and streams, and dispatch between them, so a
/// record kind can't exist without a number and a stream.
macro_rules! records {
    (
        transcript { $( $tnum:literal $tvar:ident $($tbox:ident)? ),* $(,)? }
        ledger { $( $lnum:literal $lvar:ident $($lbox:ident)? ),* $(,)? }
    ) => {
        /// One record of a session log.
        #[derive(Debug, Clone, PartialEq, Serialize)]
        #[serde(tag = "kind")]
        pub enum Record {
            $( $tvar(variant_type!($tvar $($tbox)?)), )*
            $( $lvar(variant_type!($lvar $($lbox)?)), )*
        }

        const _: () = assert!(crate::codec::ascending(&[$($tnum,)* $($lnum,)*]));

        impl Record {
            /// The record's kind number, as stored in its frame header.
            pub fn kind(&self) -> u32 {
                match self {
                    $( Record::$tvar(_) => $tnum, )*
                    $( Record::$lvar(_) => $lnum, )*
                }
            }

            pub fn name(&self) -> &'static str {
                match self {
                    $( Record::$tvar(_) => stringify!($tvar), )*
                    $( Record::$lvar(_) => stringify!($lvar), )*
                }
            }

            pub fn stream(&self) -> Stream {
                kind_stream(self.kind()).expect("every declared kind has a stream")
            }

            /// The record's body, without its kind: the frame header carries that.
            pub fn encode_body(&self) -> Vec<u8> {
                match self {
                    $( Record::$tvar(record) => record.encode(), )*
                    $( Record::$lvar(record) => record.encode(), )*
                }
            }

            /// Decodes a body of the given kind. An unknown kind refuses: whether such a record
            /// may be skipped is the frame header's to say, not the body's.
            pub fn decode(kind: u32, body: &[u8]) -> Result<Record, DecodeError> {
                match kind {
                    $( $tnum => Ok(Record::$tvar($tvar::decode(body)?.into())), )*
                    $( $lnum => Ok(Record::$lvar($lvar::decode(body)?.into())), )*
                    other => Err(DecodeError::new(
                        "Record",
                        DecodeErrorKind::UnknownVariant(u64::from(other)),
                    )),
                }
            }
        }

        /// The stream of a known kind number.
        pub fn kind_stream(kind: u32) -> Option<Stream> {
            match kind {
                $( $tnum => Some(Stream::Transcript), )*
                $( $lnum => Some(Stream::Ledger), )*
                _ => None,
            }
        }

        $( const _: () = assert!($tnum >= 1 && $tnum < 64, "transcript kinds are 1-63"); )*
        $( const _: () = assert!($lnum >= 64, "ledger kinds are 64 and up"); )*
    };
}

records! {
    transcript {
        1 Input,
        2 Assistant,
        3 ToolReply,
        4 ResultAppend,
        5 SetupStub,
        6 Compaction,
    }
    ledger {
        // One per run, and much larger than the rest.
        64 RunStart boxed,
        65 RunEnd,
        66 StepStart,
        67 StepEnd,
        68 Attempt,
        69 BatchStart,
        70 BatchEnd,
        71 ToolIntent,
        72 ToolOutcome,
        73 QueuedSend,
        74 SessionRefresh,
        75 StructuredResult,
        76 Event,
    }
}

#[cfg(test)]
mod tests;
