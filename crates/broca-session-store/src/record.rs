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

use broca_protocol::caller::{PauseReason, RunFinishReason};
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
        /// Written by a recovery that may have lost records (see `session::recover`): storage
        /// damage removed records, and actions near the end may have run unrecorded.
        7 DamageNotice,
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
        /// For a message accepted before it was placed (a steer, or a queued send delivered
        /// into a running run): the seq of the ledger record that accepted it. It arrives
        /// while a step runs and is placed at the next step boundary, so the two are apart.
        8 opt receipt_seq: u64,
        /// Opaque payloads removed when the record left the open log (see `prune`).
        9 rep [must] pruned: crate::prune::PrunedOpaque,
        /// Where the bytes of each medium the message carries are stored: a media block, or a
        /// media part of a tool result in it. Only an imported message carries any. A reader
        /// that renders the message needs them, so a record carrying any is refused by a reader
        /// that doesn't know the field.
        10 rep [must] media: InputMedia,
    }
}

message! {
    /// The stored bytes of one medium of an input message, stored as a tool result's are (see
    /// [`MediaObject`]).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct InputMedia {
        /// The message's content block holding the medium.
        1 req block: u32,
        /// For a medium inside a tool result block: its position in the result's parts.
        2 opt part: u32,
        /// The open log the object was written under.
        3 req log: crate::codec::Id128,
        4 req generation: crate::codec::Id128,
        5 req seq: u64,
    }
}

message! {
    /// The stored bytes of one medium of a message accepted before it is placed: a queued send's
    /// or a steer's. The object is written under the accepting record, as an input message's is
    /// (see [`InputMedia`]), and the message's copy in the transcript points at the same object.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct SendMedia {
        /// The message's position in the record's messages.
        1 req message: u32,
        /// The message's content block holding the medium.
        2 req block: u32,
        /// For a medium inside a tool result block: its position in the result's parts.
        3 opt part: u32,
        /// The open log the object was written under.
        4 req log: crate::codec::Id128,
        5 req generation: crate::codec::Id128,
        6 req seq: u64,
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
        /// The run's last step, taken after its tool-call budget asked the model to finish. Any
        /// calls in it are never run, so the message enters the conversation without them; the
        /// record keeps them as the model wrote them.
        9 opt budget_finalize: bool,
        /// Opaque payloads removed when the record left the open log (see `prune`). A record
        /// carrying any must be rendered without its opaque parts.
        10 rep [must] pruned: crate::prune::PrunedOpaque,
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
        /// Where the bytes of each media part of `result` are stored. A reader that renders the
        /// result needs them, so a record carrying any is refused by a reader that doesn't know
        /// the field.
        6 rep [must] media: MediaObject,
    }
}

message! {
    /// The stored bytes of one media part: an append object, named by the record that first
    /// named it (`log`, `generation`, `seq`) and a keyed hash of the bytes' digest. A later record
    /// naming the same bytes while that object is in the open log points at the same object.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct MediaObject {
        /// The media part's position in the result's parts.
        1 req part: u32,
        /// The open log the object was written under.
        2 req log: crate::codec::Id128,
        3 req generation: crate::codec::Id128,
        4 req seq: u64,
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
        /// The id of the send whose text this is, which reads serve beside the text. A reader
        /// that doesn't know the field would serve the text without it, so it is
        /// must-understand.
        6 opt [must] send_id: String,
        /// The ride envelope (llm-runner 0.8.11): the carrying call's key, the sender and the
        /// forms the send allowed, fixed when the text was delivered. Text with an envelope is
        /// a ride: it is never joined into the stored result, and every request renders it
        /// after the result through the runner's own ride rules. Text without one was delivered
        /// before envelopes existed and keeps its historical rendering. A reader that doesn't
        /// know the field would render a ride the old way, so it is must-understand.
        7 opt [must] ride: RideEnvelope,
    }
}

values! {
    /// How a ride renders. Only the append form is ever recorded: broca never renders a ride
    /// as system text.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum RideForm {
        1 Append,
    }
}

message! {
    /// What a ride carries besides its text and its send's id, as today's `SendDelivered`
    /// record holds it in its `ride` member.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct RideEnvelope {
        /// The key of the tool call whose result carries the ride, recorded at delivery and
        /// never looked up again: a call id can repeat in a session, a key can't.
        1 req call_key: String,
        /// The module that sent it, or `broca`.
        2 req sender: String,
        /// The send's delivery list restricted to `tool_result` and `turn_system`, in order.
        3 rep forms: String,
        4 req form: RideForm,
        /// The agent the sending module spoke for (llm-runner's `SpeaksFor` JSON), which reads
        /// serve on the ride's notice. A reader that doesn't know the field would serve the
        /// notice without it, so it is must-understand.
        5 opt [must] speaks_for: serde_json::Value,
    }
}

message! {
    /// Token counts the provider reported for the request that produced an assistant entry, as
    /// `session.entries` serves them. Absent means not reported, never zero.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
    pub struct StepUsage {
        1 opt prompt_tokens: u64,
        2 opt cache_read_tokens: u64,
        3 opt cache_write_tokens: u64,
        4 opt output_tokens: u64,
        5 opt reasoning_tokens: u64,
    }
}

message! {
    /// A compaction view, named by its version and checksum.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ViewRef {
        1 req version: u64,
        2 req checksum: String,
    }
}

message! {
    /// A note a send delivered riding on one of a tool results message's results, as
    /// `session.read` serves it among the message's display facts (llm-runner's `NoticeFact`).
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct RideNotice {
        /// The result's position among the message's results: its call's position among the
        /// tool calls of the assistant message before it.
        1 req part: u32,
        2 req send_id: String,
        /// The module that sent it, or `broca`.
        3 req sender: String,
        /// The key of the call whose result carries it.
        4 req call_key: String,
        /// The text the send delivered, without the blank line that separates it from the
        /// result when it is rendered.
        5 req text: String,
        /// The agent the sending module spoke for, as the ride's envelope recorded it
        /// (llm-runner's `SpeaksFor` JSON). A reader that doesn't know the field would serve the
        /// notice without it, so it is must-understand.
        6 opt [must] speaks_for: serde_json::Value,
    }
}

message! {
    /// What `session.entries` serves about one conversation message besides the message itself,
    /// decided when the message was placed and never derived again. Written right after the
    /// record that places the message (or, for an assistant message still waiting for its tool
    /// results, the record that reserves its ordinal), in the same batch. A later `EntryFacts`
    /// for the same ordinal replaces it. Content-free but for the reasoning summaries and ride
    /// notices reads show: delivered text itself stays in its `ResultAppend`, and a notice
    /// repeats it so a read of the message needs no other record.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct EntryFacts {
        /// The message's place in the whole conversation, counting from 1.
        1 req ordinal: u64,
        /// When the message was written. On a session on the current compaction contract it is
        /// always set and never moves later than an earlier entry's: a message without its own
        /// time takes the newest earlier entry's, as `session.entries` serves it. Off the
        /// contract it is the message's own time, absent when it has none, as `session.read`
        /// serves it there.
        2 opt written_at_ms: u64,
        /// The part number before this message's first numbered part: its parts take the
        /// numbers after it, in order.
        3 req tag_base: u64,
        4 opt notice: Notice,
        5 opt usage: StepUsage,
        /// The compaction view in force when the model wrote this message.
        6 opt generated_under: ViewRef,
        /// A model step wrote it, so its completion time is `written_at_ms` once every tool call
        /// it made has a result. False for an assistant message that came in as caller input.
        7 req native: bool,
        /// Whether the run's requests render part numbers, and clean them out of the model's
        /// replies. Frozen per run; kept here so a message renders without its run's start.
        8 req render_tags: bool,
        9 req clean_reply: bool,
        /// The message's id, which never repeats in the session, across reverts. A message
        /// converted from today's storage keeps today's id. A new message's id is minted from
        /// the writer generation and the seq this record takes (`Session::message_id`).
        10 req mid: String,
        /// The run that placed the message. Absent when no run owns it, such as an imported
        /// seed. `session.read` names it as the message's run.
        11 opt run_id: String,
        /// When the message was accepted, as `session.read` serves it. An input, steer or
        /// delivered send takes its send's acceptance time, kept across a queue. An assistant
        /// message takes the send time of its step's last complete attempt, not counting a warm
        /// request. A tool result message has none.
        12 opt created_at_ms: u64,
        /// How a delivered send reached the conversation, as `session.read` names it beside the
        /// message's run: `steer` or `turn_system` for the messages it placed, `idle_start` for
        /// the input of a run it started, `tool_result` for the tool result message it rode
        /// on. A steer that wasn't queued, and every other message, has none.
        13 opt delivery_mode: String,
        /// For a send that rode on a tool result: the call whose result carried it.
        14 opt delivery_tool_call_id: String,
        /// The `client_message_id` of the send that placed the message, as `session.read`
        /// serves it in the message's display facts.
        15 opt client_message_id: String,
        /// The attested module id of whoever sent the message, as recorded at its send.
        16 opt sender: String,
        /// The send's display marks, as llm-runner's `MessageMarks` JSON: `speaks_for` and a
        /// true `notice`.
        17 opt marks: serde_json::Value,
        /// The message was converted from a log of today's storage that had no lineage, so
        /// today's read serves it none of what an `include_display_facts` read adds, and nor does
        /// the new storage's. Absent for every message broca places itself.
        18 opt no_display_facts: bool,
        /// For an assistant message: the summaries its reasoning blocks carry, in the order of
        /// its content blocks, as `session.read` serves them with display facts. Recorded when
        /// the message is placed because a seal prunes the encrypted reasoning they are read
        /// from. Absent when it has none.
        19 rep reasoning_summaries: ReasoningSummary,
        /// For an assistant message a run placed: the Rich UI stamp the run's plan froze, which
        /// `session.read` serves on it with `include_rich_ui`. Recorded here so a read finds it
        /// without looking up the run's start. Absent when the run's plan has none.
        20 opt rich_ui: RichUi,
        /// The highest part number the runner's own numbering (llm-runner's tags: stamped on a
        /// v1 run's records, computed by `broca_protocol::tags::number_message` from each later
        /// run's checkpoint) shows before this message. The message's numbered parts take the
        /// numbers after it, under its run's rules. It is the numbering today's read attaches to
        /// each message and output references resolve a reply's `$$tc(N)` against; on the
        /// compaction contract the model is shown `tag_base`'s numbering instead. Absent on
        /// sessions written before it was recorded.
        21 opt runner_tag_base: u64,
        /// On a session converted from a log whose compaction provider spoke the first
        /// compaction protocol only: the seq of that log's view event (`compaction_view_applied`,
        /// kept whole as a ledger event) in force when the model step that wrote this message
        /// sent its request, which output references check a named part against. Absent when no
        /// such view had applied; the run loop never writes first-protocol views.
        22 opt legacy_view_seq: u64,
        /// For a tool results message: each send that rode on one of its results, in the order
        /// delivered, as `session.read` serves it in the message's `notices` display fact. Only
        /// a ride delivered with its envelope has one; text delivered before envelopes existed
        /// stays joined into its result and has none.
        23 rep notices: RideNotice,
    }
}

message! {
    /// A run's Rich UI stamp: which Rich UI dialect its system text taught, and against which
    /// catalog (llm-runner's `RichUiStamp`).
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RichUi {
        1 req dialect: String,
        2 req catalog_digest: String,
    }
}

message! {
    /// The summary one reasoning block of an assistant message carries.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ReasoningSummary {
        /// The block's position among the message's content blocks.
        1 req part: u32,
        2 req text: String,
    }
}

message! {
    /// What reads serve about each tool call of one assistant message besides the message
    /// itself, decided once and never derived again. Written in the batch that places the
    /// message's tool results, so a sealed block never splits it from its turn. A later
    /// `TurnFacts` for the same ordinal replaces it: a run that ends cancelled restates its last
    /// turn's calls it abandoned as cancelled.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct TurnFacts {
        /// The ordinal of the assistant message that made the calls.
        1 req ordinal: u64,
        /// One per call that has a key, in the order the message made them: a dispatched call's
        /// is its intent's, a call answered without an intent its outcome's
        /// (`ToolOutcome::call_key`).
        2 rep calls: CallFacts,
    }
}

message! {
    /// One tool call's identity and times, as reads serve them.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct CallFacts {
        /// The call's id in the message.
        1 req tool_call_id: ToolCallId,
        /// The call's durable key, unique within the runner that minted it.
        2 req call_key: String,
        /// The schema pin the call ran under: for a call broca dispatched, the pin as reads
        /// serve it (llm-runner's encoded `SchemaPin`); for an imported call, as its runner
        /// recorded it.
        3 opt schema_pin: serde_json::Value,
        /// The attested module id of the runner that minted `call_key`, when that wasn't broca:
        /// a call imported from another runner keeps its key, which is unique only within that
        /// runner.
        4 opt carrier: String,
        /// The module the call was dispatched to.
        5 opt dispatched_to: String,
        6 opt dispatched_at_ms: u64,
        7 opt finished_at_ms: u64,
        /// Why the call has no answer from its tool, when it has none: reads serve it as the
        /// call's `unanswered`. Absent for a call its tool answered.
        8 opt unanswered: Unanswered,
    }
}

values! {
    /// Why a tool call has no answer from its tool, as reads name it (llm-runner's
    /// `ToolTime::unanswered`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum Unanswered {
        /// Refused: denied by a person or an approval, or blocked by policy.
        1 Denied,
        /// Never run: its input failed validation, or its tool provider couldn't take it.
        2 NotExecuted,
        /// Abandoned before it finished, or sent with no answer back.
        3 Interrupted,
        /// Stopped by a cancel, or by a step limit while it was pending.
        4 Cancelled,
    }
}

impl Unanswered {
    /// The name reads serve.
    pub fn as_str(self) -> &'static str {
        match self {
            Unanswered::Denied => "denied",
            Unanswered::NotExecuted => "not_executed",
            Unanswered::Interrupted => "interrupted",
            Unanswered::Cancelled => "cancelled",
        }
    }

    /// The value read as `name`, if reads serve one by that name.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "denied" => Unanswered::Denied,
            "not_executed" => Unanswered::NotExecuted,
            "interrupted" => Unanswered::Interrupted,
            "cancelled" => Unanswered::Cancelled,
            _ => return None,
        })
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
        /// The provider's frozen render config, from which the renderer of each of the run's
        /// requests is rebuilt. Opaque to the store and the run loop.
        8 RenderConfig,
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
        /// A digest of what renderers read from this setup's render config when rendering
        /// another run's messages (`broca_provider`'s `episode_render_key`). The conversation
        /// keeps runs past the window only where it changes. Absent on setups converted before
        /// it was recorded.
        4 opt render_key: Digest,
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
        /// The run was seeded from an import or fork rather than started by a send. Such a run
        /// never calls a provider, so it can't be resumed, only closed.
        26 opt seeded: bool,
        /// Owned by the run loop: the refusal-fallback schedule the run started with. Decided
        /// at admission: an earlier run's schedule carries over only when this run uses the
        /// model that armed it, and the schedule last changed no more than a day before this
        /// run was admitted (`broca_core::refusal_fallback::on_episode_admitted`). Absent when
        /// the run starts with none.
        27 opt refusal_schedule: serde_json::Value,
        /// On an import's seeded run: the session object that maps each imported message's
        /// source id to its ordinal. Message ids broca mints name where their message is, and
        /// an imported id doesn't, so a reader resolving an imported id needs the map.
        28 opt [must] imported_ids: ImportedIds,
        /// The display marks of the send whose input the run places (llm-runner's
        /// `MessageMarks` JSON: `speaks_for` and a true `notice`), which reads serve on each of
        /// those messages. A reader that doesn't know the field would serve them without their
        /// marks, so it is must-understand.
        29 opt [must] message_marks: serde_json::Value,
    }
}

message! {
    /// Where an import's id map and receipt are stored: session objects written under the open
    /// log by the batch whose seeded run start names them, before that batch.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ImportedIds {
        1 req log: crate::codec::Id128,
        2 req generation: crate::codec::Id128,
        /// The seq of the run start naming it.
        3 req seq: u64,
    }
}

message! {
    /// One imported message's source id and ordinal.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ImportedId {
        1 req mid: String,
        2 req ordinal: u64,
    }
}

message! {
    /// An import's id map, the body of the object [`ImportedIds`] names: every imported
    /// message's source id, sorted by id.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ImportedIdMap {
        1 rep ids: ImportedId,
    }
}

message! {
    /// One source message of an import's move list and the ordinal it became, if any.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ImportedMove {
        1 req mid: String,
        2 opt ordinal: u64,
    }
}

message! {
    /// What an import answered, kept so a retry of it is answered the same after its source is
    /// gone: the body of a session object written before the import's batch, beside the id
    /// map, at the place [`ImportedIds`] names.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ImportReceipt {
        /// The import's idempotency key, and the digest of the payload it was made with.
        1 req import_id: String,
        2 req digest: String,
        /// The move list's shape, as the reply names it.
        3 req shape: String,
        /// One per source message on the session's path, in source order.
        4 rep moves: ImportedMove,
        5 req imported_messages: u64,
        6 req dropped_system_messages: u64,
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
        /// The id of the assistant message this step writes, decided here so a live subscriber
        /// can name the message from the step's start. The new loop mints it from its writer
        /// generation and this record's seq; a step converted from a WAL log keeps the id the
        /// WAL projection gives the message at that position. The message's `EntryFacts`
        /// carries the same id, and a fold refuses one that doesn't. A step that writes no
        /// message leaves it unused. Absent on steps written before it was recorded.
        3 opt mid: String,
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
        /// The run's last step, taken after its tool-call budget asked the model to finish. The
        /// transcript's `Assistant` carries the same flag; each fold reads only its own record.
        11 opt budget_finalize: bool,
        /// The step's whole usage, when a provider fallback inside the step served several
        /// model calls and `usage` holds only the last one's. Absent when it equals `usage`.
        12 opt total_usage: Usage,
        /// Whether one of the run's stop conditions was met by this step.
        13 opt stop_met: bool,
        /// Whether a stop condition other than "the loop finished" was met, which forbids an
        /// extra step to place a steer that arrived during this one.
        14 opt limiting_stop_met: bool,
        /// The step's usage split by the model each part counts under, as the run loop decides
        /// it when it writes the step: a refusal fallback's or a provider's own hand-off moves
        /// usage off the run's model. The session's usage by model is the sum of these.
        15 rep [must] by_model: ModelStepUsage,
    }
}

message! {
    /// One model's share of a step's usage, as `StepEnd::by_model` and `StepRefused::by_model`
    /// record it. `variant` is the run's variant only when the run's own model answered the
    /// whole step: another model doesn't run the caller's variant.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ModelStepUsage {
        1 req provider: String,
        2 req model: String,
        3 opt variant: String,
        4 req usage: Usage,
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
        /// Whether the provider accepted the request for processing, so its prompt prefix was
        /// read into or written to the provider's cache, as the run loop decides it when it
        /// records the attempt. Absent means it didn't, or isn't known to have.
        19 opt reached_cache: bool,
        /// For a request that reached the cache: the cache lifetime it asked the provider for,
        /// when it asked for one the provider honours (a first-party Anthropic marker's `ttl`,
        /// or first-party OpenAI's 24-hour retention), as the run loop decides it from the
        /// run's frozen render config when it records the attempt. Only this lifetime can prove
        /// the cache expired. Absent when the request named none.
        20 opt requested_cache_ttl_ms: u64,
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
        /// The id of the tool-results message this batch's replies make, decided here as
        /// `StepStart::mid` is for the step's assistant message, with the same rules.
        5 opt mid: String,
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
        /// The converter derived `call_key` from today's log: today's intent stores no key, and
        /// the runner sent `<log lineage id>:<intent's position>`, which the provider keys the
        /// call's late result, approval and withdrawal on. The key is stored so it never has to
        /// be derived from a position again; this marks it as derived, so the record converts
        /// back exactly.
        13 opt key_derived: bool,
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
        /// The key of a call answered without a dispatch intent (denied, invalid, synthetic, or
        /// refused with its batch), which no intent holds: the new loop mints it from this
        /// record's writer generation and seq, in the form of the store's ids, and a converted
        /// call keeps today's key, made from the log's lineage and its terminal record's seq.
        /// Absent for a dispatched call, whose key is its intent's, and on outcomes written
        /// before it was recorded.
        11 opt call_key: String,
    }
}

message! {
    /// A compaction message from the first compaction protocol, found only on converted
    /// sessions, which keep the last view it applied. The body is the record as written; the
    /// first protocol's code is what reads it.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Compaction {
        /// Always 1: the protocol the message belongs to.
        1 req version: u32,
        2 req body: serde_json::Value,
    }
}

message! {
    /// Where the compaction provider's cut falls, from the current compaction contract: the
    /// first entry the applied view keeps, and the view that set it. It sits on the transcript
    /// because an edit and resend can remove it, which moves the cut back.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Marker {
        1 req view_version: u64,
        /// The first kept entry's message id and ordinal.
        2 req first_kept_mid: String,
        3 req first_kept_ordinal: u64,
        /// The part number the provider's numbering resumes from at the cut.
        4 req checkpoint: u64,
        /// The session the marker names, which a reader checks against the session it opened.
        5 opt harness: String,
        6 opt session: String,
    }
}

message! {
    /// A compaction view the provider handed over and broca accepted. It applies at the next
    /// step boundary, recorded by `ViewApplied`.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct View {
        1 req version: u64,
        2 req checksum: String,
        /// Owned by the compaction code: the view's edits as stored, each either the
        /// provider's edit as raw JSON or the index of the identical edit in the previous
        /// accepted view.
        3 req edits: serde_json::Value,
    }
}

message! {
    /// The view a step renders with, recorded before the step's request is sent.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ViewApplied {
        1 req step_id: u64,
        2 req version: u64,
        3 req checksum: String,
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
        /// The receipt: sender, principal, retention, title and hooks.
        8 opt receipt: serde_json::Value,
        /// How the send asked to be delivered, as the caller named it (`interrupt` drains ahead
        /// of every plain send). Absent for a plain queued send.
        9 opt delivery: String,
        /// Where the bytes of each medium the send's messages carry are stored. The run or
        /// boundary that places the messages points their transcript copies at the same objects.
        /// A reader that places or renders the messages needs them, so a record carrying any is
        /// refused by a reader that doesn't know the field.
        10 rep [must] media: SendMedia,
        /// The send's display marks (llm-runner's `MessageMarks` JSON), which reads serve on the
        /// messages it places. A reader that doesn't know the field would place them without
        /// their marks, so it is must-understand.
        11 opt [must] message_marks: serde_json::Value,
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
        /// The model the refreshed plan chooses for the session's next runs (its `model`), as
        /// admission read it from the plan; absent when the plan chooses none. A send that
        /// leaves its model to the session, and `session.head`, follow the latest refresh that
        /// chooses one.
        7 opt selection: serde_json::Value,
    }
}

message! {
    /// The pending refresh's policy changed (`session.refresh_policy`), durable before the
    /// caller is answered. `send_id` is the change's idempotency key: a repeat is answered from
    /// this record.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RefreshPolicy {
        1 req generation: u64,
        2 req policy: String,
        3 req send_id: String,
    }
}

message! {
    /// The session's frozen prefix was flushed (`session.flush_prefix`): the next run applies any
    /// pending refresh, whatever its policy. `pending_generation` is the refresh pending when the
    /// flush was accepted, which a repeat under the same `send_id` is answered with.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct PrefixFlush {
        1 req send_id: String,
        2 opt pending_generation: u64,
    }
}

message! {
    /// The run whose start shares this record's batch applied the pending refresh `generation`:
    /// `applied_by` is `fold` when a prefix rebuild applied it and `append` when the run applied
    /// it with the prefix intact. It carries the facts the session's baseline reports for the
    /// applied change, decided with it: the whole manifest's digest, its composition digest when
    /// it has one, and the plan items its fetch left absent. Written after the run's input
    /// records, so the run's start is still followed directly by its input.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RefreshApplied {
        1 req generation: u64,
        2 req applied_by: String,
        3 req manifest_digest: String,
        4 opt composition_digest: String,
        /// Owned by the fetch-plan code: each absent item as the baseline reports it.
        5 rep absent_items: serde_json::Value,
    }
}

message! {
    /// A session that ran outside the current compaction contract moved onto it: the run whose
    /// start shares this record's batch is its first run on the contract. The messages already
    /// placed, ordinals 1 through `through_ordinal`, keep their records and their facts; what
    /// `session.entries` serves about them on the contract was decided here, once, and is stored
    /// in the session object `overlay` names. Written after the run's input records, so the run's
    /// start is still followed directly by its input. A session enrolls at most once.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct CompactionEnrolled {
        /// The newest message placed before the enrolling run.
        1 req through_ordinal: u64,
        /// The highest part number any request before the enrolling run showed the model, or 0:
        /// the earlier messages' parts are numbered after it.
        2 req tag_floor: u64,
        /// The last part number the earlier messages took. The enrolling run's own messages are
        /// numbered after it.
        3 req tag_high: u64,
        4 req overlay: EnrollmentOverlayRef,
    }
}

message! {
    /// Where an enrollment's per-message decisions are stored: a session object written under the
    /// open log, as an import's id map is, and durable before the batch whose
    /// `CompactionEnrolled` names it. Its body is an [`EnrolledEntries`].
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct EnrollmentOverlayRef {
        1 req log: crate::codec::Id128,
        2 req generation: crate::codec::Id128,
        /// The seq the session's next record would take when the object was written, which the
        /// enrolling run's start batch then takes unless another record came first. It keeps the
        /// object's name apart from every other enrollment object the session could write: a
        /// later one is written at a later seq, unless no batch named this one.
        3 req seq: u64,
    }
}

message! {
    /// What `session.entries` serves about one message placed before a session enrolled in the
    /// current compaction contract, in place of its own facts' contract members, which were
    /// recorded off the contract.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct EnrolledEntry {
        1 req ordinal: u64,
        /// The part number before the message's first numbered part, in the projection's
        /// numbering.
        2 req tag_base: u64,
        /// The message's own time, else the newest earlier entry's, else 0.
        3 req written_at_ms: u64,
        /// A global system message: the enrolling run's system text takes its place, so it is
        /// neither served nor rendered, and takes no part numbers.
        4 req suppressed: bool,
        /// Who delivered it, where the session's records show a module other than the owner
        /// sent it.
        5 opt notice: Notice,
    }
}

message! {
    /// An enrollment's per-message decisions, the body of the object [`EnrollmentOverlayRef`]
    /// names: one entry per ordinal from 1 through the enrollment's `through_ordinal`, in order.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct EnrolledEntries {
        1 rep entries: EnrolledEntry,
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

impl Value for PauseReason {
    const WIRE: Wire = Wire::Varint;
    const NAME: &'static str = "PauseReason";
    fn write(&self, out: &mut Vec<u8>) {
        let n: u64 = match self {
            PauseReason::AuthRequired => 1,
            PauseReason::Restart => 2,
            PauseReason::AccountLimit => 3,
            PauseReason::ToolHeld => 4,
        };
        put_varint(out, n);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let n = u64::read(raw).map_err(|e| DecodeError::new(Self::NAME, e.kind))?;
        Ok(match n {
            1 => PauseReason::AuthRequired,
            2 => PauseReason::Restart,
            3 => PauseReason::AccountLimit,
            4 => PauseReason::ToolHeld,
            other => {
                return Err(DecodeError::new(
                    Self::NAME,
                    DecodeErrorKind::UnknownVariant(other),
                ))
            }
        })
    }
}

message! {
    /// A run continued after a crash or a restart: what it rebuilt from its records without
    /// calling anyone, what it does again because no durable record of it survived, and which
    /// calls may have run without a recorded result. Those calls are never sent again.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Resumed {
        1 req run_id: String,
        /// The seq of the last durable record the resume read.
        2 req resumed_from_seq: u64,
        /// Messages of the rebuilt conversation, every earlier run included.
        3 req messages: u64,
        /// The run's model steps whose answer is durable and isn't asked for again.
        4 req model_steps: u64,
        /// Durable results of the run's open tool batch, used as recorded.
        5 req tool_results: u64,
        6 rep redone: Redo,
        7 rep indeterminate: ToolCallId,
    }
}

message! {
    /// One thing a resume does again: a model step asked for again (`step_id`), or a planned
    /// call dispatched because it has no durable intent (`tool_call_id`). Exactly one is set.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct Redo {
        1 opt step_id: u64,
        2 opt tool_call_id: ToolCallId,
    }
}

message! {
    /// A run paused without ending: it waits for re-authentication, a restart, an account
    /// limit's reset, or a held tool call's late result, and resumes as the same run.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Pause {
        1 req run_id: String,
        2 req reason: PauseReason,
        3 opt ts_ms: u64,
        /// Owned by the provider layer: the error that paused the run.
        4 opt error: serde_json::Value,
        /// Owned by the run loop: when an account-limit pause may resume.
        5 opt account_limit: serde_json::Value,
    }
}

message! {
    /// A tool provider took responsibility for answering a dispatched call later. The call is
    /// not indeterminate while it is held: its result arrives as a late result.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ToolHeld {
        1 req batch_id: String,
        2 req tool_call_id: ToolCallId,
        3 req call_key: String,
        4 req module: String,
        5 req reason: String,
        6 opt detail_digest: String,
        7 req held_at_ms: u64,
        /// Whether later calls of the batch wait for this one.
        8 req sequential: bool,
    }
}

values! {
    /// What a tool-call budget firing does.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum BudgetFiring {
        /// Reminds the model how many calls it has made.
        1 Nudge,
        /// Asks the model to finish: the next step is the run's last, with tools disabled.
        2 Finalize,
    }
}

message! {
    /// A tool-call budget threshold fired. The message it placed is a transcript `Input` right
    /// after it.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct BudgetFire {
        1 req run_id: String,
        2 req firing: BudgetFiring,
        3 req threshold: u64,
        /// The conversation position the message took, as today's log recorded it.
        4 req ordinal: u64,
        /// The text the record stored; older records didn't store it.
        5 opt caller_text: String,
        6 opt tags_v1: serde_json::Value,
        /// For a nudge, its index in the run's frozen budget. Decided when the firing was
        /// written, so the fold never re-derives which nudge fired.
        7 opt nudge_index: u64,
    }
}

message! {
    /// A steer accepted into a running run. It keeps its messages: they are durable from
    /// acceptance, and the transcript gets copies where the next step boundary places them.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Steer {
        1 rep input: Message,
        2 opt written_at_ms: u64,
        3 opt tags_v1: serde_json::Value,
        /// The receipt: sender, send id, delivery, retention, title and hooks.
        4 opt receipt: serde_json::Value,
        /// Where the bytes of each medium the steer's messages carry are stored, which the
        /// messages' transcript copies point at. A record carrying any is refused by a reader
        /// that doesn't know the field.
        5 rep [must] media: SendMedia,
        /// The steer's display marks (llm-runner's `MessageMarks` JSON), which reads serve on the
        /// messages it places. A reader that doesn't know the field would place them without
        /// their marks, so it is must-understand.
        6 opt [must] message_marks: serde_json::Value,
    }
}

message! {
    /// A queued send delivered into a running run: as user messages placed at the next step
    /// boundary, as turn-scoped system text, or as text appended to a tool result.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Delivery {
        1 req submission_id: String,
        2 req run_id: String,
        /// `steer`, `turn_system` or `tool_result`.
        3 req mode: String,
        4 rep input: Message,
        5 opt tool_call_id: ToolCallId,
        6 opt appended_text: String,
        7 opt written_at_ms: u64,
        8 opt tags_v1: serde_json::Value,
        /// Whether the record listed `input` at all, which an empty list can't show.
        9 opt input_listed: bool,
    }
}

values! {
    /// When a caller asked a run to stop.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum CancelPoint {
        1 Now,
        2 StepBoundary,
    }
}

message! {
    /// A queued send withdrawn before it was placed: by its sender, or because admitting it as
    /// a run failed.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Retraction {
        1 req submission_id: String,
        /// Owned by admission: why admitting the send failed, when it did.
        2 opt failure: serde_json::Value,
    }
}

message! {
    /// A queued send ended undelivered: replaced by a newer send with the same dedupe key, or
    /// expired.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct SendEnded {
        1 req submission_id: String,
        /// Owned by the queue: why it ended.
        2 opt reason: serde_json::Value,
    }
}

message! {
    /// The ordered delivery list of a queued send, written just before its `QueuedSend`.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct DeliveryOrdered {
        1 req submission_id: String,
        /// Owned by the queue: the delivery modes in order, the dedupe key and the expiry.
        2 req order: serde_json::Value,
        /// The sender whose dedupe key the send carries, when it isn't the session's owner.
        3 opt sent_by: serde_json::Value,
    }
}

message! {
    /// The context-ceiling check before a step's request: the request doesn't fit the model's
    /// window even after the compaction provider's answer, so the run ends without sending it.
    /// `detail` is the protocol's `ContextCeilingDetail` (the usable window, the request's tokens
    /// and why), kept as the caller reads it; absent when the check let the step send.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct CeilingDecided {
        1 req step_id: u64,
        2 opt detail: serde_json::Value,
    }
}

message! {
    /// A step's output-token cap, lowered below the run's frozen cap before the step's request
    /// was sent, so the estimated input plus the output reservation stays inside the context
    /// limit. Written only for a step whose cap was lowered, and durable before the request goes
    /// out, so a resume renders the cap the provider saw.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct OutputCapLowered {
        1 req step_id: u64,
        /// The cap the step's request carries.
        2 req max_output_tokens: u32,
        /// The input-token estimate the cap was derived from, kept for diagnosis only.
        3 req estimated_input_tokens: u64,
        /// The run's frozen context limit the estimate was checked against.
        4 req context_limit: u64,
    }
}

message! {
    /// The provider refused a run's strict structured-output schema as too large a grammar, so
    /// the run's schema goes out without the strict flag from here on: its step is sent again,
    /// and a resume renders the run downgraded. Durable before the step is sent again.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct StructuredDowngraded {
        1 req run_id: String,
    }
}

message! {
    /// The run's own model refused a step with a content filter, and the step goes to the
    /// run's refusal-fallback model instead. The refused answer is not kept. Its billed usage
    /// counts toward the run, and the schedule the refusal armed applies to the session. Durable
    /// before the step is sent to the fallback, and a resume sends the step there again.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct StepRefused {
        1 req run_id: String,
        2 req step_id: u64,
        3 req usage: Usage,
        /// Owned by the provider layer: the model that answered and each model's usage, when
        /// the provider handed the request on itself.
        4 opt served_model: String,
        5 opt usage_iterations: serde_json::Value,
        /// How many content blocks the refused answer held.
        6 req discarded_blocks: u32,
        /// Owned by the run loop: the refusal-fallback schedule after this refusal.
        7 req schedule: serde_json::Value,
        8 opt ts_ms: u64,
        /// The refused step's usage split by model, as `StepEnd::by_model`: a refused step was
        /// billed, so it counts as a step of the model that refused.
        9 rep [must] by_model: ModelStepUsage,
    }
}

message! {
    /// A paused run resumed before any provider call, and its provider now renders with a
    /// different config than the run froze: the run renders with this one from here on. Only
    /// before the first call is that safe, since no request bytes depend on the old config.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ConfigRefrozen {
        1 req run_id: String,
        2 opt ts_ms: u64,
        3 req renderer_version: u32,
        /// Owned by the provider layer, as the run start's render config is.
        4 req render_config: serde_json::Value,
    }
}

message! {
    /// A paused run resumed on another provider account than it froze, which routing gave it:
    /// its later requests and its spend name this account. Account metadata changes no request
    /// bytes, so this may follow provider calls. Not written for a run paused on its account's
    /// limit, which resumes only on that account.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct AuthRefrozen {
        1 req run_id: String,
        2 opt ts_ms: u64,
        /// Owned by admission, as the run start's auth selection is.
        3 req auth_selection: serde_json::Value,
    }
}

message! {
    /// The runner sent a request to keep the session's prompt cache warm: the prefix a step's
    /// request wrote, read again so it outlives the wait. Billed, and counted toward how long
    /// the cache lives.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Warmed {
        1 req run_id: String,
        /// The step whose request was refreshed.
        2 req step_id: u64,
        /// What asked for it: `keep_warm` for a run's own tool wait, `source_keep_warm`, or
        /// absent for a caller's manual warm.
        3 opt trigger: String,
        /// The step's request whose prefix was refreshed.
        4 req request_body_sha256: Digest,
        /// The warm request's own body.
        5 req warm_body_sha256: Digest,
        6 req usage: Usage,
        7 opt served_account_ref: String,
        8 req sent_at_ms: u64,
        /// The cache lifetime the warm was scheduled by, and when the cache then expires.
        9 opt ttl_ms: u64,
        10 opt expires_at_ms: u64,
        11 opt attempt_id: String,
        /// Owned by billing: the reservation and envelope of a billed warm.
        12 opt billing: serde_json::Value,
        /// Owned by account routing: the provider's rate-limit headers on the warm's answer.
        13 opt ratelimit_headers: serde_json::Value,
        /// Whether the warm reached the provider's cache, as decided when it was recorded.
        /// Absent means it didn't, or isn't known to have.
        14 opt reached_cache: bool,
        /// For a warm that reached the cache: the lifetime the run's requests ask for, when
        /// they ask for one the provider honours, as on `Attempt`.
        15 opt requested_cache_ttl_ms: u64,
    }
}

message! {
    /// A run's spend facts are in the usage export. A run's end makes its billing owed (see
    /// `Operations::billing_owed`); this record, written only after the facts' insert commits,
    /// settles it. Billing a run again inserts nothing new, so a crash between the insert and
    /// this record only bills the run again.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct Billed {
        1 req run_id: String,
    }
}

message! {
    /// A caller asked to cancel a run.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct CancelRequest {
        1 req run_id: String,
        2 req at: CancelPoint,
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

message! {
    /// A writer opened the log: it committed a new generation in the head and may now mint
    /// identities from (generation, seq). Written as a barrier before the writer's first other
    /// record, so the head and the log can't name different generations.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct WriterOpened {
        1 req generation: crate::codec::Id128,
    }
}

crate::values! {
    /// What a change does.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    pub enum ChangeKind {
        /// Seals the transcript before a compaction cut, and every ledger record.
        1 Cut,
        /// Seals every ledger record and keeps the transcript open.
        2 LedgerSeal,
        /// Ends the transcript branch before an earlier user input and starts a new run there
        /// with an edited input (see `session::revert`). Seals every ledger record.
        3 Revert,
        /// Seals every ledger record of a session idle for days, and, if no run is unfinished,
        /// prunes finished runs' opaque reasoning from the transcript it keeps open (see
        /// `session::cold`).
        4 Cold,
        /// Repairs a damaged open log (see `session::recover`). Its intent lives in the head, not
        /// in the damaged log, which nothing writes to again.
        5 Recover,
    }
}

message! {
    /// A change began: it may now create objects under its id and a new open log. Written as a
    /// barrier before anything is created, it closes the log: the only record the log accepts
    /// after it is the change's `ChangeAborted`. A committed change settles it by making its new
    /// log the session's open log.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ChangeIntent {
        1 req change: crate::codec::Id128,
        2 req kind: ChangeKind,
        /// For a cut, the seq of the first record kept above the cut. For a revert, the seq of
        /// the first transcript record the branch drops.
        3 opt cut: u64,
    }
}

message! {
    /// A revert ended the transcript branch before message `ordinal` and started a new run
    /// there. Its records stay in the ledger: spend, tool side effects and accepted sends aren't
    /// undone. Only a revert's own ledger segment holds this record, never the log the revert
    /// replaced, so a revert that never committed leaves no trace of it.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct TranscriptReverted {
        1 req change: crate::codec::Id128,
        /// The seq of the first transcript record dropped, and of the last one the branch held.
        2 req from_seq: u64,
        3 req through_seq: u64,
        4 req ordinal: u64,
        /// The session's history version after the revert.
        5 req history_version: u64,
        /// On a session on the current compaction contract: the highest part number any message
        /// had taken before the revert, which the dropped branch's messages may have held. Part
        /// numbers never repeat in a session, so later messages are numbered above it. A reader
        /// that ignored it would number them again from the kept branch, so it is
        /// must-understand. Absent off the contract, where part numbers aren't served.
        6 opt [must] tag_high: u64,
    }
}

message! {
    /// A recovery replaced a damaged open log (see `session::recover`). Only the recovery's own
    /// ledger segment holds this record.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct RecoveryCut {
        1 req change: crate::codec::Id128,
        /// The first damaged byte of the replaced log, and its durable bound.
        2 req at: u64,
        3 req durable_bound: u64,
        /// Seqs from `removed_from` through `removed_through` that the new log doesn't hold are
        /// removed: they were in the damaged bytes, or were never issued. Records written after
        /// the recovery take seqs above `removed_through`.
        4 req removed_from: u64,
        5 req removed_through: u64,
        /// Some record may have been lost, so open work was closed and a damage notice written.
        6 req loss_aware: bool,
        /// When transcript records may have been lost: the first transcript seq dropped, the
        /// ordinal from which the compaction provider must read again, and the new history
        /// version.
        7 opt transcript_from: u64,
        8 opt ordinal: u64,
        9 opt history_version: u64,
        /// The ledger may have lost records of actions no surviving record identifies: an action
        /// may have run without any record of it.
        10 req unattributed_risk: bool,
        /// The object holding the replaced log's bytes from `at` on, kept for an operator.
        11 req saved: String,
    }
}

message! {
    /// A change that never committed was undone: its objects and new log are deleted, and the log
    /// accepts records again.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct ChangeAborted {
        1 req change: crate::codec::Id128,
    }
}

impl Record {
    /// The time this record says something happened in the session, from whichever time field
    /// its kind carries. `None` for kinds that carry none, which includes `WriterOpened`: opening
    /// a writer isn't activity.
    pub fn activity_ms(&self) -> Option<u64> {
        match self {
            Record::Input(r) => r.written_at_ms,
            Record::Assistant(r) => r.last_content_at_ms.or(r.written_at_ms),
            Record::ToolReply(r) => r.finished_at_ms,
            Record::ResultAppend(r) => r.written_at_ms,
            Record::EntryFacts(r) => r.written_at_ms,
            Record::RunStart(r) => r.written_at_ms.or(r.ts_ms),
            Record::RunEnd(r) => r.ts_ms,
            Record::Attempt(r) => r.last_content_at_ms.or(Some(r.ts_ms)),
            Record::ToolOutcome(r) => r.finished_at_ms,
            Record::QueuedSend(r) => r.written_at_ms.or(r.ts_ms),
            Record::Pause(r) => r.ts_ms,
            Record::Steer(r) => r.written_at_ms,
            Record::Delivery(r) => r.written_at_ms,
            _ => None,
        }
    }
}

records! {
    transcript {
        1 Input,
        2 Assistant,
        3 ToolReply,
        4 ResultAppend,
        5 SetupStub,
        6 Compaction,
        7 Marker,
        8 EntryFacts,
        9 TurnFacts,
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
        77 Pause,
        78 ToolHeld,
        79 BudgetFire,
        80 Steer,
        81 Delivery,
        82 CancelRequest,
        83 Retraction,
        84 SendEnded,
        85 DeliveryOrdered,
        86 View,
        87 ViewApplied,
        88 WriterOpened,
        89 ChangeIntent,
        90 ChangeAborted,
        91 TranscriptReverted,
        92 RecoveryCut,
        93 Resumed,
        94 CeilingDecided,
        95 OutputCapLowered,
        96 StructuredDowngraded,
        97 Warmed,
        98 ConfigRefrozen,
        99 AuthRefrozen,
        100 StepRefused,
        101 Billed,
        102 RefreshPolicy,
        103 PrefixFlush,
        104 RefreshApplied,
        105 CompactionEnrolled,
    }
}

#[cfg(test)]
mod tests;
