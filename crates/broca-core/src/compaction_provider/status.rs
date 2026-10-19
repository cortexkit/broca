// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! `compaction.step`: the per-step status the runner sends.
//!
//! The status carries everything the provider needs for one step: the
//! messages added since its last call, with the ordinal cursor they start
//! after, so it never depends on reading the runner's storage; the id and
//! version of the last applied CompactionMessage, so a provider that lost
//! its counter resumes above it; and the step's model, window and usage
//! numbers. For anything older the provider reads the transcript itself.
//!
//! Decoded leniently: a provider ignores fields it does not know, so a
//! newer runner can add some without breaking an older provider.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use cortexkit_role_step_transform_provider::subscription::{
    SubjectPartProblem, UnservedSubject,
};

/// History inherited by the current lineage, through the ordinal inclusive.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct DescendsFrom {
    pub lineage_id: String,
    pub through_ordinal: u64,
}

impl DescendsFrom {
    pub fn new(lineage_id: impl Into<String>, through_ordinal: u64) -> Self {
        Self {
            lineage_id: lineage_id.into(),
            through_ordinal,
        }
    }
}

/// A message the runner wrote, by its place in the lineage.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MessageRef {
    pub ordinal: u64,
    /// The runner's opaque message id.
    pub mid: String,
}

/// One original message: `ordinal` is its position in the transcript lineage,
/// `mid` is its stable opaque id and `message` is its parsed final body. The
/// optional sidecar preserves original native JSON bytes instead of rebuilding
/// them from the parsed body. Unknown fields are ignored.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct StatusMessage {
    pub ordinal: u64,
    pub mid: String,
    pub message: Value,
    /// Original provider-message JSON bytes with only recognized transport
    /// cache_control members removed; never reconstructed from `message`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_message: Option<String>,
}

/// Token usage the previous step reported.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Usage {
    pub input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub output: u64,
    /// When the step completed, in milliseconds since the Unix epoch.
    pub completed_at: u64,
    /// The model provider's finish reason, as an open string.
    pub finish_reason: String,
}

/// The runner's size estimate of the request it is about to send.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Estimate {
    /// Estimated input tokens of the request about to be sent. An estimate:
    /// most runners have no tokenizer for most model families.
    pub request_tokens: u64,
    /// The previous step's measured input tokens, when there was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_input: Option<u64>,
}

impl Estimate {
    pub fn new(request_tokens: u64) -> Self {
        Self {
            request_tokens,
            previous_input: None,
        }
    }

    pub fn with_previous_input(mut self, input: u64) -> Self {
        self.previous_input = Some(input);
        self
    }
}

/// Set when this step's prefix is being rebuilt for another reason, so a
/// compaction on this step costs no extra prompt-cache miss.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PrefixRebuild {
    /// Why, as an open string: `flush`, `manifest_change`, `model_switch`
    /// or `expired_cache` ([`rebuild_reasons`]).
    pub reason: String,
}

/// The values [`PrefixRebuild::reason`] takes today.
pub mod rebuild_reasons {
    pub const FLUSH: &str = "flush";
    pub const MANIFEST_CHANGE: &str = "manifest_change";
    pub const MODEL_SWITCH: &str = "model_switch";
    pub const EXPIRED_CACHE: &str = "expired_cache";
}

/// The kinds of step, as [`StepStatus::step_kind`] spells them.
pub mod step_kinds {
    /// The step that answers a new user turn.
    pub const USER_TURN: &str = "user_turn";
    /// A step that follows tool results within a turn.
    pub const TOOL_STEP: &str = "tool_step";
}

/// A CompactionMessage by its id and version.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct AppliedRef {
    pub compaction_id: String,
    pub version: u64,
}

/// The newest CompactionMessage the runner recorded and did not apply, and
/// why: a [`crate::compaction_provider::fence::NotApplied::name`], or [`STRUCTURAL`] for the
/// runner's own checks. `reason` is decoded open.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NotAppliedRef {
    pub compaction_id: String,
    pub version: u64,
    pub reason: String,
}

impl NotAppliedRef {
    pub fn new(compaction_id: impl Into<String>, version: u64, reason: impl Into<String>) -> Self {
        Self {
            compaction_id: compaction_id.into(),
            version,
            reason: reason.into(),
        }
    }
}

/// The `last_not_applied.reason` a runner writes when its own structural
/// checks (roles, tool-call pairing, its validity rules) fail a
/// CompactionMessage that passed the fence.
pub const STRUCTURAL: &str = "structural";

/// The byte cap on a status's `messages` a runner uses unless it is
/// configured otherwise: 4 MiB, measured as the sum of each entry's compact
/// JSON encoding ([`message_bytes`]).
pub const DEFAULT_MESSAGE_CAP_BYTES: usize = 4 * 1024 * 1024;

/// The size a message counts against the cap: the length of its compact
/// JSON encoding as a status entry `{ordinal, mid, message}`.
pub fn message_bytes(message: &StatusMessage) -> usize {
    serde_json::to_vec(message).map_or(0, |bytes| bytes.len())
}

/// How many of `pending` (the messages after the cursor, oldest first) one
/// status carries under `cap_bytes`, and whether more remain (`more`).
/// Messages are taken in order while their total stays within the cap. The
/// first message is always taken, so a single message over the cap is sent
/// alone and the cursor still moves past it; a later message that would
/// overflow the cap waits for the next status.
pub fn take_capped(pending: &[StatusMessage], cap_bytes: usize) -> (usize, bool) {
    let mut used = 0usize;
    let mut count = 0usize;
    for message in pending {
        let size = message_bytes(message);
        if count > 0 && used.saturating_add(size) > cap_bytes {
            break;
        }
        used = used.saturating_add(size);
        count += 1;
    }
    (count, count < pending.len())
}

/// The `compaction.step` request.
///
/// Non-exhaustive so a later optional member is additive: decode one, or
/// build one with [`StepStatus::new`] and the `with_*` setters.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct StepStatus {
    /// The runner's opaque name for the session. The provider keys its own
    /// per-session state on it and echoes it verbatim in `compaction.ready`;
    /// it never uses it to choose what it returns.
    pub session: String,
    /// The harness named in the session's key, which identifies the caller
    /// (for example `broca`). This differs from the route's bind harness,
    /// the harness a connection declares when it opens the route; a runner
    /// always declares `runner`. Required: a request without it does
    /// not decode. A provider keys a runner conversation on
    /// `(project_root, session, harness)`.
    pub harness: String,
    /// This request's id, an opaque string the provider compares only for
    /// equality. Every answer names it, and `compaction.ready` echoes it.
    pub request_id: String,
    /// The session's current lineage. Ordinals, the cursor and ranges are
    /// all in this lineage.
    pub lineage_id: String,
    /// The step this status is for, opaque.
    pub step_id: String,
    /// `user_turn` or `tool_step` ([`step_kinds`]), decoded open.
    pub step_kind: String,
    /// The model the step is for.
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The model's context window in tokens, when the runner's model catalog
    /// resolves it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    /// The model's output limit in tokens, when resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_limit: Option<u64>,
    /// The previous step's usage, when it recorded any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_usage: Option<Usage>,
    /// When the previous run ended `error`, its `provider_code`. An overflow
    /// ends a run that way with no usage, so this can come without
    /// `previous_usage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_provider_code: Option<String>,
    pub estimate: Estimate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_rebuilding: Option<PrefixRebuild>,
    /// The newest message written in the lineage; absent when it has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest: Option<MessageRef>,
    /// The last applied CompactionMessage. Absent only before Setup's is
    /// recorded, which never happens on a step call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied: Option<AppliedRef>,
    /// The newest CompactionMessage recorded and not applied, if it is newer
    /// than the last applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_not_applied: Option<NotAppliedRef>,
    /// The cursor: the last ordinal of this lineage the provider was sent.
    /// Absent on the first call in the lineage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ordinal: Option<u64>,
    /// Every message after the cursor, oldest first, contiguous from it.
    pub messages: Vec<StatusMessage>,
    /// `true` when the runner stopped `messages` at its byte cap before the
    /// newest message. The cursor then advances only to the last message
    /// sent, and the next status carries the rest.
    #[serde(default, skip_serializing_if = "is_false")]
    pub more: bool,
    /// The runner's clock, in milliseconds since the Unix epoch.
    pub now: u64,
    /// History the current lineage continues. The provider must hold the
    /// named lineage through this ordinal or refuse `history_unreadable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descends_from: Option<DescendsFrom>,
    /// Highest durably served ordinal after the pass's append transaction.
    /// Absence grants no ordinal-confirmed promotion; it is not zero or newest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_through_ordinal: Option<u64>,
    /// Subjects whose pending output must be sent before a compaction view can replace
    /// them. Entries use the step-transform provider's `claude_code::UnservedSubject`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unserved_subjects: Option<Vec<UnservedSubject>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geometry: Option<crate::compaction_provider::geometry::Geometry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_reserve: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mid_turn: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill: Option<crate::compaction_provider::geometry::Fill>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel2_delivered_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel2_receipt: Option<crate::compaction_provider::notice::DispatchReceipt>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl StepStatus {
    /// A status with the required members and nothing else.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: impl Into<String>,
        harness: impl Into<String>,
        request_id: impl Into<String>,
        lineage_id: impl Into<String>,
        step_id: impl Into<String>,
        step_kind: impl Into<String>,
        model: impl Into<String>,
        estimate: Estimate,
        now: u64,
    ) -> Self {
        Self {
            session: session.into(),
            harness: harness.into(),
            request_id: request_id.into(),
            lineage_id: lineage_id.into(),
            step_id: step_id.into(),
            step_kind: step_kind.into(),
            model: model.into(),
            variant: None,
            context_window: None,
            output_limit: None,
            previous_usage: None,
            previous_provider_code: None,
            estimate,
            prefix_rebuilding: None,
            newest: None,
            last_applied: None,
            last_not_applied: None,
            after_ordinal: None,
            messages: Vec::new(),
            more: false,
            now,
            descends_from: None,
            served_through_ordinal: None,
            unserved_subjects: None,
            geometry: None,
            output_reserve: None,
            mid_turn: None,
            fill: None,
            channel2_delivered_id: None,
            channel2_receipt: None,
        }
    }

    pub fn with_geometry(
        mut self,
        geometry: crate::compaction_provider::geometry::Geometry,
    ) -> Self {
        self.geometry = Some(geometry);
        self
    }

    pub fn with_output_reserve(mut self, tokens: u64) -> Self {
        self.output_reserve = Some(tokens);
        self
    }

    pub fn with_mid_turn(mut self, mid_turn: bool) -> Self {
        self.mid_turn = Some(mid_turn);
        self
    }

    pub fn with_fill(mut self, fill: crate::compaction_provider::geometry::Fill) -> Self {
        self.fill = Some(fill);
        self
    }

    pub fn with_channel2_receipt(
        mut self,
        directive_id: impl Into<String>,
        receipt: crate::compaction_provider::notice::DispatchReceipt,
    ) -> Self {
        self.channel2_delivered_id = Some(directive_id.into());
        self.channel2_receipt = Some(receipt);
        self
    }

    /// Lane-only validation, separate from ordinary status validation. Caller
    /// identity must come from the daemon connection, not this status's harness.
    pub fn validate_claude_code(
        &self,
        lane: &cortexkit_role_step_transform_provider::claude_code::ClaudeCodeLane,
        principal: &str,
        connection: &str,
    ) -> Result<(), cortexkit_role_step_transform_provider::claude_code::InvalidParams> {
        use cortexkit_role_step_transform_provider::claude_code::InvalidParams;
        let Some(bundle) = lane.selected_for(principal, connection)? else {
            return Ok(());
        };
        if !self.geometry.as_ref().is_some_and(|geometry| {
            geometry.is_valid(self.context_window)
                && self.output_reserve.is_none_or(|reserve| {
                    geometry
                        .usable_hard
                        .checked_add(reserve)
                        .is_some_and(|reserved| reserved <= geometry.absolute_wall)
                })
        }) {
            return Err(InvalidParams::new("geometry"));
        }
        if self.channel2_delivered_id.is_some() != self.channel2_receipt.is_some() {
            return Err(InvalidParams::new("channel2_receipt"));
        }
        if self.channel2_receipt.is_some() && !bundle.notice_dispatch_ack {
            return Err(InvalidParams::new("channel2_receipt"));
        }
        for message in &self.messages {
            cortexkit_role_step_transform_provider::claude_code::check_raw_message(
                message.raw_message.as_deref(),
            )?;
            if serde_json::to_vec(message)
                .map_err(|_| InvalidParams::new("messages"))?
                .len()
                > bundle.caps.single_message_bytes
            {
                return Err(InvalidParams::new("messages"));
            }
        }
        let envelope = crate::compaction_provider::OpRequest::new(
            crate::compaction_provider::ops::COMPACTION_STEP,
            self,
        );
        if serde_json::to_vec(&envelope)
            .map_err(|_| InvalidParams::new("messages"))?
            .len()
            >= bundle.caps.ingress_envelope_bytes
        {
            return Err(InvalidParams::new("messages"));
        }
        Ok(())
    }

    pub fn with_descends_from(mut self, descends_from: DescendsFrom) -> Self {
        self.descends_from = Some(descends_from);
        self
    }

    pub fn with_served_through_ordinal(mut self, ordinal: u64) -> Self {
        self.served_through_ordinal = Some(ordinal);
        self
    }

    pub fn with_unserved_subjects(mut self, subjects: Vec<UnservedSubject>) -> Self {
        self.unserved_subjects = Some(subjects);
        self
    }

    /// Validate burn-entry part ids before any promote-and-burn mutation.
    /// Admission, held-history conflicts and watermark monotonicity require
    /// provider context and are not checked here. Check message ordering with
    /// [`Self::check_messages`] as well.
    pub fn validate(&self) -> Result<(), SubjectPartProblem> {
        cortexkit_role_step_transform_provider::subscription::validate_unserved_subjects(
            self.unserved_subjects.as_deref().unwrap_or_default(),
        )
    }

    /// Set the cursor and the messages after it.
    pub fn with_messages(
        mut self,
        after_ordinal: Option<u64>,
        messages: Vec<StatusMessage>,
    ) -> Self {
        self.after_ordinal = after_ordinal;
        self.messages = messages;
        self
    }

    /// Set the last applied CompactionMessage.
    pub fn with_last_applied(mut self, compaction_id: impl Into<String>, version: u64) -> Self {
        self.last_applied = Some(AppliedRef {
            compaction_id: compaction_id.into(),
            version,
        });
        self
    }

    pub fn with_model_details(
        mut self,
        variant: Option<String>,
        context_window: Option<u64>,
        output_limit: Option<u64>,
    ) -> Self {
        self.variant = variant;
        self.context_window = context_window;
        self.output_limit = output_limit;
        self
    }

    pub fn with_previous(mut self, usage: Option<Usage>, code: Option<String>) -> Self {
        self.previous_usage = usage;
        self.previous_provider_code = code;
        self
    }

    pub fn with_prefix_rebuilding(mut self, reason: impl Into<String>) -> Self {
        self.prefix_rebuilding = Some(PrefixRebuild {
            reason: reason.into(),
        });
        self
    }

    pub fn with_newest(mut self, newest: MessageRef) -> Self {
        self.newest = Some(newest);
        self
    }

    pub fn with_not_applied(mut self, reference: NotAppliedRef) -> Self {
        self.last_not_applied = Some(reference);
        self
    }

    pub fn with_more(mut self, more: bool) -> Self {
        self.more = more;
        self
    }

    /// Set the cursor and as many of `pending` as fit under `cap_bytes`
    /// ([`take_capped`]), with `more` when some are left for the next
    /// status.
    pub fn with_capped_messages(
        self,
        after_ordinal: Option<u64>,
        pending: &[StatusMessage],
        cap_bytes: usize,
    ) -> Self {
        let (count, more) = take_capped(pending, cap_bytes);
        self.with_messages(after_ordinal, pending[..count].to_vec())
            .with_more(more)
    }

    /// Where the cursor stands once this status's answer is durable: the
    /// last message sent, or the old cursor when none was sent. A capped
    /// status advances it only to its last message, never to `newest`.
    pub fn next_cursor(&self) -> Option<u64> {
        self.messages
            .last()
            .map(|message| message.ordinal)
            .or(self.after_ordinal)
    }

    /// Check what a provider relies on: the messages are oldest first,
    /// strictly after the cursor, and none is newer than `newest`.
    pub fn check_messages(&self) -> Result<(), StatusProblem> {
        let mut previous = self.after_ordinal;
        for message in &self.messages {
            if previous.is_some_and(|p| message.ordinal <= p) {
                return Err(StatusProblem::MessagesOutOfOrder);
            }
            previous = Some(message.ordinal);
        }
        let newest = self.newest.as_ref().map(|n| n.ordinal);
        if previous.is_some() && previous > newest && !self.messages.is_empty() {
            return Err(StatusProblem::MessageAfterNewest);
        }
        Ok(())
    }

    /// The lowest version the provider's next CompactionMessage may carry:
    /// one above the higher of the last applied version and `own`, the
    /// provider's own counter (`None` when it lost it).
    pub fn next_version(&self, own: Option<u64>) -> u64 {
        let applied = self.last_applied.as_ref().map_or(0, |a| a.version);
        applied.max(own.unwrap_or(0)).saturating_add(1)
    }
}

/// Why a status breaks what a provider relies on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusProblem {
    /// A message is not strictly after the cursor or the one before it.
    MessagesOutOfOrder,
    /// A message is newer than `newest`.
    MessageAfterNewest,
}

impl StatusProblem {
    pub fn name(self) -> &'static str {
        match self {
            Self::MessagesOutOfOrder => "messages_out_of_order",
            Self::MessageAfterNewest => "message_after_newest",
        }
    }
}
