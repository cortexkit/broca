//! The runner's side of asynchronous compaction views (compaction-provider/v1 §5 to §8):
//! deciding what to do with a fetched view, assembling the history a request sends under it,
//! the strip-or-defer rule for signed reasoning, the context ceiling, and advance notices.
//!
//! Everything here is a pure function of the session's folded [`CompactionState`] and its
//! transcript. The run loop records each decision in the WAL (`CompactionViewApplied`,
//! `CompactionNotice`) before anything renders or publishes it, so a resumed run renders the
//! same bytes without asking the provider again.

use std::collections::BTreeMap;

use broca_protocol::{tags::part_key, ContentBlock, Message, Role, Usage};
use broca_wal::{CompactionStrip, CompactionViewChoice, OpaqueJson};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    conversation, is_signed, native_message, native_schema_message, CompactionState, Conversation,
};
use crate::compaction_provider::{
    ceiling::{CeilingDecision, CeilingPolicy, WaitEnd},
    digest::content_digest,
    notice::{
        companion_edits, defer_reasons, headers, refusal_reasons, AdvanceNotice, Event, LastUsage,
        ViewReady,
    },
    ride::{Overflow, RideCause, RideRequest},
    view::{dispose_fetched, CompactionView, TranscriptMessage, ViewDisposition, ViewProblem},
};

/// A view the session applied, and how.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedView {
    pub view: CompactionView,
    pub choice: CompactionViewChoice,
}

impl AppliedView {
    /// Decode a recorded view. `None` when the record's view does not decode.
    pub(crate) fn decode(view: &OpaqueJson, choice: &CompactionViewChoice) -> Option<Self> {
        Some(Self {
            view: serde_json::from_str(view.as_json_text()).ok()?,
            choice: choice.clone(),
        })
    }
}

/// What the newest recorded notice announced: enough to tell whether a later boundary has
/// anything new to say.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Announced {
    #[serde(default)]
    pub est_tokens: Option<u64>,
    #[serde(default)]
    pub window: Option<u64>,
    pub lineage_id: Option<String>,
    pub through_ordinal: Option<u64>,
    pub applied_view_version: Option<u64>,
    pub model: Option<String>,
}

impl Announced {
    pub(crate) fn from_headers(map: &BTreeMap<String, String>) -> Self {
        let number = |name: &str| map.get(name).and_then(|value| value.parse().ok());
        Self {
            est_tokens: number(headers::EST_TOKENS),
            window: number(headers::WINDOW),
            lineage_id: map.get(headers::LINEAGE_ID).cloned(),
            through_ordinal: number(headers::THROUGH_ORDINAL),
            applied_view_version: number(headers::APPLIED_VIEW_VERSION),
            model: map.get(headers::MODEL).cloned(),
        }
    }
}

/// One transcript message as the role's checks see it: its ordinal, its id (`m<ordinal>`),
/// its part keys (`m<ordinal>#<index>`, the keys `session.read` tags) and its JSON as
/// `session.read` returns it. `None` for a message without an ordinal.
pub fn transcript_message(message: &Message) -> Option<TranscriptMessage> {
    let ordinal = message.ordinal?;
    Some(TranscriptMessage::new(
        ordinal,
        format!("m{ordinal}"),
        (0..message.content.len())
            .map(|index| part_key(ordinal, index))
            .collect(),
        serde_json::to_value(message).expect("native message JSON"),
    ))
}

/// The transcript messages of `prompt`: every message with an ordinal, in order, except
/// system messages, which the runner places ahead of the history itself (as it does under
/// Setup's initial message).
fn transcript(prompt: &[Message]) -> impl Iterator<Item = &Message> {
    prompt
        .iter()
        .filter(|message| message.ordinal.is_some() && message.role != Role::System)
}

/// Why a view cannot be assembled over the transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssemblyProblem {
    /// Bytes that are not messages or parts in broca's schema; the view is rejected.
    Rejected(ViewProblem),
    /// Messages or parts the compaction provider wrote itself that carry signed or encrypted
    /// reasoning, provider-signed text or a model origin, which only the model may produce
    /// (a byte-identical copy of the transcript is allowed); the view is refused
    /// `provider_constraint`.
    ProviderConstraint,
}

/// The history a request sends under `view` (compaction-provider/v1 §7): for each range in
/// order, the transcript's messages inside a keep range with their part edits applied, or a
/// replace range's messages, then every transcript message after the coverage. Transcript
/// messages keep their ordinals; the provider's messages have none. `CompactionView::assemble`
/// is the role's reference for the same rule over JSON.
pub fn assemble(
    view: &CompactionView,
    prompt: &[Message],
) -> Result<Vec<Message>, AssemblyProblem> {
    assemble_judged(view, prompt, None)
}

/// [`assemble`], also judging who wrote the view's bytes when `stored` is given: the transcript
/// as stored, which is what `session.read` returns and what a provider copies from (the prompt
/// shows the same messages with runner tags and loop guidance folded into their text). A
/// replace-range message or an edited part that carries signed or encrypted reasoning,
/// provider-signed text or a model origin must be a byte-identical copy of the stored message
/// or part it stands for; otherwise only the model could have written it.
fn assemble_judged(
    view: &CompactionView,
    prompt: &[Message],
    stored: Option<&[Message]>,
) -> Result<Vec<Message>, AssemblyProblem> {
    use crate::compaction_provider::view::ViewRange;
    let stored_at = |ordinal: u64| {
        stored.and_then(|stored| transcript(stored).find(|m| m.ordinal == Some(ordinal)))
    };
    let in_range = |from: u64, to: u64| {
        transcript(prompt).filter(move |m| m.ordinal.is_some_and(|o| o >= from && o < to))
    };
    let mut out = Vec::new();
    for range in &view.ranges {
        match range {
            ViewRange::Keep { from, to, edits } => {
                for message in in_range(*from, *to) {
                    let ordinal = message.ordinal.expect("transcript message");
                    let mine: Vec<_> = edits.iter().filter(|e| e.ordinal == ordinal).collect();
                    if mine.is_empty() {
                        out.push(message.clone());
                        continue;
                    }
                    let mut edited = message.clone();
                    edited.content.clear();
                    for (index, block) in message.content.iter().enumerate() {
                        let key = part_key(ordinal, index);
                        match mine.iter().find(|edit| edit.part == key) {
                            None => edited.content.push(block.clone()),
                            Some(edit) if edit.drop => {}
                            Some(edit) => {
                                let bytes = edit.bytes.as_deref().unwrap_or_default();
                                let original = stored
                                    .is_some()
                                    .then(|| stored_at(ordinal).and_then(|m| m.content.get(index)));
                                edited
                                    .content
                                    .push(edited_part(bytes, original, message.role)?);
                            }
                        }
                    }
                    out.push(edited);
                }
            }
            ViewRange::Replace { from, to, bytes } => {
                let values: Vec<Value> = serde_json::from_str(bytes)
                    .map_err(|_| AssemblyProblem::Rejected(ViewProblem::ReplacementNotArray))?;
                let carried: Option<Vec<Message>> = stored.map(|stored| {
                    transcript(stored)
                        .filter(|m| m.ordinal.is_some_and(|o| o >= *from && o < *to))
                        .cloned()
                        .collect()
                });
                let carried = carried
                    .as_deref()
                    .map(crate::compaction::CarriedMessages::new);
                for value in &values {
                    let message = match &carried {
                        Some(carried) => native_message(value, carried),
                        None => native_schema_message(value),
                    };
                    let mut message = message.ok_or_else(|| replacement_problem(value))?;
                    message.ordinal = None;
                    out.push(message);
                }
            }
            // A range kind this build does not know cannot reach here: views decode strictly
            // on `kind`, so such a view never decodes.
            _ => return Err(AssemblyProblem::Rejected(ViewProblem::Inverted)),
        }
    }
    let covered = view.covers_through.ordinal;
    out.extend(
        transcript(prompt)
            .filter(|m| m.ordinal.is_some_and(|o| o > covered))
            .cloned(),
    );
    Ok(out)
}

/// A replacement message broca refuses: bytes that are not a message in broca's schema are
/// rejected; a message in the schema that claims model authorship is a provider constraint.
fn replacement_problem(value: &Value) -> AssemblyProblem {
    match serde_json::from_value::<Message>(value.clone()) {
        Ok(message) if message.origin.is_some() || message.content.iter().any(is_signed) => {
            AssemblyProblem::ProviderConstraint
        }
        _ => AssemblyProblem::Rejected(ViewProblem::ReplacementNotArray),
    }
}

/// One edited part: `bytes` decoded as a content block of broca's schema. A block that only
/// the model may author (signed or encrypted reasoning, provider-signed text) is accepted only
/// as a byte-identical copy of the part it replaces.
/// `original` is the stored part the edit replaces, when the caller judges authorship
/// (`Some(None)` when there is no such stored part); `None` to skip that judgement.
fn edited_part(
    bytes: &str,
    original: Option<Option<&ContentBlock>>,
    role: Role,
) -> Result<ContentBlock, AssemblyProblem> {
    let value: Value = serde_json::from_str(bytes)
        .map_err(|_| AssemblyProblem::Rejected(ViewProblem::EditNotJson))?;
    // Reuse the whole-message schema check on a one-part message of the same role.
    let probe = serde_json::json!({"role": role, "content": [value]});
    let block: ContentBlock = match serde_json::from_value::<Message>(probe.clone()) {
        Ok(mut message) if super::native_fields(&probe) => message.content.remove(0),
        _ => return Err(AssemblyProblem::Rejected(ViewProblem::EditNotJson)),
    };
    if let Some(original) = original {
        if is_signed(&block) && original != Some(&block) {
            return Err(AssemblyProblem::ProviderConstraint);
        }
    }
    Ok(block)
}

/// Remove signed reasoning from every transcript message whose ordinal lies in one of
/// `strips`, and, when `copies`, from every message the provider wrote into the view (those
/// carry no ordinal): a copy of a signed message stands behind a changed prefix by
/// construction.
pub fn apply_strips(history: &mut [Message], strips: &[CompactionStrip], copies: bool) {
    for message in history {
        let stripped = match message.ordinal {
            None => copies,
            Some(ordinal) => strips
                .iter()
                .any(|strip| ordinal >= strip.from_ordinal && ordinal < strip.to_ordinal),
        };
        if stripped {
            message.content.retain(|block| !is_prefix_bound(block));
        }
    }
}

/// Whether the applied view went with the companion strip, which also covers the signed
/// reasoning of the messages the provider copied into it.
fn strips_copies(state: &CompactionState) -> bool {
    state
        .view
        .as_ref()
        .is_some_and(|applied| matches!(applied.choice, CompactionViewChoice::Companion { .. }))
}

/// Signed or encrypted reasoning: the blocks a model provider that binds reasoning to its
/// prefix rejects once the bytes before them change. The same blocks the renderer leaves out
/// of the prefix of a replacement the synchronous client applied.
pub fn is_prefix_bound(block: &ContentBlock) -> bool {
    matches!(
        block,
        ContentBlock::Reasoning {
            signature: Some(_),
            ..
        } | ContentBlock::RedactedReasoning { .. }
    )
}

/// The history the session renders now, before any newer view: the applied view, or else
/// Setup's initial CompactionMessage (or a replacement the synchronous client applied) in
/// place of its range, or else the transcript as written.
pub fn rendered_history(state: &CompactionState, prompt: &[Message]) -> Vec<Message> {
    if let Some(applied) = &state.view {
        if let Ok(mut history) = assemble(&applied.view, prompt) {
            apply_strips(&mut history, &state.strips, strips_copies(state));
            return history;
        }
    }
    match &state.applied {
        Some(applied) => applied
            .replacement
            .iter()
            .cloned()
            .map(|mut message| {
                message.ordinal = None;
                message
            })
            .chain(
                transcript(prompt)
                    .filter(|m| m.ordinal.is_some_and(|o| o >= applied.range.to))
                    .cloned(),
            )
            .collect(),
        None => transcript(prompt).cloned().collect(),
    }
}

/// What the runner does with a fetched view at a step boundary.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Record it (`CompactionViewApplied`) with this choice, and render it from this step on.
    Apply(CompactionViewChoice),
    /// Hold it and keep rendering the current view; report `view_deferred:<version>:<reason>`.
    Defer(&'static str),
    /// Never render it and keep the current view; report `view_refused:<version>:<reason>`.
    Refuse(&'static str),
    /// Its coverage is not a prefix of this transcript. broca never truncates a transcript,
    /// so this is a view of another lineage, which the provider leaves behind once it reads
    /// the new one. Dropped without a report: `view_discarded` is defined only beside a
    /// `revert_below`, and broca has no revert to report.
    Discard,
    /// Not newer than the view rendered now; ignored.
    Stale,
    /// Not the view asked for, malformed, or not in broca's message schema. Not recorded and
    /// not reported; the current view stays.
    Rejected(ViewProblem),
}

impl Decision {
    /// The event this decision reports on the next notice. An applied view is reported by
    /// `applied_view_version` itself, and its companion edit by the fold of its record.
    pub fn event(&self, view_version: u64) -> Option<Event> {
        match self {
            Self::Defer(reason) => Some(Event::ViewDeferred {
                view_version,
                reason: (*reason).into(),
            }),
            Self::Refuse(reason) => Some(Event::ViewRefused {
                view_version,
                reason: (*reason).into(),
            }),
            Self::Apply(_) | Self::Discard | Self::Stale | Self::Rejected(_) => None,
        }
    }
}

/// Decide what to do with `view`, fetched for `ready`, at a step boundary of the session's
/// lineage `lineage_id` (compaction-provider/v1 §7).
///
/// `prompt` is the prompt the next request is rendered from, exactly as [`render`] gets it:
/// ordinals stamped, runner tags and loop guidance already in its text. The decision and the
/// request must see the same bytes, or a view could be judged unchanged where the request
/// changes. `stored` is the same transcript as stored (what `session.read` returns, without
/// the folded tags and guidance), against which the view's own bytes are judged for who wrote
/// them.
///
/// First the role's fixed checks (`dispose_fetched`: asked for, well formed, announced digest,
/// newer, a prefix of this transcript, every edited part exists). Then broca's own: the view's
/// bytes are messages and parts of broca's schema, and the history it gives keeps every tool
/// call next to its result and every message in a role its place allows.
///
/// Last, signed reasoning, when the model provider binds it to its prefix (`prefix_bound`,
/// as Anthropic's thinking signatures are). A view whose history first differs from the one
/// rendered now at some message invalidates the signature of every signed reasoning block
/// kept after that point, and the renderer leaves out the signed reasoning of every message the
/// provider copied into a replace range. The model provider requires the reasoning of the open
/// tool turn (the last assistant message of the history, when it makes tool calls, whatever
/// user text follows its results); a view that would invalidate or leave out that reasoning is
/// deferred until a later boundary where the turn is over (`view_deferred:<v>:signed_reasoning`).
/// Any other invalidated reasoning is left out of every request from now on, by applying the
/// view with the companion strip (`view_companion:<v>:strip_signed_reasoning`) over the
/// ordinals from the first invalidated block to the end of the transcript. Messages written
/// after the view applies are produced against its prefix and keep their reasoning.
#[allow(clippy::too_many_arguments)]
pub fn decide(
    view: &CompactionView,
    ready: &ViewReady,
    state: &CompactionState,
    lineage_id: &str,
    prompt: &[Message],
    stored: &[Message],
    prefix_bound: bool,
) -> Decision {
    let entries: BTreeMap<u64, TranscriptMessage> = transcript(prompt)
        .filter_map(transcript_message)
        .map(|entry| (entry.ordinal, entry))
        .collect();
    let current = state.view.as_ref().map(|applied| applied.view.view_version);
    match dispose_fetched(view, ready, current, lineage_id, |ordinal| {
        entries.get(&ordinal).cloned()
    }) {
        ViewDisposition::Apply => {}
        ViewDisposition::Stale => return Decision::Stale,
        ViewDisposition::Rejected(problem) => return Decision::Rejected(problem),
        ViewDisposition::Discard => return Decision::Discard,
        ViewDisposition::Refuse(reason) => return Decision::Refuse(reason),
    }
    // Authorship is judged against the stored transcript a provider copies from. The bytes a
    // view gives are sent exactly as given (compaction-provider CONTRACT.md §6: "replaced bytes
    // are final ... the runner adds nothing inside replaced bytes"), so a verbatim copy of a
    // stored message is accepted, never refused, and goes out without the runner tags and loop
    // guidance the prompt shows on the original; the difference that makes is judged below like
    // any other change. Do not fold tags into the copy to make it match: that would put runner
    // bytes inside replaced bytes.
    let mut next = match assemble_judged(view, prompt, Some(stored)) {
        Ok(history) => history,
        Err(AssemblyProblem::Rejected(problem)) => return Decision::Rejected(problem),
        Err(AssemblyProblem::ProviderConstraint) => {
            return Decision::Refuse(refusal_reasons::PROVIDER_CONSTRAINT)
        }
    };
    match conversation(next.iter()) {
        Ok(()) => {}
        Err(Conversation::ToolPairSplit) => {
            return Decision::Refuse(refusal_reasons::TOOL_PAIR_SPLIT)
        }
        Err(Conversation::RoleOrder) => return Decision::Refuse(refusal_reasons::ROLE_ORDER),
    }
    apply_strips(&mut next, &state.strips, false);
    if !prefix_bound {
        return Decision::Apply(CompactionViewChoice::Applied);
    }
    let now = rendered_history(state, prompt);
    let first = first_difference(&now, &next);
    if open_turn(&next)
        .is_some_and(|open| open >= first && next[open].content.iter().any(is_prefix_bound))
    {
        return Decision::Defer(defer_reasons::SIGNED_REASONING);
    }
    let invalidated: Vec<u64> = next[first..]
        .iter()
        .filter(|message| message.content.iter().any(is_prefix_bound))
        .filter_map(|message| message.ordinal)
        .collect();
    let invalidated_copies = next[first..]
        .iter()
        .any(|message| message.ordinal.is_none() && message.content.iter().any(is_prefix_bound));
    if invalidated.is_empty() && !invalidated_copies {
        return Decision::Apply(CompactionViewChoice::Applied);
    }
    let to_ordinal = transcript(prompt)
        .filter_map(|message| message.ordinal)
        .max()
        .map_or(0, |last| last + 1);
    // With only copies invalidated the recorded range is empty; the companion choice itself
    // strips the copies' reasoning when the view renders.
    let from_ordinal = invalidated.iter().min().copied().unwrap_or(to_ordinal);
    Decision::Apply(CompactionViewChoice::Companion {
        edit: companion_edits::STRIP_SIGNED_REASONING.into(),
        strip: CompactionStrip {
            from_ordinal,
            to_ordinal,
        },
    })
}

/// The index of the first message at which two histories differ in their bytes, or the
/// shorter length when one is a prefix of the other.
fn first_difference(before: &[Message], after: &[Message]) -> usize {
    before
        .iter()
        .zip(after)
        .position(|(a, b)| a.role != b.role || a.content != b.content || a.origin != b.origin)
        .unwrap_or(before.len().min(after.len()))
}

/// The index in `history` of its open tool turn: its last assistant message, when that
/// message makes tool calls. The next request continues that turn after its tool results, so
/// the model provider requires its reasoning unchanged. A user message after the results (a
/// steer delivered at the boundary) does not close the turn: the provider takes it together
/// with the results. `None` when the last assistant message makes no call.
fn open_turn(history: &[Message]) -> Option<usize> {
    let last_assistant = history.iter().rposition(|m| m.role == Role::Assistant)?;
    history[last_assistant]
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall(_)))
        .then_some(last_assistant)
}

/// A recorded view that does not assemble over the prompt being rendered. It assembled when it
/// was applied, over the same prompt, so this means the prompt changed under it. Rendering
/// anything else (Setup's head, no strips) would send bytes no recorded decision covers, so it
/// is an error, never a fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewNotRenderable {
    pub view_version: u64,
    pub problem: AssemblyProblem,
}

impl std::fmt::Display for ViewNotRenderable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "applied compaction view {} no longer assembles over the prompt: {:?}",
            self.view_version, self.problem
        )
    }
}

/// The prompt a request sends under the session's applied view: the runner's own system
/// messages, then the view's history with every recorded strip applied. `None` when the
/// session has no applied view.
pub(crate) fn render(
    opts: &broca_protocol::CallOptions,
    state: &CompactionState,
) -> Option<Result<broca_protocol::CallOptions, ViewNotRenderable>> {
    let applied = state.view.as_ref()?;
    let mut history = match assemble(&applied.view, &opts.prompt) {
        Ok(history) => history,
        Err(problem) => {
            return Some(Err(ViewNotRenderable {
                view_version: applied.view.view_version,
                problem,
            }))
        }
    };
    apply_strips(&mut history, &state.strips, strips_copies(state));
    let mut prompt: Vec<_> = opts
        .prompt
        .iter()
        .filter(|m| m.role == Role::System)
        .cloned()
        .collect();
    let history_start = prompt.len();
    prompt.extend(history);
    let mut view = opts.with_prompt(prompt);
    // Episode decorations follow the transcript messages the view keeps; the provider's own
    // messages never inherit one.
    view.render_episodes = std::sync::Arc::new(broca_protocol::request::RenderHistory {
        compaction_prefix_len: state
            .applied
            .as_ref()
            .map(|applied| applied.thinking_prefix_len),
        episodes: opts
            .render_episodes
            .iter()
            .enumerate()
            .filter_map(|(index, episode)| {
                let end = opts
                    .render_episodes
                    .get(index + 1)
                    .map_or(usize::MAX, |next| next.prompt_start);
                let kept = view.prompt[history_start..].iter().position(|m| {
                    m.ordinal
                        .is_some_and(|o| o >= episode.prompt_start as u64 && o < end as u64)
                })?;
                let first_user = episode.first_user.and_then(|first| {
                    view.prompt[history_start..]
                        .iter()
                        .position(|m| m.ordinal == Some(first as u64))
                        .map(|i| history_start + i)
                });
                Some(broca_protocol::request::EpisodeRenderInput {
                    prompt_start: history_start + kept,
                    first_user,
                    frozen_config: episode.frozen_config.clone(),
                })
            })
            .collect(),
    });
    Some(Ok(view))
}

/// What a step boundary knows that a notice reports besides the folded state.
pub struct NoticeFacts<'a> {
    /// `llm-runner/v1`'s session reference.
    pub session_ref: String,
    /// The WAL lineage the transcript's ordinals count in.
    pub lineage_id: &'a str,
    /// The runner's estimate of its next request under its current view, in tokens.
    pub est_tokens: u64,
    pub model: &'a str,
    pub window: Option<u64>,
    /// The last completed model step's measured usage.
    pub usage: Option<Usage>,
    /// Events decided at this boundary (a deferred or refused view).
    pub events: Vec<Event>,
    /// The lowest sequence number this notice may take. When retention deleted earlier lineages
    /// of this session, their tombstones record the number their next notice would have had,
    /// and this lineage's notices continue from the highest of those.
    pub seq_floor: u64,
}

/// The advance notice a step boundary records (compaction-provider/v1 §5), or `None` when
/// there is nothing to announce: no transcript yet, or the same lineage, newest message,
/// applied view and model as the newest recorded notice and no events.
///
/// broca never forks a lineage, so no notice carries `descends_from`. A session's new
/// lineage (after retention deleted the old one) starts from an empty compaction state, so
/// its first notice names neither a parent nor an applied view. Its sequence numbers continue
/// from the deleted lineages' (`NoticeFacts::seq_floor`), because the provider and the bus
/// track notices per session.
pub fn next_notice(
    state: &CompactionState,
    prompt: &[Message],
    facts: NoticeFacts<'_>,
) -> Option<AdvanceNotice> {
    let through = transcript(prompt).last()?;
    let through_ordinal = through.ordinal?;
    let through_digest = content_digest(&serde_json::to_value(through).expect("native JSON"));
    let applied = state.applied_view_version(facts.lineage_id);
    let mut events: Vec<Event> = state
        .unannounced
        .iter()
        .filter_map(|token| Event::parse(token).ok())
        .collect();
    let announced = state.announced.as_ref();
    if announced
        .and_then(|a| a.model.as_deref())
        .is_some_and(|model| model != facts.model)
    {
        events.push(Event::ModelChange);
    }
    events.extend(facts.events);
    let unchanged = announced.is_some_and(|a| {
        a.lineage_id.as_deref() == Some(facts.lineage_id)
            && a.through_ordinal == Some(through_ordinal)
            && a.applied_view_version == applied
    });
    if unchanged && events.is_empty() {
        return None;
    }
    let mut notice = AdvanceNotice::new(
        facts.session_ref,
        (state.notice_seq + 1).max(facts.seq_floor),
        facts.lineage_id,
        through_ordinal,
        through_digest,
        facts.est_tokens,
        facts.model,
    )
    .with_events(events);
    if let Some(window) = facts.window {
        notice = notice.with_window(window);
    }
    if let Some(usage) = facts.usage.and_then(last_usage) {
        notice = notice.with_last_usage(usage);
    }
    if let Some(version) = applied {
        notice = notice.with_applied_view_version(version);
    }
    Some(notice)
}

/// `last_usage` needs all four measured counts; a response that did not report one is not
/// announced rather than guessed.
fn last_usage(usage: Usage) -> Option<LastUsage> {
    Some(LastUsage::new(
        usage.input_tokens?,
        usage.cached_input_tokens?,
        usage.cache_write_tokens?,
        usage.output_tokens?,
    ))
}

/// Setup's ceiling policy, as frozen in the session's Setup record.
pub fn ceiling_policy(state: &CompactionState) -> Option<CeilingPolicy> {
    serde_json::from_str(state.ceiling.as_ref()?.as_json_text()).ok()
}

/// What the runner does with a request estimated at `size` tokens before it sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeforeSend {
    Send,
    /// The request reached the ceiling: ride the provider for a view and wait for one.
    Pause,
    /// The request cannot be sent: refuse the run `context_ceiling` with this detail.
    Refuse(cortexkit_role_llm_runner::errors::ContextCeilingDetail),
}

/// The check before a request goes out (compaction-provider/v1 §8). A request over the
/// window is never sent, with or without a ceiling policy: it is refused `over_window`. A
/// request that reached the policy's ceiling pauses for a view. An unknown window cannot be
/// compared and is sent.
pub fn before_send(policy: Option<&CeilingPolicy>, size: u64, window: Option<u64>) -> BeforeSend {
    let Some(window) = window else {
        return BeforeSend::Send;
    };
    if size > window {
        return BeforeSend::Refuse(context_ceiling(
            window,
            size,
            crate::compaction_provider::ceiling::context_ceiling_causes::OVER_WINDOW,
        ));
    }
    if policy.is_some_and(|policy| policy.reached(size, window)) {
        return BeforeSend::Pause;
    }
    BeforeSend::Send
}

/// The decision at the end of a ceiling wait, as the policy states it; `Err` is the
/// `context_ceiling` refusal's detail.
pub fn after_wait(
    policy: &CeilingPolicy,
    size: u64,
    window: u64,
    overflowed: bool,
    end: WaitEnd,
) -> Result<(), cortexkit_role_llm_runner::errors::ContextCeilingDetail> {
    match policy.decide(size, window, overflowed, end) {
        CeilingDecision::Send => Ok(()),
        CeilingDecision::Refuse { cause } => Err(context_ceiling(window, size, cause)),
    }
}

fn context_ceiling(
    window: u64,
    size: u64,
    cause: &str,
) -> cortexkit_role_llm_runner::errors::ContextCeilingDetail {
    cortexkit_role_llm_runner::errors::ContextCeilingDetail::new(window, size, cause)
}

/// The context limit a model provider's refusal states, when its text states one:
/// Anthropic's `prompt is too long: N tokens > M maximum` and OpenAI's `maximum context
/// length is M tokens`. Anything else gives `None`; the limit is never guessed or taken from
/// the model catalog.
pub fn reported_limit_tokens(message: &str) -> Option<u64> {
    fn leading_number(text: &str) -> Option<u64> {
        let digits: String = text
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    }
    if let Some(rest) = message
        .find("prompt is too long:")
        .map(|at| &message[at + "prompt is too long:".len()..])
    {
        // `N tokens > M maximum`: the limit is the number after `>`.
        let limit = rest.split_once('>').map(|(_, after)| after)?;
        let number = leading_number(limit)?;
        return limit
            .trim_start()
            .trim_start_matches(|c: char| c.is_ascii_digit())
            .trim_start()
            .starts_with("maximum")
            .then_some(number);
    }
    let rest = message
        .find("maximum context length is")
        .map(|at| &message[at + "maximum context length is".len()..])?;
    let number = leading_number(rest)?;
    rest.trim_start()
        .trim_start_matches(|c: char| c.is_ascii_digit())
        .trim_start()
        .starts_with("tokens")
        .then_some(number)
}

/// The ride at the ceiling (compaction-provider/v1 §8). `overflow_error` is the model
/// provider's refusal text when the pause follows one: the ride then carries `overflow`, with
/// `reported_limit_tokens` only when that text states the limit. Without it the ceiling came
/// from broca's own count and the ride carries no `overflow`.
pub fn ceiling_ride(
    session_ref: impl Into<String>,
    deadline_ms: u64,
    overflow_error: Option<&str>,
) -> RideRequest {
    let ride = RideRequest::new(session_ref, RideCause::Ceiling, deadline_ms);
    match overflow_error {
        None => ride,
        Some(text) => ride.with_overflow(match reported_limit_tokens(text) {
            Some(limit) => Overflow::new().with_reported_limit_tokens(limit),
            None => Overflow::new(),
        }),
    }
}

/// Why the request at this boundary rewrites its prefix anyway, so that a ride costs the
/// session nothing it would not pay already (compaction-provider/v1 §8): today only a model
/// change, judged against the model the newest notice announced. `None` before the first
/// notice, which has nothing to compare with.
///
/// broca names no other cause. It places cache markers without knowing the lifetime the model
/// provider gives them, so it never claims `ttl_expired`. It does not detect a change of the
/// system text or tool surface between requests, so it never claims `surface_change` either.
pub fn rewrite_ride_cause(state: &CompactionState, model: &str) -> Option<RideCause> {
    state
        .announced
        .as_ref()?
        .model
        .as_deref()
        .filter(|announced| *announced != model)
        .map(|_| RideCause::ModelChange)
}

#[cfg(test)]
#[path = "view_tests.rs"]
mod tests;
