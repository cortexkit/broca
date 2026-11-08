//! A session's move onto the current compaction contract (enrollment), which a plan refresh that
//! adds a compaction provider makes at the first run that freezes it.
//!
//! The session keeps its conversation, its records and its message ids. The enrolling run
//! decides once, for each message placed before it, what `session.entries` serves about it on
//! the contract: its part number base in the projection's numbering, its time, who delivered it
//! where the records show a module did, and whether it is a global system message, which the
//! run's own system text takes the place of. The decisions are stored (`Session::put_enrollment`)
//! and named by a `CompactionEnrolled` record in the run's start batch, after its input; from
//! then on every reader of the session's entries sees them through the session.
//!
//! The rules a refresh is checked against before it adds a compaction provider live here too, so
//! the refresh and the run that applies it decide alike.

use std::collections::HashMap;

use broca_core::compaction_v2::metadata::numbered_parts;
use broca_core::compaction_v2::systems::is_global;
use broca_protocol::content::{Message, Role};
use broca_protocol::request::EpisodeRenderInput;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::record::{
    CompactionEnrolled, EnrolledEntries, EnrolledEntry, EnrollmentOverlayRef, Notice,
};
use broca_session_store::session::EnrollmentOverlay;

use crate::run::LoopError;
use crate::writer::Writer;

/// The highest part number the protocol can carry.
const MAX_TAG: u64 = 9_007_199_254_740_991;

/// Why a session can't enroll in the current compaction contract. Each names itself on the wire
/// by [`Refusal::reason`], which stays stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// No context limit resolves for the plan's model. A run on the contract can't run
    /// without one, and the compaction provider can't be removed again, so the session would
    /// be stuck.
    NoContextLimit,
    /// The plan has no system text, while the session's earlier runs carried global system
    /// messages: the enrollment would drop the session's system prompt.
    NoSystemText,
    /// History was cut or sealed out of the window, or a window change is pending, so the
    /// enrollment can't decide about every earlier message.
    WindowNotWhole,
}

impl Refusal {
    /// The stable name a refusal's reply carries.
    pub fn reason(self) -> &'static str {
        match self {
            Refusal::NoContextLimit => "no_context_limit",
            Refusal::NoSystemText => "no_system_text",
            Refusal::WindowNotWhole => "window_not_whole",
        }
    }

    /// The refusal in words.
    pub fn message(self) -> &'static str {
        match self {
            Refusal::NoContextLimit => {
                "a compaction provider can't be added: no context limit resolves for the plan's model"
            }
            Refusal::NoSystemText => {
                "a compaction provider can't be added by a plan without system text: the session's earlier runs carried a system prompt"
            }
            Refusal::WindowNotWhole => {
                "a compaction provider can't be added: the session's history was cut or sealed, or a window change is pending"
            }
        }
    }
}

/// Whether the conversation holds every message the session placed, with no cut marked and no
/// window change pending: the state an enrollment needs, since it decides about each earlier
/// message.
pub fn window_whole(conversation: &Conversation) -> bool {
    conversation.window_start() == 1
        && conversation.marker.is_none()
        && !conversation.window_pending
}

/// Whether any message of the window is a global system message (`systems::is_global`): one
/// the session's runs repeated into their input, which an enrollment suppresses.
pub fn carries_global_system(conversation: &Conversation) -> bool {
    conversation.messages.iter().any(is_global)
}

/// The session-state rules an enrollment by a plan is checked against, in this order: the window
/// must be whole (`window_whole`, see [`window_whole`]), and a plan without system text
/// (`plan_has_system_text` false) may not replace the global system messages a session's
/// earlier runs carried (`carries_global_system`, see [`carries_global_system`]). The context
/// limit is resolved and checked by the caller, which knows the plan's model.
pub fn check(
    window_whole: bool,
    carries_global_system: bool,
    plan_has_system_text: bool,
) -> Result<(), Refusal> {
    if !window_whole {
        return Err(Refusal::WindowNotWhole);
    }
    if !plan_has_system_text && carries_global_system {
        return Err(Refusal::NoSystemText);
    }
    Ok(())
}

/// What an enrolling run decided about the messages placed before it.
#[derive(Debug, Clone, PartialEq)]
pub struct Decided {
    pub entries: EnrolledEntries,
    /// The newest message placed before the run.
    pub through_ordinal: u64,
    /// The highest part number any earlier request showed the model.
    pub tag_floor: u64,
    /// The last part number the earlier messages took.
    pub tag_high: u64,
}

impl Decided {
    /// The record naming these decisions, once they are stored at `overlay`.
    pub fn record(&self, overlay: EnrollmentOverlayRef) -> CompactionEnrolled {
        CompactionEnrolled {
            through_ordinal: self.through_ordinal,
            tag_floor: self.tag_floor,
            tag_high: self.tag_high,
            overlay,
        }
    }
}

/// The refusal a run that would enroll answers with, before anything is written.
pub fn refused(refusal: Refusal) -> LoopError {
    LoopError::Refused(broca_core::RunError::Config(format!(
        "plan.compaction_item: {}: {}",
        refusal.reason(),
        refusal.message()
    )))
}

/// Decides, for each message the session placed, what `session.entries` serves about it once the
/// session is on the current compaction contract. `tag_floor` is the highest part number any
/// earlier request showed the model.
///
/// - Part numbers follow the projection's numbering: each message's parts are numbered in order
///   from `tag_floor + 1`, a tool-results message taking none, as a session on the contract
///   numbers them as it places them.
/// - A global system message is suppressed: the run's own system text takes its place, so it is
///   neither served nor rendered, and takes no numbers.
/// - A message takes its own time, else the newest earlier served message's, else 0, as the
///   contract's entries take them.
/// - A notice is set only where the records show a module other than the owner delivered the
///   message, by the rule a run on the contract records it with: a budget reminder is broca's,
///   and a run's input is its start's sender when a module sent it. Anything else has none,
///   which compaction reads as the person's own message.
pub async fn decide<W: Writer>(writer: &W, tag_floor: u64) -> Result<Decided, LoopError> {
    let conversation = writer.conversation();
    if !window_whole(conversation) {
        return Err(refused(Refusal::WindowNotWhole));
    }
    if conversation.open_tool_turn().is_some() {
        return Err(LoopError::Unsupported(
            "enrolling a session whose newest tool turn waits for results",
        ));
    }
    if conversation.messages.len() as u64 != conversation.placed
        || conversation.entries.len() != conversation.messages.len()
    {
        return Err(LoopError::Parts(format!(
            "the window holds {} messages and {} entries, but the session placed {}",
            conversation.messages.len(),
            conversation.entries.len(),
            conversation.placed
        )));
    }
    let starts: HashMap<u64, &str> = conversation
        .episodes
        .iter()
        .map(|episode| (episode.start, episode.run_id.as_str()))
        .collect();
    // Each run's sender, read once from its start.
    let mut senders: HashMap<String, Option<Notice>> = HashMap::new();
    // The run whose input the walk is in: from the run's first message up to the first message
    // a model step or the runner wrote.
    let mut input_of: Option<&str> = None;
    let mut high = tag_floor;
    let mut time: Option<u64> = None;
    let mut entries = Vec::with_capacity(conversation.messages.len());
    for (index, (message, state)) in conversation
        .messages
        .iter()
        .zip(&conversation.entries)
        .enumerate()
    {
        let ordinal = index as u64 + 1;
        let facts = state.facts.as_ref();
        if let Some(run) = starts.get(&ordinal) {
            input_of = Some(*run);
        }
        let native = facts.is_some_and(|facts| facts.native);
        if native {
            input_of = None;
        }
        let own_time = facts
            .and_then(|facts| facts.written_at_ms)
            .filter(|at| *at != 0);
        if is_global(message) {
            entries.push(EnrolledEntry {
                ordinal,
                tag_base: high,
                written_at_ms: own_time.unwrap_or(0),
                suppressed: true,
                notice: None,
            });
            continue;
        }
        let written_at_ms = own_time.or(time).unwrap_or(0);
        time = Some(written_at_ms);
        let notice = match (message.role == Role::User, native, facts) {
            (false, _, _) | (true, _, None) => None,
            (true, true, Some(_)) => Some(Notice {
                sender: "broca".into(),
                template: None,
            }),
            (true, false, Some(facts)) => match (input_of, facts.run_id.as_deref()) {
                (Some(run), Some(own)) if run == own && facts.delivery_mode.is_none() => {
                    run_sender(writer, &mut senders, run).await?
                }
                _ => None,
            },
        };
        entries.push(EnrolledEntry {
            ordinal,
            tag_base: high,
            written_at_ms,
            suppressed: false,
            notice,
        });
        high = high
            .checked_add(numbered_parts(message))
            .filter(|high| *high <= MAX_TAG)
            .ok_or(LoopError::Unsupported(
                "part numbers past the numbering limit",
            ))?;
    }
    Ok(Decided {
        entries: EnrolledEntries { entries },
        through_ordinal: conversation.placed,
        tag_floor,
        tag_high: high,
    })
}

/// Who sent run `run`'s input, by the rule a run on the contract records it with
/// (`crate::entries::input_sender`), read from the run's start once.
async fn run_sender<W: Writer>(
    writer: &W,
    senders: &mut HashMap<String, Option<Notice>>,
    run: &str,
) -> Result<Option<Notice>, LoopError> {
    if let Some(sender) = senders.get(run) {
        return Ok(sender.clone());
    }
    let start = writer.run_start(run).await?.ok_or_else(|| {
        LoopError::Parts(format!(
            "the window names run {run} but the session holds no start for it"
        ))
    })?;
    let sender = crate::entries::input_sender(&start);
    senders.insert(run.to_owned(), sender.clone());
    Ok(sender)
}

/// `prompt`, each of whose messages is stamped with its 0-based position in the session,
/// without the global system messages an enrollment suppressed, as a session on the contract
/// never holds them in its transcript. Done before the prompt is lined up with the entries,
/// which leave them out alike.
pub fn without_suppressed(prompt: &mut Vec<Message>, overlay: &EnrollmentOverlay) {
    prompt.retain(|message| {
        !message
            .ordinal
            .is_some_and(|position| overlay.suppressed(position + 1))
    });
}

/// `episodes` with each run's place in the prompt moved past the suppressed global system
/// messages it starts with, so it names the run's first message the prompt still holds, as a
/// run on the contract starts at its first message after its system text. A run's first user
/// message is never suppressed.
pub fn translate_episodes(episodes: &mut [EpisodeRenderInput], overlay: &EnrollmentOverlay) {
    for episode in episodes {
        while overlay.suppressed(episode.prompt_start as u64 + 1) {
            episode.prompt_start += 1;
        }
    }
}

/// The prompt size the newest step before the session enrolled reported, from the operational
/// fold's newest step usage, as a step's entry reports it (`metadata::step_usage`). It stands
/// for the session's fill until a step on the contract reports its own, since the messages
/// placed before the enrollment carry no usage.
pub fn last_prompt_tokens(
    operations: &broca_session_store::fold::operations::Operations,
) -> Option<u64> {
    let usage = operations.estimate.as_ref()?.step_usage?;
    let usage = broca_core::compaction_v2::metadata::step_usage(usage)?;
    // The contract crate's usage can't be read field by field outside it; its JSON form is
    // its contract.
    serde_json::to_value(usage)
        .ok()?
        .get("prompt_tokens")?
        .as_u64()
}

/// Whether `run_id` is the run that moved the session onto the current compaction contract.
pub fn is_enrolling_run(
    operations: &broca_session_store::fold::operations::Operations,
    run_id: &str,
) -> bool {
    operations
        .enrolled
        .as_ref()
        .is_some_and(|enrolled| enrolled.run_id == run_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_protocol::request::EpisodeRenderInput;
    use broca_session_store::fold::conversation::Conversation;
    use broca_session_store::record::{EnrolledEntries, EnrolledEntry};
    use broca_session_store::session::EnrollmentOverlay;

    use super::*;

    fn message(role: Role, text: &str, turn_system: bool) -> Message {
        let mut message = Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
        message.turn_system = turn_system;
        message
    }

    /// The overlay of a session whose first and fourth messages were suppressed global system
    /// messages.
    fn overlay() -> EnrollmentOverlay {
        let entries = (1..=5)
            .map(|ordinal| EnrolledEntry {
                ordinal,
                tag_base: 0,
                written_at_ms: 0,
                suppressed: ordinal == 1 || ordinal == 4,
                notice: None,
            })
            .collect();
        EnrollmentOverlay::new(Arc::new(EnrolledEntries { entries })).unwrap()
    }

    /// The rules a refresh adding a compaction provider is checked against, in order: a window
    /// that isn't whole first, then a plan without system text over a session whose runs carried
    /// global system messages. A turn-scoped note is no global system message.
    #[test]
    fn an_enrollment_needs_a_whole_window_and_system_text_for_a_system_prompt() {
        let mut conversation = Conversation::new(false);
        assert!(window_whole(&conversation));
        conversation.messages = vec![
            message(Role::System, "a note for this turn", true),
            message(Role::User, "hi", false),
        ];
        conversation.placed = 2;
        assert!(!carries_global_system(&conversation));
        assert_eq!(check(true, false, false), Ok(()));
        conversation
            .messages
            .insert(0, message(Role::System, "you are terse", false));
        conversation.placed = 3;
        assert!(carries_global_system(&conversation));
        assert_eq!(check(true, true, false), Err(Refusal::NoSystemText));
        assert_eq!(check(true, true, true), Ok(()));
        conversation.window_pending = true;
        assert!(!window_whole(&conversation));
        assert_eq!(check(false, true, false), Err(Refusal::WindowNotWhole));
        assert_eq!(Refusal::NoContextLimit.reason(), "no_context_limit");
        assert_eq!(Refusal::NoSystemText.reason(), "no_system_text");
        assert_eq!(Refusal::WindowNotWhole.reason(), "window_not_whole");
    }

    /// Suppressed messages leave the prompt by their stamped positions, and a run that starts
    /// with one starts at its first message the prompt still holds.
    #[test]
    fn suppressed_messages_leave_the_prompt_and_runs_start_after_them() {
        let overlay = overlay();
        let mut prompt: Vec<Message> = ["sys", "hi", "hello", "sys again", "next"]
            .iter()
            .enumerate()
            .map(|(position, text)| {
                let mut message = message(Role::User, text, false);
                message.ordinal = Some(position as u64);
                message
            })
            .collect();
        without_suppressed(&mut prompt, &overlay);
        let kept: Vec<Option<u64>> = prompt.iter().map(|message| message.ordinal).collect();
        assert_eq!(kept, vec![Some(1), Some(2), Some(4)]);
        let mut episodes = vec![
            EpisodeRenderInput {
                prompt_start: 0,
                first_user: Some(1),
                frozen_config: serde_json::Value::Null,
            },
            EpisodeRenderInput {
                prompt_start: 3,
                first_user: Some(4),
                frozen_config: serde_json::Value::Null,
            },
        ];
        translate_episodes(&mut episodes, &overlay);
        assert_eq!(
            episodes
                .iter()
                .map(|episode| (episode.prompt_start, episode.first_user))
                .collect::<Vec<_>>(),
            vec![(1, Some(1)), (4, Some(4))]
        );
    }
}
