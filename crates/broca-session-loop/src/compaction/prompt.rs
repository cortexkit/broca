//! A compacted run's stored prompt, kept between its steps.
//!
//! A request on the current compaction contract renders from the compaction provider's view,
//! which reads the stored prompt message by message beside the transcript's entries
//! (`compaction::prepare`). The stored prompt contains the window's messages: each with the
//! repetition warnings its run's steps earn, its position in the session and its part numbers,
//! with an empty placeholder in place of each global system message an enrollment suppressed
//! ([`contract_prompt`]). Deriving it copies every window message, so a compacted run keeps it
//! and at each step takes in only the messages placed since the last one. A placed message
//! changes only when delivered text is joined into it, which the conversation fold records
//! (`Conversation::changes`); that, a new window start, a new enrollment or a new list of the
//! window's runs derives it again from the window's first message. Debug builds compare it with
//! the prompt derived whole at every step.

use std::sync::Arc;

use broca_core::repetition::RepetitionGuidance;
use broca_core::tags::TagNumbering;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{EpisodeRenderInput, RenderHistory};
use broca_protocol::tags::ComputedRules;
use broca_records::record::ComputedTagRules;
use broca_session_store::fold::conversation::{ChangeMark, Conversation};
use broca_session_store::session::EnrollmentOverlay;

use crate::run::LoopError;
use crate::writer::Writer;

/// A run's stored prompt between its steps (see the module's notes), and the episode list its
/// requests carry. A new one keeps nothing, and its first step takes in the whole window.
#[derive(Default)]
pub struct KeptPrompt(Option<Kept>, Option<KeptEpisodes>);

/// The episode list a compacted run's requests carry: the run's list, moved past the global
/// system messages an enrollment suppressed, read as raw ordinals, as rendering a view reads it
/// (`render::options`). Kept while the run's list and the enrollment stay the same, so each
/// request shares it rather than copying it.
struct KeptEpisodes {
    of: Arc<RenderHistory>,
    enrolled_through: Option<u64>,
    episodes: Arc<RenderHistory>,
}

/// What the kept prompt was derived under: a change to any of it derives it again.
struct Key {
    session: String,
    window_start: u64,
    /// The newest message placed before the session enrolled, for a session that enrolled.
    enrolled_through: Option<u64>,
    /// The window's runs, by the list the run holds.
    episodes: Arc<RenderHistory>,
    after_input: bool,
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.session == other.session
            && self.after_input == other.after_input
            && self.window_start == other.window_start
            && self.enrolled_through == other.enrolled_through
            && Arc::ptr_eq(&self.episodes, &other.episodes)
    }
}

struct Kept {
    key: Key,
    /// Where the conversation's record of changes stood when the last messages were taken in.
    seen: ChangeMark,
    /// How many window messages were taken in.
    taken: usize,
    /// The sealed messages a transcript put in front (`front` of them), then the window's.
    prompt: Vec<Message>,
    front: usize,
    /// The sealed messages in front, as the transcript gave them.
    front_of: Option<crate::compaction::SealedMessages>,
    /// The part numbers given so far, which the next messages continue.
    numbering: TagNumbering,
    /// The window run the newest message taken in belongs to, by its place in the list, and its
    /// repetition counters.
    run: Option<usize>,
    guidance: RunGuidance,
}

/// A run's repetition counters while its messages are taken in.
enum RunGuidance {
    /// Counting starts at the run's first assistant message.
    Waiting,
    Counting(RepetitionGuidance),
}

/// The numbering a request on the current compaction contract gives its prompt: the window's
/// messages in order, from no number given.
fn contract_numbering() -> TagNumbering {
    TagNumbering::for_run(
        &[],
        Some(ComputedRules {
            render: false,
            clean: false,
        }),
        Some(ComputedTagRules { checkpoint: 0 }),
    )
}

fn overflow(_: broca_protocol::tags::NumberingOverflow) -> LoopError {
    LoopError::Unsupported("part numbers past the numbering limit")
}

/// The stored prompt of a request on the current compaction contract, derived whole from the
/// window: its messages with each run's repetition warnings, each stamped with its 0-based
/// position in the session, with a placeholder in place of each global system message
/// `enrollment` suppressed (see [`suppressed_placeholder`]), and numbered in order.
pub(crate) fn contract_prompt(
    conversation: &Conversation,
    episodes: &[EpisodeRenderInput],
    enrollment: Option<&EnrollmentOverlay>,
    after_input: bool,
    session: &str,
) -> Result<Vec<Message>, LoopError> {
    let mut prompt: Vec<Message> = conversation
        .held_messages()
        .map_err(crate::compaction::not_held)?
        .iter()
        .map(|message| Message::clone(message))
        .collect();
    let first = conversation.window_start() - 1;
    crate::repetition::repetition_guidance(
        &mut prompt,
        episodes,
        first,
        conversation.window_repetition.as_ref(),
        after_input.then_some((conversation, session.to_owned())),
    )?;
    // Each message keeps its place in the session, which the window starts after once history
    // is sealed: the window's first message has the 0-based position `window_start - 1`.
    for (index, message) in prompt.iter_mut().enumerate() {
        let position = first + index as u64;
        if enrollment.is_some_and(|overlay| overlay.suppressed(position + 1)) {
            *message = suppressed_placeholder(position);
        } else {
            message.ordinal = Some(position);
        }
    }
    contract_numbering()
        .number_new(&mut prompt)
        .map_err(overflow)?;
    Ok(prompt)
}

/// What the stored prompt holds at 0-based session position `position` in place of a global
/// system message the session's enrollment suppressed: a system message with nothing in it. A
/// session on the contract never holds such a message, since the run's system text takes its
/// place above the conversation, but the entries serve it at its own ordinal with no parts, and a
/// view's render plan renders an entry with no parts as nothing. So the stored prompt keeps one
/// message per entry, as rendering lines them up, and nothing of the message is sent.
fn suppressed_placeholder(position: u64) -> Message {
    let mut placeholder = Message::new(Role::System, Vec::new());
    placeholder.ordinal = Some(position);
    placeholder
}

#[cfg(test)]
thread_local! {
    /// How many window messages this thread copied into a stored prompt on the contract.
    pub(crate) static TAKEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Counts `count` window messages copied into a stored prompt, for the tests that check how
/// many a step copies.
#[cfg_attr(not(test), allow(unused_variables))]
pub(crate) fn taken(count: usize) {
    #[cfg(test)]
    TAKEN.with(|taken| taken.set(taken.get() + count));
}

impl KeptPrompt {
    /// The episode list a request built from `episodes` under `enrollment` carries (see
    /// [`KeptEpisodes`]).
    pub(crate) fn request_episodes(
        &mut self,
        episodes: &Arc<RenderHistory>,
        enrollment: Option<&EnrollmentOverlay>,
    ) -> Arc<RenderHistory> {
        let enrolled_through = enrollment.map(EnrollmentOverlay::through_ordinal);
        let kept = self.1.as_ref().filter(|kept| {
            Arc::ptr_eq(&kept.of, episodes) && kept.enrolled_through == enrolled_through
        });
        if let Some(kept) = kept {
            return Arc::clone(&kept.episodes);
        }
        let mut list = RenderHistory::from(episodes.episodes.clone());
        if let Some(enrollment) = enrollment {
            crate::enroll::translate_episodes(&mut list.episodes, enrollment);
        }
        list.raw_ordinals = true;
        // The floor is an ordinal, which leaving suppressed messages out doesn't move.
        list.signed_floor = episodes.signed_floor;
        let list = Arc::new(list);
        self.1 = Some(KeptEpisodes {
            of: Arc::clone(episodes),
            enrolled_through,
            episodes: Arc::clone(&list),
        });
        list
    }

    /// Whether bringing the kept prompt up to `writer`'s window ([`KeptPrompt::update`]) would
    /// derive it again from the whole window rather than take in only the newest messages. A
    /// caller whose window has released content reads it back first then (see
    /// [`crate::writer::whole_conversation`]): deriving reads every message's content.
    pub(crate) fn rederives<W: Writer>(
        &self,
        writer: &W,
        episodes: &Arc<RenderHistory>,
        enrollment: Option<&EnrollmentOverlay>,
        after_input: bool,
    ) -> bool {
        let key = Self::key(
            writer,
            writer.conversation(),
            episodes,
            enrollment,
            after_input,
        );
        !self.keeps(&key, writer.conversation())
    }

    fn key<W: Writer>(
        writer: &W,
        conversation: &Conversation,
        episodes: &Arc<RenderHistory>,
        enrollment: Option<&EnrollmentOverlay>,
        after_input: bool,
    ) -> Key {
        Key {
            session: writer.session_id(),
            window_start: conversation.window_start(),
            enrolled_through: enrollment.map(EnrollmentOverlay::through_ordinal),
            episodes: Arc::clone(episodes),
            after_input,
        }
    }

    fn keeps(&self, key: &Key, conversation: &Conversation) -> bool {
        self.0.as_ref().is_some_and(|kept| {
            kept.key == *key
                && kept.taken <= conversation.messages.len()
                && conversation.changes.messages_since(kept.seen).is_none()
        })
    }

    /// Brings the kept prompt up to `writer`'s window, read under `enrollment` with the window's
    /// runs `episodes`: takes in the messages placed since the last step, or derives it again
    /// (see the module's notes). `whole` is the window with every message held, which a caller
    /// reads back when [`KeptPrompt::rederives`] says so and some content was released; the
    /// writer's own window serves otherwise.
    pub(crate) fn update<W: Writer>(
        &mut self,
        writer: &W,
        episodes: &Arc<RenderHistory>,
        enrollment: Option<&EnrollmentOverlay>,
        after_input: bool,
        whole: Option<&Conversation>,
    ) -> Result<(), LoopError> {
        let conversation = whole.unwrap_or_else(|| writer.conversation());
        let key = Self::key(writer, conversation, episodes, enrollment, after_input);
        let keeps = self.keeps(&key, conversation);
        if !keeps {
            self.0 = Some(Kept {
                key,
                seen: ChangeMark::default(),
                taken: 0,
                prompt: Vec::new(),
                front: 0,
                front_of: None,
                numbering: contract_numbering(),
                run: None,
                guidance: RunGuidance::Waiting,
            });
        }
        let kept = self.0.as_mut().expect("set above");
        kept.seen = conversation.changes.mark();
        let first = conversation.window_start() - 1;
        let from = kept.taken;
        let corrected_starts = after_input.then(|| {
            episodes
                .iter()
                .map(|episode| {
                    crate::repetition::first_answer(
                        conversation,
                        episode.prompt_start,
                        &kept.key.session,
                    )
                })
                .collect::<Vec<_>>()
        });
        let start = conversation.window_start();
        for (index, placed) in conversation.messages.iter().enumerate().skip(from) {
            let placed = placed
                .held()
                .ok_or(broca_session_store::fold::conversation::NotHeld {
                    ordinal: start + index as u64,
                });
            let mut message = Message::clone(placed.map_err(crate::compaction::not_held)?);
            kept.guide(
                index,
                &mut message,
                episodes,
                first,
                conversation.window_repetition.as_ref(),
                corrected_starts.as_deref(),
            )?;
            let position = first + index as u64;
            message.ordinal = Some(position);
            kept.taken = index + 1;
            if enrollment.is_some_and(|overlay| overlay.suppressed(position + 1)) {
                message = suppressed_placeholder(position);
            }
            kept.prompt.push(message);
        }
        taken(kept.taken - from);
        let front = kept.front;
        kept.numbering
            .number_new(&mut kept.prompt[front..])
            .map_err(overflow)?;
        // Once content is released the kept prompt holds released blocks and the window can't
        // be derived without reading back, so the check compares only while all is held.
        #[cfg(debug_assertions)]
        if conversation.released_ordinals().is_empty()
            && !kept.prompt[front..]
                .iter()
                .any(|message| message.content.iter().any(ContentBlock::holds_released))
        {
            assert!(
                kept.prompt[front..]
                    == contract_prompt(
                        conversation,
                        episodes,
                        enrollment,
                        after_input,
                        &writer.session_id()
                    )?[..],
                "the kept stored prompt differs from the window's derived whole"
            );
        }
        Ok(())
    }

    /// Releases from the kept prompt the blocks `blocks` names, by their position in the stored
    /// prompt (the sealed messages in front included) and block index: each becomes its released
    /// form (`ContentBlock::released`). Only a block equal to the window message's own block is
    /// released, so reading the window message back restores it exactly; a block the kept prompt
    /// changed (a repetition warning added to a result) stays held. `window_holds` says whether
    /// the window message at an ordinal holds exactly this block at this index, or released
    /// exactly this block earlier. Returns the released blocks by window ordinal, for the writer
    /// to release in the window too.
    pub(crate) fn release(
        &mut self,
        blocks: &[(usize, Vec<usize>)],
        window_start: u64,
        mut window_holds: impl FnMut(u64, usize, &ContentBlock) -> bool,
    ) -> Vec<(u64, Vec<usize>)> {
        let Some(kept) = self.0.as_mut() else {
            return Vec::new();
        };
        let mut released = Vec::new();
        for (position, indexes) in blocks {
            let Some(at) = position.checked_sub(kept.front) else {
                continue;
            };
            let ordinal = window_start + at as u64;
            let Some(message) = kept.prompt.get_mut(*position) else {
                continue;
            };
            for &index in indexes {
                let Some(block) = message.content.get_mut(index) else {
                    continue;
                };
                if window_holds(ordinal, index, block) {
                    if let Some(form) = block.released() {
                        *block = form;
                    }
                }
            }
            released.push((ordinal, indexes.clone()));
        }
        released
    }

    /// Fills in the released blocks of the stored prompt's message at `position` from `whole`,
    /// its window message read back, block by block at the same indexes.
    pub(crate) fn restore(&mut self, position: usize, whole: &Message) -> Result<(), LoopError> {
        let Some(message) = self
            .0
            .as_mut()
            .and_then(|kept| kept.prompt.get_mut(position))
        else {
            return Ok(());
        };
        for (index, block) in message.content.iter_mut().enumerate() {
            if block.holds_released() {
                *block = whole
                    .content
                    .get(index)
                    .cloned()
                    .ok_or(LoopError::Parts(format!(
                        "stored prompt message {position} read back with fewer blocks"
                    )))?;
            }
        }
        Ok(())
    }

    /// The stored prompt a request renders from: `sealed`, the sealed messages a transcript put
    /// in front of the window's entries, each stamped with its own position, then the window's
    /// messages as kept. Called after [`KeptPrompt::update`].
    pub(crate) fn stored(&mut self, sealed: &crate::compaction::SealedMessages) -> &[Message] {
        let kept = self
            .0
            .as_mut()
            .expect("a stored prompt is lent after it is brought up to date");
        if !kept
            .front_of
            .as_ref()
            .is_some_and(|front| Arc::ptr_eq(front, sealed))
        {
            let stamped: Vec<Message> = sealed
                .iter()
                .map(|(ordinal, message)| {
                    let mut message = Message::clone(message);
                    message.ordinal = Some(ordinal - 1);
                    message
                })
                .collect();
            let count = stamped.len();
            kept.prompt.splice(..kept.front, stamped);
            kept.front = count;
            kept.front_of = Some(Arc::clone(sealed));
        }
        &kept.prompt
    }
}

impl Kept {
    /// Adds to `message`, the window message at `index`, the repetition warnings its run's
    /// counters give it, as `repetition::repetition_guidance` adds them run by run: a run's counters
    /// start at its first assistant message, or, for the run a cut split (it starts before the
    /// window's first message `first`), from the counters the cut kept for its sealed steps,
    /// `spanning`, when there are any.
    fn guide(
        &mut self,
        index: usize,
        message: &mut Message,
        episodes: &[EpisodeRenderInput],
        first: u64,
        spanning: Option<&serde_json::Value>,
        corrected: Option<&[Option<usize>]>,
    ) -> Result<(), LoopError> {
        let start =
            |episode: &EpisodeRenderInput| episode.prompt_start.saturating_sub(first as usize);
        // The run the message belongs to: the last one starting at or before it.
        let mut run = self.run;
        loop {
            let next = run.map_or(0, |run| run + 1);
            match episodes.get(next) {
                Some(episode) if start(episode) <= index => run = Some(next),
                _ => break,
            }
        }
        let Some(at) = run else {
            return Ok(());
        };
        if let Some(starts) = corrected {
            let Some(first_answer) = starts[at] else {
                return Ok(());
            };
            if index < first_answer.saturating_sub(first as usize) {
                return Ok(());
            }
        }
        if run != self.run {
            self.run = run;
            self.guidance = match spanning.filter(|_| episodes[at].prompt_start < first as usize) {
                Some(seed) => {
                    let seed = serde_json::from_value(seed.clone()).map_err(|error| {
                        LoopError::Parts(format!("stored repetition counters: {error}"))
                    })?;
                    RunGuidance::Counting(RepetitionGuidance::seeded(&seed).ok_or_else(|| {
                        LoopError::Parts(
                            "stored repetition counters were taken under other warning rules"
                                .into(),
                        )
                    })?)
                }
                None => RunGuidance::Waiting,
            };
        }
        if matches!(self.guidance, RunGuidance::Waiting) && message.role == Role::Assistant {
            self.guidance = RunGuidance::Counting(RepetitionGuidance::new());
        }
        if let RunGuidance::Counting(guidance) = &mut self.guidance {
            guidance.guide(message);
        }
        Ok(())
    }
}
