use broca_protocol::content::Message;
use broca_protocol::request::EpisodeRenderInput;
use broca_session_store::fold::conversation::Conversation;

use crate::run::LoopError;

pub(crate) fn after_input(config: &serde_json::Value) -> bool {
    config
        .get("repetition_after_input")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

/// The first model answer, not an assistant message supplied as input. New checkpoints retain
/// the ordinal across cuts; older checkpoints can recover it from their existing entry facts.
pub(crate) fn first_answer(
    conversation: &Conversation,
    start: usize,
    session: &str,
) -> Option<usize> {
    // The window's runs are in the order they started, so their starts never decrease: a
    // binary search finds the first run starting at `start` and the first one after it. A
    // request asks this for every run on every step.
    let episodes = &conversation.episodes;
    let at = episodes.partition_point(|episode| (episode.start as usize) < start + 1);
    let episode = episodes
        .get(at)
        .filter(|episode| episode.start as usize == start + 1)?;
    if let Some(ordinal) = episode.first_model_answer {
        return Some(ordinal as usize - 1);
    }
    let end = episodes[at..]
        .get(episodes[at..].partition_point(|next| next.start <= episode.start))
        .map_or(u64::MAX, |next| next.start);
    let mut missing = None;
    for ordinal in episode.start.max(conversation.window_start())..end.min(conversation.placed + 1)
    {
        let Some((message, entry)) = conversation.window_entry(ordinal) else {
            continue;
        };
        if message.role() != broca_protocol::Role::Assistant {
            continue;
        }
        match entry.facts.as_ref() {
            Some(facts) if facts.native => return Some(ordinal as usize - 1),
            Some(_) => {}
            None => {
                missing.get_or_insert(ordinal as usize - 1);
            }
        }
    }
    // An older cut can omit both attribution and the original input records. There is no
    // trustworthy input length in that checkpoint; keep the old boundary rather than refuse.
    if missing.is_some()
        || (episode.start < conversation.window_start() && conversation.window_repetition.is_some())
    {
        tracing::warn!(
            target: "repetition_boundary",
            session,
            run_id = %episode.run_id,
            "input attribution unavailable; retaining the recorded legacy boundary"
        );
        return missing.or(Some(conversation.window_start() as usize - 1));
    }
    None
}

/// Adds repetition guidance to copies of tool results sent in the request, never to stored
/// results. Each run's model steps (from its first answer up to the next run's input) are
/// folded with fresh counters, independently per run.
/// Adds each run's repetition warnings to its window messages. `prompt` holds the window,
/// whose first message has session position `first`, while each run's place is a session
/// position. A run that started before the window counts on from `spanning`, the counters
/// the latest cut kept for its sealed steps; without them none of its steps were sealed.
pub(crate) fn repetition_guidance(
    prompt: &mut [Message],
    episodes: &[EpisodeRenderInput],
    first: u64,
    spanning: Option<&serde_json::Value>,
    corrected: Option<(
        &broca_session_store::fold::conversation::Conversation,
        String,
    )>,
) -> Result<(), LoopError> {
    let len = prompt.len();
    let index = |position: usize| position.saturating_sub(first as usize).min(len);
    for (at, episode) in episodes.iter().enumerate() {
        let start = index(episode.prompt_start);
        let end = index(
            episodes
                .get(at + 1)
                .map_or(usize::MAX, |next| next.prompt_start),
        );
        if start >= end {
            continue;
        }
        let corrected_start = match &corrected {
            Some((conversation, session)) => {
                match first_answer(conversation, episode.prompt_start, session) {
                    Some(position) => Some(index(position)),
                    None => continue,
                }
            }
            None => None,
        };
        if episode.prompt_start < first as usize {
            if let Some(seed) = spanning {
                let seed = serde_json::from_value(seed.clone()).map_err(|error| {
                    LoopError::Parts(format!("stored repetition counters: {error}"))
                })?;
                if !broca_core::repetition::restore_repetition_guidance_seeded(
                    &mut prompt[start..end],
                    &seed,
                ) {
                    return Err(LoopError::Parts(
                        "stored repetition counters were taken under other warning rules".into(),
                    ));
                }
                continue;
            }
        }
        let first_step = corrected_start.or_else(|| {
            (start..end)
                .find(|position| prompt[*position].role == broca_protocol::content::Role::Assistant)
        });
        if let Some(first_step) = first_step {
            broca_core::repetition::restore_repetition_guidance(&mut prompt[first_step..end]);
        }
    }
    Ok(())
}
