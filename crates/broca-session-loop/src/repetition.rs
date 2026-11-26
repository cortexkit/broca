use std::borrow::Cow;

use broca_core::tool_repetition::WarningRules;
use broca_protocol::content::Message;
use broca_protocol::request::EpisodeRenderInput;
use broca_session_store::fold::conversation::Conversation;
use serde_json::Value;

use crate::run::LoopError;

pub(crate) fn after_input(config: &Value) -> bool {
    config
        .get("repetition_after_input")
        .and_then(Value::as_bool)
        == Some(true)
}

const WARNING_RULES: &str = "repetition_rules";
const CONSECUTIVE_RULES: u64 = 2;

/// Freeze the warning rules at admission. The earlier input-boundary flag predates the
/// consecutive-call warning and cannot distinguish runs that saw it from runs that did not.
pub(crate) fn freeze_warning_rules(mut config: Value) -> Value {
    if let Some(config) = config.as_object_mut() {
        config.insert(WARNING_RULES.into(), CONSECUTIVE_RULES.into());
    }
    config
}

pub(crate) fn remove_warning_rules(config: &mut Value) {
    if let Some(config) = config.as_object_mut() {
        config.remove(WARNING_RULES);
    }
}

/// The warning rules shape the prompt in the run loop, not in the provider renderer.
pub(crate) fn provider_config(config: &Value) -> Cow<'_, Value> {
    if let Some(members) = config
        .as_object()
        .filter(|members| members.contains_key(WARNING_RULES))
    {
        let mut members = members.clone();
        members.remove(WARNING_RULES);
        Cow::Owned(Value::Object(members))
    } else {
        Cow::Borrowed(config)
    }
}

pub(crate) fn warning_rules(episode: &EpisodeRenderInput) -> WarningRules {
    if episode
        .frozen_config
        .get(WARNING_RULES)
        .and_then(Value::as_u64)
        == Some(CONSECUTIVE_RULES)
    {
        WarningRules::Consecutive
    } else {
        WarningRules::Legacy
    }
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
    spanning: Option<&Value>,
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
        let rules = warning_rules(episode);
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
                    rules,
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
            broca_core::repetition::restore_repetition_guidance(
                &mut prompt[first_step..end],
                rules,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::repetition::{repetition_checkpoint, RepetitionCheckpointTaken};
    use broca_provider::{families::anthropic_frozen_config, wire::FrozenRenderConfig};
    use serde_json::{from_value, json};

    #[test]
    fn a_provider_config_reader_ignores_the_frozen_warning_rules() {
        let mut legacy = anthropic_frozen_config("claude-sonnet-4-5");
        legacy.repetition_after_input = true;
        let raw = serde_json::to_value(&legacy).unwrap();
        let marked = freeze_warning_rules(raw.clone());
        assert_eq!(marked[WARNING_RULES], CONSECUTIVE_RULES);
        // Older builds use the same typed reader, without a member for the loop's warning rules.
        let read: FrozenRenderConfig = serde_json::from_value(marked.clone()).unwrap();
        assert_eq!(read, legacy);
        assert_eq!(provider_config(&marked).as_ref(), &raw);
        assert!(matches!(provider_config(&raw), Cow::Borrowed(_)));
    }

    #[test]
    fn a_legacy_checkpoint_seeds_the_legacy_warning_after_a_cut() {
        let messages: Vec<Message> = (0..5)
            .flat_map(|id| {
                [
                    from_value(json!({"role":"assistant","content":[{
                        "type":"tool_call","tool_call_id":format!("c{id}"),
                        "tool_name":"read","input":{"path":"file"}
                    }]}))
                    .unwrap(),
                    from_value(json!({"role":"tool","content":[{
                        "type":"tool_result","tool_call_id":format!("c{id}"),
                        "output":{"kind":"text","text":"same"},"is_error":false
                    }]}))
                    .unwrap(),
                ]
            })
            .collect();
        let RepetitionCheckpointTaken::Taken(checkpoint) =
            repetition_checkpoint(&messages[..4], None)
        else {
            panic!("two complete tool turns checkpoint");
        };
        let mut seed = serde_json::to_value(checkpoint).unwrap();
        assert!(seed
            .as_object_mut()
            .unwrap()
            .remove("consecutive")
            .is_some());
        let mut window = messages[4..].to_vec();
        let episode = EpisodeRenderInput {
            prompt_start: 0,
            first_user: None,
            frozen_config: json!({"repetition_after_input":true}),
        };
        repetition_guidance(&mut window, &[episode], 4, Some(&seed), None).unwrap();
        let final_message = serde_json::to_string(window.last().unwrap()).unwrap();
        assert!(final_message.contains("read returned exactly the same result 5 times."));
    }
}
