use broca_session_store::fold::conversation::Conversation;

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
    let episode = conversation
        .episodes
        .iter()
        .find(|episode| episode.start as usize == start + 1)?;
    if let Some(ordinal) = episode.first_model_answer {
        return Some(ordinal as usize - 1);
    }
    let end = conversation
        .episodes
        .iter()
        .find(|next| next.start > episode.start)
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
