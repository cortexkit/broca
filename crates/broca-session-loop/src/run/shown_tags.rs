use super::*;

/// A new run starts numbering after the highest part number shown in the prompt,
/// with each message numbered under the rules
/// of the run that placed it. Only a run that renders numbers counts anything, so a session
/// that has never rendered stays at 0.
///
/// Counted over the window. A window that doesn't start at the session's first message has had
/// numbers sealed out with the history before it, which isn't modelled yet.
pub(super) async fn shown_high<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    checked: bool,
) -> Result<u64, LoopError> {
    let conversation = writer.conversation();
    let start = conversation.window_start();
    // History before the window was sealed at a compaction cut, whose marker records the highest
    // number shown through it; numbers never change at a cut.
    let sealed_high = match (start > 1, &conversation.marker) {
        (false, _) => 0,
        (true, Some(marker)) => marker.checkpoint,
        (true, None) => {
            return Err(LoopError::Unsupported(
                "a new run after history was sealed without a compaction cut",
            ))
        }
    };
    // Each window run's frozen choices: whether it rendered numbers (its start's run config), and
    // whether its replies were cleaned (that and its render config).
    // A request lists the runs sealed before the window first; only the window's own place
    // messages here.
    let rendered = if checked {
        signed_floor::render_episodes_checked(writer, parts).map_err(signed_floor::before_start)?
    } else {
        render_episodes(writer, parts)?
    };
    let window_runs = &rendered[conversation.sealed_episodes.len()..];
    let mut runs = Vec::new();
    for (episode, render_input) in conversation.episodes.iter().zip(window_runs) {
        let start = writer.run_start(&episode.run_id).await?.ok_or_else(|| {
            LoopError::Parts(format!(
                "the window names run {} but the session holds no start for it",
                episode.run_id
            ))
        })?;
        let choices = start.run_choices.unwrap_or(Value::Null);
        let render = choices["config"]["render_tags"].as_bool().unwrap_or(false);
        let clean =
            broca_core::compaction_v2::metadata::clean_replies(render, &render_input.frozen_config);
        runs.push(((episode.start - 1) as usize, render, clean));
    }
    let mut high = sealed_high;
    // The run that placed a message is the last one starting at or before it. Runs are in the
    // order they started and messages in the order they were placed, so one pass finds each.
    let mut placing = (false, false);
    let mut next_run = runs.iter().peekable();
    for (index, message) in conversation.messages.iter().enumerate() {
        if let Some(base) = conversation
            .entries
            .get(index)
            .and_then(|entry| entry.facts.as_ref())
            .and_then(|facts| facts.runner_tag_base)
        {
            high = high.max(base);
        }
        let index = index + (start - 1) as usize;
        while let Some((_, render, clean)) = next_run.next_if(|(first, _, _)| *first <= index) {
            placing = (*render, *clean);
        }
        let (render, clean) = placing;
        high = crate::window_shape::runner_high(message, ComputedRules { render, clean }, high)
            .map_err(|_| LoopError::Unsupported("part numbers past the numbering limit"))?;
    }
    Ok(high.max(writer.retired_tag_floor().saturating_sub(1)))
}
