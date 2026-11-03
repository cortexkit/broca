//! Global instructions are run configuration, never editable transcript entries.
//! Turn-scoped system messages remain at their original conversation position.

use broca_protocol::{CallOptions, ContentBlock, Message, Role};

pub fn is_global(message: &Message) -> bool {
    message.role == Role::System && !message.turn_system
}

pub fn take_globals(input: &mut Vec<Message>) -> Vec<Message> {
    let (mut global, transcript): (Vec<_>, Vec<_>) =
        std::mem::take(input).into_iter().partition(is_global);
    for message in &mut global {
        message.ordinal = None;
        message.tags = Default::default();
    }
    *input = transcript;
    global
}

pub fn from_manifest(manifest: &broca_wal::PlanManifest) -> Vec<Message> {
    if manifest.system_text.is_empty() {
        return Vec::new();
    }
    vec![Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: manifest.system_text.clone(),
        }],
    )]
}

pub fn frozen(
    mode3: bool,
    config: &broca_wal::RunConfig,
    manifest: Option<&broca_wal::PlanManifest>,
) -> Result<Vec<Message>, String> {
    if !mode3 {
        if config.global_system.is_some() {
            return Err("global_system is only valid on mode-3 runs".into());
        }
        return Ok(Vec::new());
    }
    if let Some(manifest) = manifest {
        if config.global_system.is_some() {
            return Err("global_system must be absent on planned runs".into());
        }
        return Ok(from_manifest(manifest));
    }
    let global = config.global_system.as_deref().cloned().unwrap_or_default();
    if global.iter().any(|message| !is_global(message)) {
        return Err("global_system configuration contains a transcript message".into());
    }
    Ok(global)
}

pub fn view(opts: &CallOptions, global: &[Message]) -> Result<Option<CallOptions>, String> {
    if global.iter().any(|message| !is_global(message)) {
        return Err("global_system configuration contains a transcript message".into());
    }
    if opts.prompt.iter().any(is_global) {
        return Err("mode-3 transcript contains a global system message".into());
    }
    if global.is_empty() && opts.render_episodes.raw_ordinals {
        return Ok(None);
    }
    let mut prompt = Vec::with_capacity(global.len() + opts.prompt.len());
    prompt.extend(global.iter().cloned());
    prompt.extend(opts.prompt.iter().cloned());
    let mut view = opts.with_prompt(prompt);
    let mut history = opts.render_episodes.as_ref().clone();
    history.raw_ordinals = true;
    view.render_episodes = std::sync::Arc::new(history);
    for marker in &mut view.declared_markers {
        marker.message_index += global.len();
    }
    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_v2_globals_leave_turn_system_messages_in_native_positions() {
        let first = Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "first".into(),
            }],
        );
        let second = Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "second".into(),
            }],
        );
        let mut turn = Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "at this turn".into(),
            }],
        );
        turn.turn_system = true;
        let user = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "question".into(),
            }],
        );
        let legacy = vec![first, second, turn.clone(), user.clone()];
        let mut transcript = legacy.clone();
        let globals = take_globals(&mut transcript);
        assert_eq!(transcript, [turn, user]);
        let opts = CallOptions::new(transcript);
        let rendered = view(&opts, &globals).unwrap().unwrap();
        assert_eq!(rendered.prompt, legacy);
    }
}
