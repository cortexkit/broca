//! Global instructions are run configuration, never editable transcript entries.
//! Turn-scoped system messages remain at their original conversation position.

use broca_protocol::{CallOptions, ContentBlock, Message, Role};

pub fn is_global(message: &Message) -> bool {
    is_global_role(message.role, message.turn_system)
}

/// [`is_global`] from a message's role and its turn-system flag.
pub fn is_global_role(role: Role, turn_system: bool) -> bool {
    role == Role::System && !turn_system
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

pub fn from_manifest(manifest: &broca_records::record::PlanManifest) -> Vec<Message> {
    if manifest.system_text.is_empty() {
        return Vec::new();
    }
    vec![Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: manifest.system_text.clone().into(),
        }],
    )]
}

pub fn view(mut opts: CallOptions, global: &[Message]) -> Result<CallOptions, String> {
    if global.iter().any(|message| !is_global(message)) {
        return Err("global_system configuration contains a transcript message".into());
    }
    if opts.prompt.iter().any(is_global) {
        return Err("mode-3 transcript contains a global system message".into());
    }
    if global.is_empty() && opts.render_episodes.raw_ordinals {
        return Ok(opts);
    }
    opts.prompt.splice(..0, global.iter().cloned());
    opts.render_episodes = super::render::raw_ordinals(&opts.render_episodes);
    for marker in &mut opts.declared_markers {
        marker.message_index += global.len();
    }
    Ok(opts)
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
        let mut opts = CallOptions::new(transcript);
        opts.declared_markers = vec![broca_protocol::request::DeclaredMessageMarker {
            message_index: 1,
            rank: 2,
        }];
        let user_blocks = opts.prompt[1].content.as_ptr();
        let rendered = view(opts, &globals).unwrap();
        assert_eq!(rendered.prompt, legacy);
        assert!(rendered.render_episodes.raw_ordinals);
        assert_eq!(rendered.declared_markers[0].message_index, 3);
        assert_eq!(rendered.declared_markers[0].rank, 2);
        assert_eq!(rendered.prompt[3].content.as_ptr(), user_blocks);
    }
}
