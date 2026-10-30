//! Per-episode plan text is a render view, not a rewrite of transcript positions or tags.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    CallOptions,
};

pub(crate) fn frozen_text(
    enabled: bool,
    manifest: Option<&broca_wal::PlanManifest>,
) -> Option<&str> {
    if enabled {
        manifest.map(|manifest| manifest.system_text.as_str())
    } else {
        None
    }
}

pub(crate) fn view(opts: &CallOptions, durable: &[Message], text: &str) -> Option<CallOptions> {
    let original = durable
        .first()
        .filter(|message| message.role == Role::System);
    let content = vec![ContentBlock::Text { text: text.into() }];
    if original.is_some_and(|message| message.content == content)
        || (original.is_none() && text.is_empty())
    {
        return None;
    }
    let mut prompt = opts.prompt.clone();
    // The plan's original system message leads both the raw and compacted prompt. Leave any
    // other system messages alone; they may be instructions inserted by a transform.
    let has_original = original.is_some() && prompt.first().is_some_and(|m| m.role == Role::System);
    let delta = match (has_original, text.is_empty()) {
        (true, false) => {
            prompt[0].content = content;
            0
        }
        (true, true) => {
            prompt.remove(0);
            -1
        }
        (false, false) => {
            prompt.insert(0, Message::new(Role::System, content));
            1
        }
        (false, true) => return None,
    };
    let mut view = opts.with_prompt(prompt);
    if delta != 0 {
        let mut history = opts.render_episodes.as_ref().clone();
        for episode in &mut history.episodes {
            episode.prompt_start = episode.prompt_start.saturating_add_signed(delta);
            episode.first_user = episode.first_user.map(|i| i.saturating_add_signed(delta));
        }
        view.render_episodes = std::sync::Arc::new(history);
        view.declared_markers.retain_mut(|marker| {
            if delta < 0 && marker.message_index == 0 {
                return false;
            }
            marker.message_index = marker.message_index.saturating_add_signed(delta);
            true
        });
    }
    Some(view)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_metadata_difference_does_not_enable_an_override() {
        let manifest = serde_json::from_value::<broca_wal::PlanManifest>(serde_json::json!({
            "plan":"{}", "plan_identity":"legacy", "composition_digest":"c", "fetches":[],
            "tools":[], "system_text":"metadata text", "system_digest":"s", "join_version":1, "role_versions":{}
        })).unwrap();
        let control = crate::RunControl::default();
        assert!(!control.manifest_system_override);
        assert_eq!(
            frozen_text(control.manifest_system_override, Some(&manifest)),
            None
        );
        assert_eq!(frozen_text(true, Some(&manifest)), Some("metadata text"));
    }

    fn message(role: Role, text: &str) -> Message {
        Message::new(role, vec![ContentBlock::Text { text: text.into() }])
    }

    #[test]
    fn replacement_and_removal_leave_durable_history_unchanged() {
        let original = CallOptions::new(vec![
            message(Role::System, "old"),
            message(Role::User, "hi"),
        ]);
        let before = crate::canonical_request_bytes(&original);
        assert!(view(&original, &original.prompt, "old").is_none());
        let replaced = view(&original, &original.prompt, "new").unwrap();
        assert_eq!(replaced.prompt[0], message(Role::System, "new"));
        assert_eq!(replaced.prompt[1], message(Role::User, "hi"));
        let removed = view(&original, &original.prompt, "").unwrap();
        assert_eq!(removed.prompt, vec![message(Role::User, "hi")]);
        assert_eq!(crate::canonical_request_bytes(&original), before);
    }

    #[test]
    fn insertion_remaps_episode_coordinates_and_keeps_other_instructions() {
        let original = CallOptions::new(vec![message(Role::User, "hi")]);
        let mut transformed = original.with_prompt(vec![
            message(Role::System, "hook"),
            message(Role::User, "hi"),
        ]);
        transformed.render_episodes = std::sync::Arc::new(
            vec![broca_protocol::request::EpisodeRenderInput {
                prompt_start: 0,
                first_user: Some(1),
                frozen_config: serde_json::Value::Null,
            }]
            .into(),
        );
        let inserted = view(&transformed, &original.prompt, "plan").unwrap();
        assert_eq!(
            inserted.prompt,
            vec![
                message(Role::System, "plan"),
                message(Role::System, "hook"),
                message(Role::User, "hi")
            ]
        );
        assert_eq!(inserted.render_episodes[0].first_user, Some(2));
    }
}
