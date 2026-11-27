use super::*;
use broca_protocol::request::{DeclaredMessageMarker, EpisodeRenderInput, RenderHistory};
use std::sync::Arc;

/// A message at 0-based position `ordinal` in the session, as a request carries it.
fn at(ordinal: u64, role: Role, text: &str) -> Message {
    let mut message = Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
    message.ordinal = Some(ordinal);
    message
}

fn texts(opts: &CallOptions) -> Vec<(Role, bool, Option<u64>, String)> {
    opts.prompt
        .iter()
        .map(|message| {
            let ContentBlock::Text { text } = &message.content[0] else {
                panic!("text only");
            };
            (
                message.role,
                message.turn_system,
                message.ordinal,
                text.to_string(),
            )
        })
        .collect()
}

/// Two runs: entries 1 and 2 are the first run's input and reply, 3 and 4 the second's.
fn two_runs() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "system".into(),
            }],
        ),
        at(0, Role::User, "first"),
        at(1, Role::Assistant, "one"),
        at(2, Role::User, "second"),
        at(3, Role::Assistant, "two"),
    ]);
    opts.render_episodes = Arc::new(RenderHistory::from(vec![
        EpisodeRenderInput {
            prompt_start: 0,
            first_user: Some(0),
            frozen_config: serde_json::Value::Null,
        },
        EpisodeRenderInput {
            prompt_start: 2,
            first_user: Some(2),
            frozen_config: serde_json::Value::Null,
        },
    ]));
    opts.declared_markers = vec![
        DeclaredMessageMarker {
            message_index: 2,
            rank: 0,
        },
        DeclaredMessageMarker {
            message_index: 4,
            rank: 1,
        },
    ];
    opts
}

#[test]
fn notes_follow_their_anchor_as_system_messages_in_order() {
    let opts = two_runs();
    let view = view(
        &opts,
        [(1, Note::Text("note a")), (1, Note::Text("note b"))],
    )
    .expect("the anchor is held");
    assert_eq!(
        texts(&view),
        vec![
            (Role::System, false, None, "system".into()),
            (Role::User, false, Some(0), "first".into()),
            (Role::System, true, None, "note a".into()),
            (Role::System, true, None, "note b".into()),
            (Role::Assistant, false, Some(1), "one".into()),
            (Role::User, false, Some(2), "second".into()),
            (Role::Assistant, false, Some(3), "two".into()),
        ]
    );
    // Episode ordinals stay fixed and each prompt-indexed marker stays on its message.
    let starts: Vec<_> = view
        .render_episodes
        .episodes
        .iter()
        .map(|episode| (episode.prompt_start, episode.first_user))
        .collect();
    assert_eq!(starts, vec![(0, Some(0)), (2, Some(2))]);
    let marked: Vec<_> = view
        .declared_markers
        .iter()
        .map(|marker| marker.message_index)
        .collect();
    assert_eq!(marked, vec![4, 6]);
    // The options it was made from are untouched.
    assert_eq!(opts.prompt.len(), 5);
}

#[test]
fn a_note_whose_anchor_the_request_doesnt_hold_is_left_out() {
    // A compaction view dropped entry 1; the note about it no longer renders.
    let opts = two_runs();
    assert!(view(&opts, [(9, Note::Text("lost"))]).is_none());
    let view = view(&opts, [(9, Note::Text("lost")), (3, Note::Text("kept"))])
        .expect("one anchor is held");
    assert_eq!(
        texts(&view)
            .iter()
            .filter(|(_, turn_system, _, _)| *turn_system)
            .map(|(_, _, _, text)| text.as_str())
            .collect::<Vec<_>>(),
        vec!["kept"]
    );
}

#[test]
fn raw_ordinal_episodes_keep_their_places() {
    // A list read by raw ordinals names messages by their session position, which an inserted
    // note doesn't have, so nothing in it moves.
    let mut opts = two_runs();
    let mut history = opts.render_episodes.as_ref().clone();
    history.raw_ordinals = true;
    opts.render_episodes = Arc::new(history);
    let view = view(&opts, [(1, Note::Text("note"))]).expect("the anchor is held");
    assert!(Arc::ptr_eq(&view.render_episodes, &opts.render_episodes));
}
