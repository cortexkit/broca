//! Generated conversation fold states, for the checkpoint property tests. They live here because
//! a held tool turn is private to the fold.

use super::{Conversation, Delivered, EntryState, Episode, ToolTurn, WindowChanges};
use crate::fuzzing::gen::fields;

/// A fold's own record of its changes, which no checkpoint carries: a new one.
impl crate::fuzzing::gen::Gen for WindowChanges {
    fn gen(_: &mut crate::fuzzing::gen::G<'_>) -> Self {
        Self::default()
    }
}

fields!(Delivered {
    part,
    notice,
    text,
    send_id,
    ride
});
fields!(EntryState {
    facts,
    delivered,
    turn
});
fields!(Episode {
    run_id,
    start,
    first_user,
    render_config,
    render_key,
    first_model_answer,
});
fields!(ToolTurn {
    assistant,
    calls,
    replies
});
fields!(Conversation {
    messages,
    open,
    appends_in_conversation,
    marker,
    placed,
    entries,
    open_entry,
    window_pending,
    render_config,
    episodes,
    render_key,
    sealed_episodes,
    window_repetition,
    first_protocol_compaction,
    human_prompts,
    joined_through,
    changes,
});
