//! Generated conversation fold states, for the checkpoint property tests. They live here because
//! a held tool turn is private to the fold.

use super::{Conversation, Delivered, EntryState, Episode, ToolTurn};
use crate::fuzzing::gen::fields;

fields!(Delivered { part, notice, text });
fields!(EntryState { facts, delivered });
fields!(Episode {
    run_id,
    start,
    first_user,
    render_config,
    render_key,
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
});
