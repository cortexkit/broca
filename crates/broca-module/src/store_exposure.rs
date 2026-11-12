//! How the llm-runner surface exposes what a session kept in the new storage holds.
//!
//! **Replies.** The model is shown a number in front of each part (`§N§`) and often starts its
//! own reply with one. The stored message keeps what the model wrote; every caller-facing
//! exposure of it serves the text cleaned of tag notation exactly when the message's recorded
//! `EntryFacts::clean_reply` says its run cleans replies. [`served_message`] is the one place
//! that decides and cleans, for `session.read` in every mode, `session.history` (whose pages are
//! `session.read` pages), and the final message `run.result` and `send.status` carry. Only the
//! model's text parts change; ids, ordinals, part keys and tags don't, and a message whose fact
//! is false is served byte for byte as stored. The control lane's `assistant_message` unit is not
//! an exposure here: it carries the stored text on both storages, live and replayed alike.
//!
//! **History version.** Serving replies cleaned that earlier builds served with their tags is a
//! history rewrite, which the runner contract marks by raising `history_version` before the
//! changed bytes are served. Nothing stored changes, so the raise is an offset: every read a
//! client caches by version serves [`served_history_version`] of the head's stored version, and
//! a caller's version is read back through [`stored_history_version`]. An older build serves the
//! stored version with raw text again, which is what that version meant. Ids that name stored
//! state keep the stored version: an output-reference id names stored tool output, which
//! cleaning never changes, so an id handed out before stays valid. The compaction provider's
//! surface (`session.entries` and the rest of its route) carries no history version and is
//! unaffected.

use std::borrow::Cow;

use broca_protocol::Message;

/// How many times this runner has changed how it exposes the replies a session kept in the new
/// storage holds, without changing what is stored: the offset between a session's stored history
/// version and the one the llm-runner surface serves. It is 1 since replies are served cleaned of
/// tag notation where their run cleans them. Raise it by one if the exposure of stored history
/// ever changes again, so every reader's cached pages are refused as after any rewrite.
pub const EXPOSURE_EPOCH: u64 = 1;

/// The history version the llm-runner surface serves for a session whose head stores `stored`.
///
/// Use it only where a client caches what it reads by version: the `history_version` that
/// `session.read`, `session.head`, `session.history` (and its cursors) and an import's reply
/// serve, and the version a caller sends back with `after_mid`. Never use it for anything that
/// scopes an id or names stored state: output-reference ids, subscribe cursors and compaction
/// entries name the stored transcript and keep the stored version.
pub fn served_history_version(stored: u64) -> u64 {
    stored.saturating_add(EXPOSURE_EPOCH)
}

/// The stored history version a caller's `served` version names, or `None` when it names none
/// this build serves (one from before the epoch was raised), which no session's history matches.
/// Output-reference ids are scoped by it (`ServeHandler::output_ref_history_version`), so an id
/// handed out before the epoch was raised still names the same stored transcript.
pub fn stored_history_version(served: u64) -> Option<u64> {
    served.checked_sub(EXPOSURE_EPOCH)
}

/// `message` as the llm-runner surface serves it: the model's text cleaned of tag notation when
/// `clean_reply`, the message's recorded `EntryFacts::clean_reply`, is true, and otherwise
/// unchanged.
pub fn served_message(message: &Message, clean_reply: bool) -> Cow<'_, Message> {
    broca_protocol::tags::exposed_reply(message, clean_reply)
}
