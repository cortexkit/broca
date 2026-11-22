//! Rebuilding the compaction state from what the session stored: the views it accepted, the
//! steps that applied them, and the markers its cuts wrote. The session store's own records
//! name each of these; the host reads them in order and calls the function for each.

use broca_protocol::BindIdentity;
use broca_records::record::StoredViewEdit;
use cortexkit_role_compaction::view::AppliedView;

use super::{state::State, storage};

/// An accepted view, rebuilt from its stored edits against the view accepted before it, and
/// the module that handed it over through `session.apply_view` (`None` for a view a run's step
/// accepted).
pub fn restore_view(
    state: &mut State,
    version: u64,
    checksum: String,
    edits: &[StoredViewEdit],
    built_through: Option<String>,
    new_signed: Option<String>,
    applied_by: Option<String>,
) -> Result<(), String> {
    let received = storage::rebuild(
        state.latest_received(),
        version,
        checksum,
        edits,
        built_through,
        new_signed,
    )?;
    state.restore_accepted(received)?;
    state.restore_accepted_by(applied_by);
    Ok(())
}

/// A view a step applied: `version` and `checksum` name an accepted view.
pub fn restore_applied(
    state: &mut State,
    step: u64,
    version: u64,
    checksum: &str,
) -> Result<(), String> {
    state.restore_applied_at(step, &AppliedView::new(version, checksum))
}

/// A marker a cut wrote, under view `view_version`. A marker names the session it was written
/// for: one naming another session, as a log copied along with a fork does, must not authorize
/// Setup or rendering under `session`'s address.
pub fn restore_marker(
    state: &mut State,
    harness: &str,
    marker_session: &str,
    view_version: u64,
    session: &BindIdentity,
) -> Result<(), String> {
    if harness != session.harness || marker_session != session.session {
        return Err(format!(
            "compaction marker session mismatch: marker names {harness}/{marker_session}, \
             loaded as {}/{}",
            session.harness, session.session
        ));
    }
    state.restore_marker(view_version);
    Ok(())
}
