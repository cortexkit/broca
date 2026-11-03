//! Durable v2 state is folded independently of the legacy replacement renderer.

use broca_protocol::BindIdentity;
use broca_wal::WalRecord;
use cortexkit_role_compaction::view::AppliedView;

use super::{state::State, storage};

pub fn fold(state: &mut State, record: &WalRecord, session: &BindIdentity) -> Result<(), String> {
    match record {
        WalRecord::RunStarted { .. } => state.begin_run(),
        WalRecord::StepStarted { step_id, .. } => state.start_step(step_id.0),
        WalRecord::CompactionViewV2(view) => {
            let view = storage::rebuild(
                state.latest_received(),
                view.version,
                view.checksum.clone(),
                &view.edits,
            )?;
            state.restore_accepted(view)?;
        }
        WalRecord::CompactionViewAppliedV2 {
            step_id,
            version,
            checksum,
        } => {
            state.restore_applied_at(*step_id, &AppliedView::new(*version, checksum))?;
        }
        WalRecord::CompactionMarker {
            harness,
            session: marker_session,
            view_version,
            ..
        } => {
            check_marker_identity(harness, marker_session, session)?;
            state.restore_marker(*view_version);
            state.marker = Some(Box::new(record.clone()));
        }
        _ => {}
    }
    Ok(())
}

/// Check cached markers too: a checkpoint copied along with a fork must not
/// authorize Setup or rendering under another session's address.
pub fn validate_identity(state: &State, session: &BindIdentity) -> Result<(), String> {
    match state.marker.as_deref() {
        None => Ok(()),
        Some(WalRecord::CompactionMarker {
            harness,
            session: marker_session,
            ..
        }) => check_marker_identity(harness, marker_session, session),
        Some(_) => Err("compaction marker checkpoint contains a non-marker record".into()),
    }
}

fn check_marker_identity(
    harness: &str,
    marker_session: &str,
    session: &BindIdentity,
) -> Result<(), String> {
    if harness == session.harness && marker_session == session.session {
        return Ok(());
    }
    Err(format!("compaction marker session mismatch: marker names {harness}/{marker_session}, loaded as {}/{}", session.harness, session.session))
}
