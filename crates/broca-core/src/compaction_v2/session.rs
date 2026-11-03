//! Session-owned memory shared by run boundaries and pushed-view admission.

use broca_protocol::BindIdentity;
use broca_wal::{FramedRecord, WalRecord};

use super::{conditions::check_plan_version, fold, runtime::Runtime, state::State, systems};

#[derive(Default)]
pub struct Session {
    pub state: tokio::sync::Mutex<State>,
    pub runtime: Runtime,
}

impl Session {
    /// The actor reloads at its serialized admission point, before spawning a
    /// run. Never replace this state from a stale snapshot while that run is active.
    pub async fn restore(&self, state: State) {
        self.runtime.restore_marker(state.marker_version());
        *self.state.lock().await = state;
    }
}

pub fn load(session: &BindIdentity, records: &[FramedRecord]) -> Result<State, String> {
    if !crate::is_mode3_log(records.iter().map(|frame| &frame.record)) {
        return Err("compaction v2 requires a mode-3 session".into());
    }
    let mut state = State::default();
    for frame in records {
        if let WalRecord::RunStarted { config, manifest, compaction_plan_version, input, .. } = &frame.record {
            let version = compaction_plan_version.ok_or("compaction_plan_version is missing from a mode-3 run")?;
            check_plan_version(version)?;
            systems::frozen(true, config, manifest.as_deref())?;
            if input.iter().any(systems::is_global) { return Err("mode-3 RunStarted.input contains a global system prompt".into()); }
        }
        fold::fold(&mut state, &frame.record, session)?;
    }
    fold::validate_identity(&state, session)?;
    Ok(state)
}
