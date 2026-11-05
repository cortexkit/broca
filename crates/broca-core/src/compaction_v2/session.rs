//! Session-owned memory shared by run boundaries and pushed-view admission.

use broca_protocol::BindIdentity;
use broca_wal::{FramedRecord, WalRecord};

use super::{conditions::check_plan_version, fold, runtime::Runtime, state::State, systems};

#[async_trait::async_trait]
pub trait Changes: Send + Sync {
    async fn changed(&self) -> Result<(), String>;
}

pub struct BoundChanges {
    pub session: std::sync::Arc<Session>,
    pub writer: std::sync::Arc<crate::WalHandle>,
    pub wal: std::sync::Arc<dyn crate::WalStore>,
    pub bind: BindIdentity,
    pub scope: Option<broca_protocol::SessionScope>,
    pub plane: Option<std::sync::Arc<dyn crate::CompactionPlane>>,
}

#[async_trait::async_trait]
impl Changes for BoundChanges {
    async fn changed(&self) -> Result<(), String> {
        super::host::notice(
            &self.session,
            &self.writer,
            Some(self.wal.as_ref()),
            &self.bind,
            self.scope.as_ref(),
            self.plane.as_deref(),
            // A pushed view arrives outside a run, without the run's cache declaration. When
            // this notice is the one that sets the provider up, its Setup lists no
            // `cache_expired`, and rides never claim it until the next Setup.
            None,
        )
        .await
    }
}

#[derive(Default)]
pub struct Session {
    pub state: tokio::sync::Mutex<State>,
    pub runtime: Runtime,
    initialized: std::sync::atomic::AtomicBool,
    serving_started: std::sync::atomic::AtomicBool,
}

impl Session {
    pub fn claim_serving_start(&self) -> bool {
        !self
            .serving_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    }
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(std::sync::atomic::Ordering::Acquire)
    }
    pub async fn initialize(
        &self,
        session: &BindIdentity,
        records: &[FramedRecord],
    ) -> Result<(), String> {
        let mut state = self.state.lock().await;
        if !self.initialized.load(std::sync::atomic::Ordering::Acquire) {
            *state = if records
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            {
                load(session, records)?
            } else {
                State::default()
            };
            self.runtime.restore_marker(state.marker_version());
            self.initialized
                .store(true, std::sync::atomic::Ordering::Release);
        }
        fold::validate_identity(&state, session)
    }
    /// The actor reloads at its serialized admission point, before spawning a
    /// run. Never replace this state from a stale snapshot while that run is active.
    pub async fn restore(&self, state: State) {
        self.runtime.restore_marker(state.marker_version());
        *self.state.lock().await = state;
        self.initialized
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

pub fn load(session: &BindIdentity, records: &[FramedRecord]) -> Result<State, String> {
    if !crate::is_mode3_log(records.iter().map(|frame| &frame.record)) {
        return Err("compaction v2 requires a mode-3 session".into());
    }
    let mut state = State::default();
    let mut active_version = None;
    for frame in records {
        if let WalRecord::RunStarted {
            config,
            manifest,
            compaction_plan_version,
            input,
            ..
        } = &frame.record
        {
            let version = compaction_plan_version
                .ok_or("compaction_plan_version is missing from a mode-3 run")?;
            active_version = Some(version);
            systems::frozen(true, config, manifest.as_deref())?;
            if input.iter().any(systems::is_global) {
                return Err("mode-3 RunStarted.input contains a global system prompt".into());
            }
        }
        if matches!(frame.record, WalRecord::RunFinished { .. }) {
            active_version = None;
        }
        fold::fold(&mut state, &frame.record, session)?;
    }
    if let Some(version) = active_version {
        check_plan_version(version)?;
    }
    fold::validate_identity(&state, session)?;
    Ok(state)
}
