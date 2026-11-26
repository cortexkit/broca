//! Runs requests through the session loop production runs, on a session kept in memory, for the
//! provider tests that drive a provider through whole runs: its unit tests, the mock-server
//! end-to-end tests and the live-provider suite all include this file. Each run's output is the
//! loop's display events followed by the control units serve projects from the run's records.

use broca_core::{tool_plane::ToolPlane, AllowAll, OutputEvent, RunRequest};
use broca_protocol::BindIdentity;
use broca_session_loop::writer::Writer as _;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::mpsc;

/// One stored session; each [`RigSession::run`] is the session's next run, as each send to one
/// session is.
pub struct RigSession {
    store: Session<MemoryBackend>,
    parts: broca_session_loop::parts::MemoryParts,
    /// Cancels the session's runs, as a caller's `run.cancel` does.
    pub cancel: tokio_util::sync::CancellationToken,
    /// When set, the next run pauses for sign-in before its first request, as one whose account
    /// is known to be signed out does.
    pub pause_for_auth_at_start: Option<broca_protocol::ProviderError>,
}

/// How one run ended, and the session's records once it had.
pub struct RigRun {
    pub finished: broca_session_loop::Finished,
    /// The model steps this run completed.
    pub steps: u64,
    /// Every record of the session's log, this run's and earlier runs'.
    pub records: Vec<Record>,
}

impl RigSession {
    /// A new, empty session for `bind`.
    pub async fn new(bind: &BindIdentity) -> Self {
        let store = Session::create(
            MemoryBackend::default(),
            SessionAddr("0123456789abcdef".into()),
            SessionTriple {
                project_root: bind.project_root.display().to_string(),
                harness: bind.harness.clone(),
                session: bind.session.clone(),
            },
            &DeploymentKey::new(1, &[7; 32]),
            false,
        )
        .await
        .expect("the session is created");
        Self {
            store,
            parts: Default::default(),
            cancel: Default::default(),
            pause_for_auth_at_start: None,
        }
    }

    /// Admits and runs `req` with `provider` and `plane`, sending its output events to `out`.
    pub async fn run<P: broca_core::Provider, T: ToolPlane>(
        &mut self,
        req: &mut RunRequest,
        provider: &P,
        plane: &T,
        out: &mpsc::Sender<OutputEvent>,
    ) -> RigRun {
        self.try_run(req, provider, plane, out)
            .await
            .expect("the run is admitted")
    }

    /// As [`Self::run`], with admission's refusal returned rather than a panic.
    pub async fn try_run<P: broca_core::Provider, T: ToolPlane>(
        &mut self,
        req: &mut RunRequest,
        provider: &P,
        plane: &T,
        out: &mpsc::Sender<OutputEvent>,
    ) -> Result<RigRun, broca_core::RunError> {
        let run_id = req.run_id.0.clone();
        let before = self
            .store
            .open_log_records()
            .await
            .expect("the session's records read")
            .len();
        let admission = broca_session_loop::admit::admission(
            req,
            &broca_core::RunControl::default(),
            provider,
            self.store.operations(),
            Some(1),
        )?;
        let now = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_millis() as u64)
        };
        let seams = broca_session_loop::Seams {
            steer_yields: None,
            provider,
            tools: plane,
            bind: req.session.clone(),
            out: Some(out),
            cancel: self.cancel.clone(),
            stop: Default::default(),
            held: None,
            approval: &AllowAll,
            hook_plane: None,
            now_ms: &now,
            session_address: None,
            compaction_plane: None,
            compaction_session: None,
            compaction_changes: false,
            keep_warm: None,
            account_reports: None,
            max_same_account_wait: Default::default(),
            auth_selection: None,
            step_progress: None,
            accepting_drain: None,
            model_call_ready: None,
            pause_for_auth_at_start: self.pause_for_auth_at_start.take(),
            account_continue: None,
        };
        let finished = broca_session_loop::run(&mut self.store, &mut self.parts, admission, &seams)
            .await
            .expect("the run loop should not fail at the storage level");
        let framed = self
            .store
            .open_log_records()
            .await
            .expect("the session's records read");
        // The session loop sends only display events on `out`; a subscriber's control units
        // come from the session's records. Project them as serve does and send this run's.
        let mut control = broca_module::store_control::StoreControl::default();
        for (index, (seq, record)) in framed.iter().enumerate() {
            let next = framed.get(index + 1).map(|(_, record)| record);
            let units = control
                .project(*seq, record, next)
                .expect("the session's records project");
            if index >= before {
                for unit in units.into_iter().filter(|unit| unit.visible) {
                    let _ = out.send(OutputEvent::Control(unit.unit)).await;
                }
            }
        }
        let records: Vec<Record> = framed.into_iter().map(|(_, record)| record).collect();
        let steps = records
            .iter()
            .filter(|record| matches!(record, Record::StepEnd(end) if end.run_id == run_id))
            .count() as u64;
        Ok(RigRun {
            finished,
            steps,
            records,
        })
    }
}

/// Runs `req` as the first run of a new session, collecting nothing from its output.
pub async fn run_once<P: broca_core::Provider, T: ToolPlane>(
    mut req: RunRequest,
    provider: &P,
    plane: &T,
) -> RigRun {
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut session = RigSession::new(&req.session).await;
    let run = session.run(&mut req, provider, plane, &tx).await;
    drop(tx);
    drain.await.unwrap();
    run
}
