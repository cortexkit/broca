//! A session admitted with a fetch plan, cut after its first `RunStarted` at every record
//! boundary, reopened through the lineage registry the serve layer calls, and sent a later
//! plain message.
//!
//! On reopen the actor re-drives the unfinished first episode from its own record (whose input
//! holds the manifest's system text and whose tools are the manifest's) and then starts the
//! later message as a new episode. Both must be admitted: the re-drive as the record left it,
//! and the new episode with the frozen manifest's tools, the same manifest on its `RunStarted`,
//! and no second system message.

use crate::scenarios;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunError,
    RunSummary, StartMode, StubProvider, StubToolPlane,
};
use broca_crash_harness::{KillPoint, KillTrigger, LogImage, RunnerHandle};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{SendRequest, ServeHandler};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_protocol::{
    content::{Message, Role},
    RunId,
};
use broca_wal::WalRecord;
use tokio::sync::mpsc;

/// The real run loop with a provider that answers text and a plane that serves `edit`.
struct CoreExec {
    wal_store: Arc<FileWalStore>,
}

#[async_trait]
impl RunExecutor for CoreExec {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        let provider = StubProvider::new(scenarios::one_text_step_scripts());
        run_loop_on_writer_admitted(
            job.req,
            &job.writer,
            mode,
            &provider,
            &StubToolPlane::new(),
            &AllowAll,
            self.wal_store.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
        )
        .await
    }
}

fn registry(state_root: &Path) -> LineageRegistry {
    let wal = Arc::new(FileWalStore::new(state_root.join("wal")));
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(state_root.join("registry-leases"))),
        wal.clone(),
        Arc::new(CoreExec {
            wal_store: wal.clone(),
        }),
    );
    LineageRegistry::new_with_reader(Arc::new(opener), wal, None)
}

/// A later plain send: a new message, leaving tools and system text to the session.
fn later_send() -> SendRequest {
    let mut request = scenarios::planned_request();
    request.run_id = RunId("later".into());
    request.input = vec![Message::new(Role::User, vec![scenarios::text("and then")])];
    let mut submission = request.to_submission();
    submission.tools = None;
    submission.policy_allowed = Default::default();
    SendRequest {
        session_class: None,
        retention: None,
        stop_sequences_named: false,
        title: None,
        conversation_key: None,
        delivery: Default::default(),
        session: request.session,
        run_id: request.run_id,
        send_id: Some("later".into()),
        request: Box::new(submission),
        skip_materialization: false,
        kind: RunKind::FreshOrResume,
        plan: None,
        order: None,
    }
}

async fn wait_for_finished(path: &Path, finished: usize) -> Vec<WalRecord> {
    broca_test_support::wait_until(
        &format!("{finished} finished episodes at {}", path.display()),
        || async {
            let records = LogImage::read(path).records();
            let ended = records
                .iter()
                .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                .count();
            if ended >= finished {
                return Ok(records);
            }
            Err(format!("records={}, finished={ended}", records.len()))
        },
    )
    .await
}

#[tokio::test]
async fn a_reopened_planned_session_redrives_its_first_episode_and_admits_a_later_one() {
    let handle = RunnerHandle::new(scenarios::PLANNED_EDITS).await;
    let manifest = scenarios::planned_manifest();
    let total = handle.reference.log.record_count();
    for records in 1..total {
        let kill = handle
            .kill_at(
                KillPoint::RecordBoundary {
                    records,
                    torn: false,
                },
                KillTrigger::Truncate,
            )
            .await;
        let registry = registry(&kill.state_root);
        registry
            .send(later_send())
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", kill.label()));
        let path = FileWalStore::new(kill.state_root.join("wal")).wal_path(&scenarios::session());
        let records = wait_for_finished(&path, 2).await;
        let started: Vec<&WalRecord> = records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
            .collect();
        assert_eq!(started.len(), 2, "{}", kill.label());
        let WalRecord::RunStarted {
            manifest: Some(later_manifest),
            config,
            input,
            ..
        } = started[1]
        else {
            panic!("{}: the later episode carries the manifest", kill.label());
        };
        assert_eq!(**later_manifest, manifest, "{}", kill.label());
        assert_eq!(
            config.tools.as_ref(),
            manifest.tools.as_ref(),
            "{}",
            kill.label()
        );
        assert!(
            input.iter().all(|message| message.role != Role::System),
            "{}: the system text renders once, from the first episode",
            kill.label()
        );
    }
}
