//! The scope carrier on the crash harness: the session's first scoped `RunStarted`, and the
//! migration of sessions recorded without a scope (owner-matched adoption), each checked at
//! every record boundary the harness can cut.
//!
//! - Both sides of the first scoped `RunStarted`: before it is durable nothing is frozen and
//!   the retry admits it again; after it the identity is frozen, and it stays frozen through
//!   the resume, in a gated frame requiring `scope/v1`.
//! - The migration test: an unscoped episode is cut and resumed at every boundary, then the
//!   scope's owner sends under its stamp. A session whose every episode recorded the owner
//!   adopts cleanly (and both sides of the adopting `RunStarted` are cut and resumed too); one
//!   whose episode recorded another principal is refused `principal_mismatch {recorded}`; one
//!   whose episode recorded no principal is refused `no_recorded_principal`. Each answer is
//!   checked on the send (the reply the serve layer returns) and on the bind (the adoption check
//!   a stamped `route.bind` gets), through the lineage registry the serve layer calls. The
//!   bytes those answers go out as are pinned through the serve path in broca-module-serve's
//!   `scope_carrier` tests. A refusal writes nothing.

use crate::scenarios;

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{
    decide_scope, run_loop_on_writer_admitted, AdmissionCheckpointV1, AdoptionRefusal, AllowAll,
    FileLeaseStore, FileWalStore, OutputEvent, RunControl, RunError, RunRequest, RunSummary,
    ScopeDecision, ScopeRefusal, SessionOwnershipV1, StartMode, StubProvider, StubToolPlane,
};
use broca_crash_harness::wal::FrameBody;
use broca_crash_harness::{
    drive, HarnessPlane, KillTrigger, LogImage, Render, RunnerHandle, Scenario, ScriptedProvider,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{SendRequest, SendResponse, ServeError, ServeHandler};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_protocol::{
    caller::RunFinishReason,
    content::{Message, Role},
    BindPrincipal, BindScope, RunId, SendScope, SessionScope,
};
use broca_wal::WalRecord;
use tokio::sync::mpsc;

use scenarios::{
    edit_plane, edit_request, empty_plane, one_edit_scripts, one_text_step_scripts, text,
};

fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn other() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "other-agent".into(),
    }
}

fn head_scope() -> SessionScope {
    SessionScope {
        owner: core(),
        scope_ref: "head-1".into(),
        scope_epoch: 3,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    }
}

fn stamped() -> SendScope {
    SendScope::Stamped(BindScope {
        scope: head_scope(),
        owner_authorized: true,
    })
}

fn default_control() -> RunControl {
    RunControl::default()
}

fn scoped_first_request() -> RunRequest {
    let mut request = edit_request();
    request.principal = Some(core());
    request.scope = Some(head_scope());
    request
}

fn recorded_request() -> RunRequest {
    let mut request = edit_request();
    request.principal = Some(core());
    request
}

fn unrecorded_request() -> RunRequest {
    edit_request()
}

fn other_principal_request() -> RunRequest {
    let mut request = edit_request();
    request.principal = Some(other());
    request
}

const fn one_edit(name: &'static str, request: fn() -> RunRequest) -> Scenario {
    Scenario {
        name,
        request,
        control: default_control,
        scripts: one_edit_scripts,
        render: Render::Canonical,
        plane: edit_plane,
        expected: RunFinishReason::Completed,
    }
}

const FIRST_SCOPED: Scenario = one_edit("first_scoped", scoped_first_request);
const RECORDED: Scenario = one_edit("unscoped_recorded", recorded_request);
const UNRECORDED: Scenario = one_edit("unscoped_unrecorded", unrecorded_request);
const OTHER_PRINCIPAL: Scenario = one_edit("unscoped_other_principal", other_principal_request);

/// The adopting episode, as the lineage registry admits it and as a resume re-drives it.
fn adopt_request() -> RunRequest {
    let mut request = edit_request();
    request.run_id = RunId("adopt".into());
    request.input = vec![Message::new(Role::User, vec![text("adopt the scope")])];
    request.tools = vec![];
    request.policy_allowed = Default::default();
    request.principal = Some(core());
    request.scope = Some(head_scope());
    request
}

const ADOPT: Scenario = Scenario {
    name: "adopt",
    request: adopt_request,
    control: default_control,
    scripts: one_text_step_scripts,
    render: Render::Canonical,
    plane: empty_plane,
    expected: RunFinishReason::Completed,
};

fn ownership(log: &LogImage) -> SessionOwnershipV1 {
    AdmissionCheckpointV1::from_frames(&log.framed())
        .expect("the log folds")
        .ownership
        .expect("a folded checkpoint carries the ownership summary")
}

/// Every `RunStarted` in `log` with its frame's version byte.
fn run_starts(log: &LogImage) -> Vec<(u8, Option<SessionScope>)> {
    log.frames
        .iter()
        .filter_map(|frame| match &frame.body {
            FrameBody::Record(framed) => match &framed.record {
                WalRecord::RunStarted { scope, .. } => {
                    Some((log.bytes[frame.start + 4], scope.clone()))
                }
                _ => None,
            },
            FrameBody::Lineage(_) => None,
        })
        .collect()
}

fn floor_of(state_root: &Path) -> Option<String> {
    std::fs::read_to_string(state_root.join(broca_wal::FEATURE_FLOOR_FILENAME)).ok()
}

#[tokio::test]
async fn both_sides_of_the_first_scoped_run_started_freeze_the_scope() {
    let handle = RunnerHandle::new(FIRST_SCOPED).await;
    assert_eq!(
        run_starts(&handle.reference.log),
        [(3, Some(head_scope()))],
        "the reference run freezes the scope in a gated frame"
    );
    for point in handle.record_boundaries(true) {
        let (kill, resumed) = handle
            .kill_resume_check(point.clone(), KillTrigger::Truncate)
            .await;
        let before = ownership(&kill.durable);
        let frozen_before = !run_starts(&kill.durable).is_empty();
        assert_eq!(
            before.scope,
            frozen_before.then(head_scope),
            "{}: frozen exactly when the scoped RunStarted is durable",
            kill.label()
        );
        assert_eq!(
            run_starts(&resumed.log),
            [(3, Some(head_scope()))],
            "{}: the resume ends with the one scoped RunStarted, gated",
            kill.label()
        );
        if !frozen_before {
            // The resume re-admitted the send and wrote the gated frame itself, so it raised
            // the feature floor in the state root first.
            assert_eq!(
                floor_of(&kill.state_root).as_deref(),
                Some("scope/v1\n"),
                "{}",
                kill.label()
            );
        }
        let after = ownership(&resumed.log);
        assert_eq!(
            decide_scope(&after, Some(&core()), &stamped()),
            Ok(ScopeDecision::Frozen(head_scope()))
        );
        assert!(matches!(
            decide_scope(&after, Some(&other()), &stamped()),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
    }
}

/// The real core loop with a one-step stub provider, for the episode a send adopts.
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
        let provider = StubProvider::new(one_text_step_scripts());
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

/// The lineage registry the serve layer calls, over a state root the harness left.
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

/// The owner's stamped send, exactly as the serve layer builds it from the bind.
fn owner_send() -> SendRequest {
    let adopt = adopt_request();
    let mut request = adopt.to_submission();
    request.scope = stamped();
    SendRequest {
        retention: None,
        stop_sequences_named: false,
        title: None,
        conversation_key: None,
        delivery: Default::default(),
        session: adopt.session,
        run_id: adopt.run_id,
        send_id: Some("adopt".into()),
        request: Box::new(request),
        skip_materialization: false,
        kind: RunKind::FreshOrResume,
        plan: None,
    }
}

async fn wait_for_records(path: &Path, finished: usize) -> LogImage {
    broca_test_support::wait_until(
        &format!("{finished} finished episodes at {}", path.display()),
        || async {
            let log = LogImage::read(path);
            let ended = log
                .records()
                .iter()
                .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
                .count();
            if ended >= finished {
                return Ok(log);
            }
            Err(format!("records={}, finished={ended}", log.records().len()))
        },
    )
    .await
}

/// The migration test: every cut of an unscoped episode, resumed, then the owner's stamped
/// send and bind, for each of the three histories.
#[tokio::test]
async fn the_migration_adopts_only_a_session_whose_every_episode_recorded_the_owner() {
    for (scenario, expected) in [
        (RECORDED, None),
        (
            OTHER_PRINCIPAL,
            Some(AdoptionRefusal::PrincipalMismatch { recorded: other() }),
        ),
        (UNRECORDED, Some(AdoptionRefusal::NoRecordedPrincipal)),
    ] {
        let handle = RunnerHandle::new(scenario).await;
        for point in handle.record_boundaries(true) {
            let (kill, resumed) = handle.kill_resume_check(point, KillTrigger::Truncate).await;
            let label = kill.label();
            let wal_path =
                broca_crash_harness::wal_path(&kill.state_root, &adopt_request().session);
            let before = std::fs::read(&wal_path).unwrap();
            assert_eq!(ownership(&resumed.log).scope, None, "{label}: unscoped");

            let registry = registry(&kill.state_root);
            let bind = registry
                .bind_adoption_refusal(
                    &adopt_request().session,
                    Some(&core()),
                    &BindScope {
                        scope: head_scope(),
                        owner_authorized: true,
                    },
                )
                .await;
            assert_eq!(bind, expected, "{label}: the bind");
            let sent = registry.send(owner_send()).await;
            match &expected {
                Some(refusal) => {
                    match sent {
                        Err(ServeError::ScopeRefused(ScopeRefusal::AdoptionRefused(got))) => {
                            assert_eq!(&got, refusal, "{label}: the send reply")
                        }
                        other => panic!("{label}: expected the adoption refusal, got {other:?}"),
                    }
                    // The same send meets the same refusal again.
                    assert!(matches!(
                        registry.send(owner_send()).await,
                        Err(ServeError::ScopeRefused(ScopeRefusal::AdoptionRefused(ref got))) if got == refusal
                    ));
                    assert_eq!(
                        std::fs::read(&wal_path).unwrap(),
                        before,
                        "{label}: a refused adoption writes nothing"
                    );
                }
                None => {
                    assert!(
                        matches!(sent, Ok(SendResponse::Active { .. })),
                        "{label}: the owner's send adopts: {sent:?}"
                    );
                    let adopted = wait_for_records(&wal_path, 2).await;
                    assert_eq!(
                        run_starts(&adopted),
                        [(1, None), (3, Some(head_scope()))],
                        "{label}: the adopting RunStarted freezes the scope, gated"
                    );
                    assert_eq!(
                        floor_of(&kill.state_root).as_deref(),
                        Some("scope/v1\n"),
                        "{label}"
                    );
                }
            }
            drop(registry);
        }

        if expected.is_none() {
            both_sides_of_the_adopting_run_started(&handle).await;
        }
    }
}

/// Cut the adopted log just before and just after the adopting `RunStarted` (whole, and as a
/// torn append of it), and resume each: before its sync nothing is frozen and the re-driven
/// send admits it again; after it the scope is frozen and the resume keeps it.
async fn both_sides_of_the_adopting_run_started(handle: &RunnerHandle) {
    let kill = handle
        .kill_at(
            broca_crash_harness::KillPoint::RecordBoundary {
                records: handle.reference.log.record_count(),
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let registry = registry(&kill.state_root);
    let wal_path = broca_crash_harness::wal_path(&kill.state_root, &adopt_request().session);
    assert!(matches!(
        registry.send(owner_send()).await,
        Ok(SendResponse::Active { .. })
    ));
    let adopted = wait_for_records(&wal_path, 2).await;
    drop(registry);
    let first_episode = handle.reference.log.record_count();

    for (records, torn) in [
        (first_episode, false),
        (first_episode, true),
        (first_episode + 1, false),
    ] {
        let root = kill.state_root.with_file_name(format!(
            "adopt-cut-{records}-{}",
            if torn { "torn" } else { "whole" }
        ));
        let _ = std::fs::remove_dir_all(&root);
        let mut end = adopted.end_of_records(records);
        if torn {
            end += (adopted.end_of_records(records + 1) - end) / 2;
        }
        let path = broca_crash_harness::wal_path(&root, &adopt_request().session);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &adopted.bytes[..end]).unwrap();
        let cut = LogImage::read(&path);
        let frozen = records > first_episode;
        assert_eq!(
            ownership(&cut).scope,
            frozen.then(head_scope),
            "cut at {records} (torn {torn}): frozen only once the adopting RunStarted is durable"
        );

        let provider = ScriptedProvider::for_scenario(&ADOPT);
        let plane = HarnessPlane::for_scenario(&ADOPT);
        let store = FileWalStore::new(root.join("wal"));
        let outcome = drive(
            adopt_request(),
            default_control(),
            &root,
            &store,
            &provider,
            &plane,
        )
        .await;
        assert_eq!(outcome.reason, RunFinishReason::Completed);
        let resumed = LogImage::read(&path);
        assert_eq!(
            run_starts(&resumed),
            [(1, None), (3, Some(head_scope()))],
            "cut at {records} (torn {torn}): one adopting RunStarted, gated, after the resume"
        );
        assert_eq!(
            decide_scope(&ownership(&resumed), Some(&core()), &stamped()),
            Ok(ScopeDecision::Frozen(head_scope()))
        );
    }
}
