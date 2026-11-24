use broca_protocol::{PauseReason, RunFinishReason};
use broca_session_store::record::{AuthRefrozen, Pause, Record, RunEnd, RunStart, StepStart};
use serde_json::json;

use super::*;

fn recorder() -> (Recorder, &'static Ring<RunOutcome>) {
    let target: &'static Ring<RunOutcome> = Box::leak(Box::new(Ring::new(0)));
    let recorder = Recorder {
        target,
        now_ms: || 42,
        run: None,
        unsynced: Vec::new(),
    };
    (recorder, target)
}

fn start(run: &str, seeded: bool) -> Record {
    Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        render_identity: None,
        message_marks: None,
        named_speaker: None,
        kept_notice: None,
        run_id: run.into(),
        model: serde_json::from_value(json!({"provider_module_id": "anthropic", "model_id": "m"}))
            .unwrap(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: Some(1),
        written_at_ms: Some(1),
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: Some(json!({"credential_id": "oauth:anthropic:acct1"})),
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: seeded.then_some(true),
        refusal_schedule: None,
    }))
}

fn pause(run: &str, reason: PauseReason, ts_ms: u64) -> Record {
    Record::Pause(Pause {
        run_id: run.into(),
        reason,
        ts_ms: Some(ts_ms),
        error: None,
        account_limit: None,
    })
}

fn step(run: &str) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: 2,
        mid: None,
    })
}

fn end(run: &str, reason: RunFinishReason, error: Option<serde_json::Value>) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason,
        usage: None,
        ts_ms: Some(9),
        retries_used: 0,
        interrupt_reason: (reason == RunFinishReason::Interrupted).then(|| "tool_outage".into()),
        error: error.map(|error| broca_protocol::json::Json::from_value(&error)),
    })
}

fn durable(recorder: &mut Recorder, records: &[Record]) {
    recorder.appended(Appended::Records {
        first: 1,
        records,
        durable: true,
    });
}

fn outcomes(ring: &Ring<RunOutcome>) -> Vec<RunOutcome> {
    ring.snapshot(0, i64::MAX).unwrap().events
}

fn outcome(at_ms: i64, run: &str, credential: &str, kind: OutcomeKind) -> RunOutcome {
    RunOutcome {
        at_ms,
        run_id: run.into(),
        provider: Some("anthropic".into()),
        model: Some("m".into()),
        credential: Some(credential.into()),
        kind,
    }
}

/// A run's error end carries its provider and model, its credential (the one an account move
/// chose, once there is one), the error's class, provider code and HTTP status, and the last step
/// the run started.
#[test]
fn an_error_end_names_the_provider_model_credential_class_code_status_and_step() {
    let (mut recorder, ring) = recorder();
    let error = json!({"class": "permanent", "message": "no credit",
        "provider_code": "billing_exhausted", "status": 402});
    durable(
        &mut recorder,
        &[
            start("r1", false),
            Record::AuthRefrozen(AuthRefrozen {
                run_id: "r1".into(),
                ts_ms: None,
                auth_selection: json!({"credential_id": "oauth:anthropic:acct2"}),
            }),
            step("r1"),
            end("r1", RunFinishReason::Error, Some(error)),
        ],
    );
    assert_eq!(
        outcomes(ring),
        [outcome(
            9,
            "r1",
            "oauth:anthropic:acct2",
            OutcomeKind::Error {
                class: Some("permanent".into()),
                code: Some("billing_exhausted".into()),
                status: Some(402),
                step: Some(2),
            },
        )]
    );
}

/// A reported pause, then the step it resumes with, then a completed end: the pause and its
/// clearing are reported, the completion itself is not. A restart pause is never reported.
#[test]
fn a_sign_in_pause_is_cleared_by_its_resume_and_a_restart_pause_is_not_reported() {
    let (mut recorder, ring) = recorder();
    durable(
        &mut recorder,
        &[
            start("r1", false),
            pause("r1", PauseReason::AuthRequired, 5),
            step("r1"),
            end("r1", RunFinishReason::Completed, None),
            start("r2", false),
            pause("r2", PauseReason::Restart, 6),
            end("r2", RunFinishReason::Completed, None),
        ],
    );
    let acct = "oauth:anthropic:acct1";
    assert_eq!(
        outcomes(ring),
        [
            outcome(
                5,
                "r1",
                acct,
                OutcomeKind::Paused {
                    reason: PauseReason::AuthRequired
                }
            ),
            outcome(42, "r1", acct, OutcomeKind::Resumed),
        ]
    );
}

/// An account-limit pause that ends the run another way clears too; an interrupted end carries
/// its reason.
#[test]
fn an_account_limit_pause_cleared_by_its_end_and_an_interruption_with_its_reason() {
    let (mut recorder, ring) = recorder();
    durable(
        &mut recorder,
        &[
            start("r1", false),
            pause("r1", PauseReason::AccountLimit, 5),
            end("r1", RunFinishReason::Cancelled, None),
            start("r2", false),
            end("r2", RunFinishReason::Interrupted, None),
        ],
    );
    let kinds: Vec<_> = outcomes(ring)
        .into_iter()
        .map(|o| (o.run_id, o.kind))
        .collect();
    assert_eq!(
        kinds,
        [
            (
                "r1".to_owned(),
                OutcomeKind::Paused {
                    reason: PauseReason::AccountLimit
                }
            ),
            ("r1".to_owned(), OutcomeKind::Resumed),
            (
                "r2".to_owned(),
                OutcomeKind::Interrupted {
                    reason: Some("tool_outage".into())
                }
            ),
        ]
    );
}

/// Nothing is reported before a barrier makes it durable, a revert drops what wasn't yet, and a
/// seeded run (an import or a fork) reports nothing.
#[test]
fn outcomes_wait_for_durability_and_seeded_runs_report_nothing() {
    let (mut recorder, ring) = recorder();
    recorder.appended(Appended::Records {
        first: 1,
        records: &[start("r1", false), end("r1", RunFinishReason::Error, None)],
        durable: false,
    });
    assert!(outcomes(ring).is_empty(), "nothing is durable yet");
    recorder.appended(Appended::Replaced);
    recorder.appended(Appended::Synced);
    assert!(outcomes(ring).is_empty(), "the revert took the end back");
    durable(
        &mut recorder,
        &[start("r2", true), end("r2", RunFinishReason::Error, None)],
    );
    assert!(outcomes(ring).is_empty(), "a seeded run has no outcomes");
    recorder.appended(Appended::Records {
        first: 5,
        records: &[start("r3", false), end("r3", RunFinishReason::Error, None)],
        durable: false,
    });
    recorder.appended(Appended::Synced);
    assert_eq!(
        outcomes(ring),
        [outcome(
            9,
            "r3",
            "oauth:anthropic:acct1",
            OutcomeKind::Error {
                class: None,
                code: None,
                status: None,
                step: None,
            },
        )]
    );
}

async fn session() -> Session<&'static broca_session_store::backend::MemoryBackend> {
    let backend: &'static broca_session_store::backend::MemoryBackend = Box::leak(Box::default());
    Session::create(
        backend,
        broca_session_store::backend::SessionAddr("0123456789abcdef".into()),
        broca_session_store::head::SessionTriple {
            project_root: "/project".into(),
            harness: "run-outcomes".into(),
            session: "s1".into(),
        },
        &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap()
}

/// A session reopened with its run paused for sign-in reports that pause, dated by its pause
/// record, and its observer knows the run: the resume clears the pause and names the run's
/// provider and credential, which no record after the open carries.
#[tokio::test]
async fn a_pause_from_before_the_open_is_reported_and_cleared_by_its_resume() {
    let mut session = session().await;
    session
        .append(
            &[
                start("r1", false),
                pause("r1", PauseReason::AuthRequired, 5),
            ],
            true,
        )
        .await
        .unwrap();
    let acct = "oauth:anthropic:acct1";
    assert_eq!(
        paused_at_open(&session).await.unwrap(),
        Some(outcome(
            5,
            "r1",
            acct,
            OutcomeKind::Paused {
                reason: PauseReason::AuthRequired
            }
        ))
    );
    let ring = observe_for_test(&mut session);
    session.append(&[step("r1")], true).await.unwrap();
    assert_eq!(
        outcomes(ring),
        [outcome(1, "r1", acct, OutcomeKind::Resumed)]
    );
}

/// A session whose latest run ended, or paused for a restart, has no pause to report at open.
#[tokio::test]
async fn only_a_sign_in_or_account_limit_pause_is_reported_at_open() {
    let mut session = session().await;
    session
        .append(
            &[
                start("r1", false),
                end("r1", RunFinishReason::Completed, None),
            ],
            true,
        )
        .await
        .unwrap();
    assert_eq!(paused_at_open(&session).await.unwrap(), None);
    session
        .append(
            &[start("r2", false), pause("r2", PauseReason::Restart, 6)],
            true,
        )
        .await
        .unwrap();
    assert_eq!(paused_at_open(&session).await.unwrap(), None);
}
