//! The resume close-out, on the shared harness.
//!
//! A resume that finds a tool call whose intent is durable and whose result is not (it may or
//! may not have run) never sends it again and makes no model call. It writes an
//! `outcome_unknown` result for it, commits the batch, and seals the episode Interrupted. The
//! next send to the session (a new episode with a new run id and input) then renders the call
//! paired with that result.

mod scenarios;

use broca_core::{
    outcome_unknown_result, reconcile_run_state, FileWalStore, RunControl, RunIndexState,
};
use broca_crash_harness::{
    drive, HarnessPlane, InFlight, KillPoint, KillReport, KillTrigger, LogImage, Render,
    RunnerHandle, ScriptedProvider, Suite,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    FinishReason, MessageId, RunId, ToolCallId, Usage,
};
use broca_wal::{
    head_proof::{sidecar_path, HeadProof, Publisher, SyncedPrefix},
    DispatchOutcome, FileWal, Wal, WalConfig, WalRecord,
};

broca_crash_harness::crash_harness_child!(scenarios::by_name);

fn t(id: &str) -> ToolCallId {
    ToolCallId(id.into())
}

fn recovery_text_provider() -> ScriptedProvider {
    ScriptedProvider::new(
        vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("after-recovery".into()),
                    content: vec![ContentBlock::Text {
                        text: "continued".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]],
        Render::Canonical,
    )
}

/// The record count just after the dispatch intent of `call` in the reference log.
fn after_intent_of(handle: &RunnerHandle, call: &str) -> usize {
    handle
        .reference
        .log
        .records()
        .iter()
        .position(|record| {
            matches!(record, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == call)
        })
        .expect("the reference run records the intent")
        + 1
}

async fn kill_after_intent_of(handle: &RunnerHandle, call: &str) -> KillReport {
    handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: after_intent_of(handle, call),
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await
}

/// Each tool result written after the first `from` records: (call, outcome).
fn results_after(records: &[WalRecord], from: usize) -> Vec<(String, DispatchOutcome)> {
    records[from..]
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolResult {
                result, outcome, ..
            } => Some((result.tool_call_id.0.clone(), *outcome)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_fsynced_intent_beyond_a_stale_head_proof_survives_open_and_resume_never_resends() {
    let handle = RunnerHandle::new(scenarios::ONE_EDIT).await;
    let kill = kill_after_intent_of(&handle, "t1").await;
    let prefix = kill.durable.records();
    let path = broca_crash_harness::wal_path(&kill.state_root, &scenarios::session());

    // Keep the reference lineage while replacing its legacy records with v4.
    std::fs::write(&path, &kill.durable.bytes[..kill.durable.end_of_records(0)]).unwrap();
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(kill.state_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
        .with_wal_config(WalConfig::OVERHAUL)
        .unwrap();
    let (intent, before_intent) = prefix.split_last().unwrap();
    assert!(
        matches!(intent, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id == &t("t1"))
    );
    for record in before_intent {
        wal.append(1, record, false).unwrap();
    }
    let mut publisher = Publisher::from_synced(SyncedPrefix::sync(&wal).unwrap()).unwrap();
    publisher.publish(|| 0).unwrap();
    let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
    let proof = HeadProof::decode(&sidecar).unwrap();

    // Dispatch is allowed after this fsync, before a newer proof is published.
    let intent_seq = wal.append(1, intent, true).unwrap();
    assert!(intent_seq > proof.durable_seq);
    let through_intent = std::fs::read(&path).unwrap();
    let reference = handle.reference.log.records();
    wal.append(1, &reference[prefix.len()], false).unwrap();
    wal.append(1, &reference[prefix.len() + 1], false).unwrap();
    drop(wal);
    let mut damaged = std::fs::read(&path).unwrap();
    let pending_end = LogImage::parse(damaged.clone())
        .record_frame(prefix.len())
        .unwrap()
        .end;
    damaged[pending_end - 1] ^= 1;
    std::fs::write(&path, damaged).unwrap();

    let reopened = FileWal::open(&path).unwrap();
    assert_eq!(reopened.last_seq(), intent_seq + 1);
    assert!(std::fs::read(&path).unwrap().starts_with(&through_intent));
    assert_eq!(std::fs::read(sidecar_path(&path)).unwrap(), sidecar);
    assert_eq!(
        reopened
            .replay()
            .unwrap()
            .into_iter()
            .filter(|f| !matches!(f.record, WalRecord::RecoveryCut { .. }))
            .map(|f| f.record)
            .collect::<Vec<_>>(),
        prefix
    );
    drop(reopened);

    let store = FileWalStore::with_config(kill.state_root.join("wal"), WalConfig::OVERHAUL);
    let provider = ScriptedProvider::for_scenario(&handle.scenario);
    let plane = HarnessPlane::for_scenario(&handle.scenario);
    let outcome = drive(
        (handle.scenario.request)(),
        (handle.scenario.control)(),
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::Interrupted);
    assert_eq!(provider.request_count(), 0);
    assert!(plane.dispatched().is_empty());
    let records = LogImage::read(&path).records();
    assert_eq!(
        results_after(&records, prefix.len()),
        vec![("t1".into(), DispatchOutcome::Unknown)]
    );
    let status = reconcile_run_state(&records, "r").unwrap();
    assert_eq!(status.state, RunIndexState::Interrupted);
    assert_eq!(status.indeterminate_tool_calls, Some(vec![t("t1")]));
}

#[tokio::test]
async fn a_removed_intent_is_unknown_on_real_resume_and_the_next_episode_runs() {
    for remove_step in [false, true] {
        let handle = RunnerHandle::new(scenarios::ONE_EDIT).await;
        let kill = kill_after_intent_of(&handle, "t1").await;
        let path = broca_crash_harness::wal_path(&kill.state_root, &scenarios::session());
        std::fs::write(&path, &kill.durable.bytes[..kill.durable.end_of_records(0)]).unwrap();
        let floor = kill.state_root.join(broca_wal::FEATURE_FLOOR_FILENAME);
        let mut wal = FileWal::open_with_recovery(&path, WalConfig::OVERHAUL, floor.clone())
            .unwrap()
            .with_feature_floor(floor)
            .with_wal_config(WalConfig::OVERHAUL)
            .unwrap();
        let reference = handle.reference.log.records();
        for record in &reference {
            wal.append(1, record, true).unwrap();
        }
        let mut publisher = Publisher::from_synced(SyncedPrefix::sync(&wal).unwrap()).unwrap();
        publisher.publish(|| 0).unwrap();
        drop(wal);
        let mut bytes = std::fs::read(&path).unwrap();
        let target = reference
            .iter()
            .position(|record| {
                if remove_step {
                    matches!(record, WalRecord::ModelStepFinished { .. })
                } else {
                    matches!(record, WalRecord::ToolDispatchIntent { .. })
                }
            })
            .unwrap();
        let frame = LogImage::parse(bytes.clone())
            .record_frame(target)
            .unwrap()
            .clone();
        bytes[frame.end - 1] ^= 1;
        let removed = bytes[frame.start..].to_vec();
        std::fs::write(&path, bytes).unwrap();

        let store = FileWalStore::with_config(kill.state_root.join("wal"), WalConfig::OVERHAUL);
        let provider = ScriptedProvider::for_scenario(&handle.scenario);
        let plane = HarnessPlane::for_scenario(&handle.scenario);
        let outcome = drive(
            (handle.scenario.request)(),
            (handle.scenario.control)(),
            &kill.state_root,
            &store,
            &provider,
            &plane,
        )
        .await;
        assert_eq!(outcome.reason, RunFinishReason::Interrupted);
        assert_eq!(provider.request_count(), 0);
        assert!(plane.dispatched().is_empty());
        let records = LogImage::read(&path).records();
        let recovery = records
            .iter()
            .find_map(|r| match r {
                WalRecord::RecoveryCut { recovery } => Some(recovery),
                _ => None,
            })
            .unwrap();
        assert!(recovery.unattributed_tool_risk);
        assert!(recovery
            .runs
            .iter()
            .flat_map(|run| &run.calls)
            .any(|call| call.tool_call_id == t("t1")));
        assert_eq!(
            std::fs::read(path.parent().unwrap().join(&recovery.sidecar_basename)).unwrap(),
            removed
        );
        assert!(records.iter().any(|r| matches!(r, WalRecord::ResumeStarted { indeterminate, .. } if indeterminate == &vec![t("t1")])));
        let unknown: Vec<_> = records
            .iter()
            .filter_map(|r| match r {
                WalRecord::ToolResult {
                    result,
                    outcome: DispatchOutcome::Unknown,
                    ..
                } => Some(result.as_ref()),
                _ => None,
            })
            .collect();
        assert_eq!(unknown, [&outcome_unknown_result(&t("t1"))]);
        let state = reconcile_run_state(&records, "r").unwrap();
        assert_eq!(state.indeterminate_tool_calls, Some(vec![t("t1")]));

        let mut next = scenarios::edit_request();
        next.run_id = RunId("after-cut".into());
        next.input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue after reviewing recovery".into(),
            }],
        )];
        let provider = ScriptedProvider::new(
            vec![vec![
                NormalizedEvent::Control(ProviderControl::AssistantMessage {
                    message: AssistantMessage {
                        message_id: MessageId("after-cut-message".into()),
                        content: vec![ContentBlock::Text {
                            text: "continued".into(),
                        }],
                    },
                }),
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage: Usage::default(),
                    finish_reason: FinishReason::Stop,
                    retries_used: 0,
                }),
            ]],
            Render::Canonical,
        );
        let plane = HarnessPlane::new(scenarios::edit_plane());
        let next = drive(
            next,
            RunControl::default(),
            &kill.state_root,
            &store,
            &provider,
            &plane,
        )
        .await;
        assert_eq!(next.reason, RunFinishReason::Completed);
        assert_eq!(provider.request_count(), 1);
        assert!(provider.rejected().is_empty());
        assert!(plane.dispatched().is_empty());

        // Even a later episode re-emitting the recovered call id cannot send it.
        let mut repeated = (handle.scenario.request)();
        repeated.run_id = RunId("repeat-recovered-id".into());
        let provider = ScriptedProvider::for_scenario(&handle.scenario);
        let plane = HarnessPlane::for_scenario(&handle.scenario);
        let repeated = drive(
            repeated,
            RunControl::default(),
            &kill.state_root,
            &store,
            &provider,
            &plane,
        )
        .await;
        assert_eq!(repeated.reason, RunFinishReason::Interrupted);
        assert!(plane.dispatched().is_empty());
        let records = LogImage::read(&path).records();
        assert_eq!(
            reconcile_run_state(&records, "repeat-recovered-id")
                .unwrap()
                .indeterminate_tool_calls,
            Some(vec![t("t1")])
        );
    }
}

#[tokio::test]
async fn unattributed_recovery_risk_interrupts_only_the_current_run() {
    let handle = RunnerHandle::new(scenarios::ONE_EDIT).await;
    let kill = kill_after_intent_of(&handle, "t1").await;
    let path = broca_crash_harness::wal_path(&kill.state_root, &scenarios::session());
    std::fs::write(&path, &kill.durable.bytes[..kill.durable.end_of_records(0)]).unwrap();
    let floor = kill.state_root.join(broca_wal::FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open_with_recovery(&path, WalConfig::OVERHAUL, floor.clone())
        .unwrap()
        .with_feature_floor(floor)
        .with_wal_config(WalConfig::OVERHAUL)
        .unwrap();
    let start = handle
        .reference
        .log
        .records()
        .into_iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    wal.append(1, &start, true).unwrap();
    drop(wal);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"unattributed undecodable bytes")
        .unwrap();
    let store = FileWalStore::with_config(kill.state_root.join("wal"), WalConfig::OVERHAUL);
    let provider = ScriptedProvider::for_scenario(&handle.scenario);
    let plane = HarnessPlane::for_scenario(&handle.scenario);
    let outcome = drive(
        (handle.scenario.request)(),
        RunControl::default(),
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::Interrupted);
    assert_eq!(provider.request_count(), 0);
    assert!(plane.dispatched().is_empty());
    let records = LogImage::read(&path).records();
    assert!(records.iter().any(|record| matches!(record, WalRecord::RecoveryCut { recovery } if recovery.unattributed_tool_risk && recovery.runs.is_empty())));
    assert_eq!(
        reconcile_run_state(&records, "r")
            .unwrap()
            .indeterminate_tool_calls,
        Some(vec![])
    );
    let mut next = scenarios::edit_request();
    next.run_id = RunId("after-unattributed-cut".into());
    let provider = recovery_text_provider();
    let plane = HarnessPlane::new(scenarios::edit_plane());
    let next = drive(
        next,
        RunControl::default(),
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    assert_eq!(next.reason, RunFinishReason::Completed);
    assert_eq!(provider.request_count(), 1);
    assert!(plane.dispatched().is_empty());
}

#[tokio::test]
async fn a_resume_with_an_indeterminate_call_writes_outcome_unknown_seals_interrupted_and_calls_no_provider(
) {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;

    // After both intents were synced: both calls may have run.
    let kill = kill_after_intent_of(&handle, "t2").await;
    assert_eq!(kill.after_sync, Some(true), "{}", kill.label());
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    assert_eq!(resumed.reason, RunFinishReason::Interrupted);
    assert_eq!(
        resumed.provider.request_count(),
        0,
        "the close-out makes no model call"
    );
    assert!(
        resumed.plane.dispatched().is_empty(),
        "nothing is sent again"
    );
    let prefix = kill.durable.record_count();
    let records = resumed.log.records();
    assert!(
        matches!(
            &records[prefix..],
            [
                WalRecord::ResumeStarted { indeterminate, .. },
                WalRecord::ToolResult { .. },
                WalRecord::ToolResult { .. },
                WalRecord::ToolBatchFinished { .. },
                WalRecord::TurnFinished { .. },
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                },
            ] if indeterminate == &vec![t("t1"), t("t2")]
        ),
        "resume record, the results, the batch commit, then the seal: {:#?}",
        &records[prefix..]
    );
    assert_eq!(
        results_after(&records, prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ]
    );
    for record in &records[prefix..] {
        if let WalRecord::ToolResult { result, .. } = record {
            assert_eq!(**result, outcome_unknown_result(&result.tool_call_id));
            assert!(result.is_error);
        }
    }

    // Inside the intents' barrier: only t1's intent survived, so t2 never left the process. It
    // is closed as interrupted, never dispatched, and only t1 is indeterminate.
    let kill = kill_after_intent_of(&handle, "t1").await;
    assert_eq!(kill.after_sync, Some(false), "{}", kill.label());
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
    let prefix = kill.durable.record_count();
    assert_eq!(
        results_after(&resumed.log.records(), prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Interrupted)
        ]
    );
}

#[tokio::test]
async fn the_next_send_renders_the_call_paired_with_its_outcome_unknown_result_and_the_provider_accepts_it(
) {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let kill = kill_after_intent_of(&handle, "t2").await;
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);

    // The owner's next send: a new episode with a new run id and new input.
    let mut next = scenarios::edit_request();
    next.run_id = RunId("r2".into());
    next.input = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue after review".into(),
        }],
    )];
    let provider = ScriptedProvider::new(
        vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m-next".into()),
                    content: vec![ContentBlock::Text {
                        text: "continued".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]],
        Render::Canonical,
    );
    let plane = HarnessPlane::new(scenarios::edit_plane());
    let outcome = drive(
        next,
        RunControl::default(),
        &kill.state_root,
        &FileWalStore::new(kill.state_root.join("wal")),
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::Completed);
    // The scripted provider refuses a prompt holding a tool call with no matching result, as
    // Anthropic refuses a `tool_use` with no `tool_result` after it.
    assert!(provider.rejected().is_empty(), "{:?}", provider.rejected());
    assert_eq!(provider.request_count(), 1);
    assert!(
        plane.dispatched().is_empty(),
        "the next send re-sends nothing"
    );

    let sent = provider.sent();
    let opts: CallOptions = serde_json::from_slice(&sent[&0]).expect("canonical request bytes");
    assert_eq!(
        opts.prompt.iter().map(|m| m.role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant, Role::Tool, Role::User]
    );
    let calls: Vec<&ToolCallId> = opts.prompt[1]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(&call.tool_call_id),
            _ => None,
        })
        .collect();
    assert_eq!(calls, vec![&t("t1"), &t("t2")]);
    let results: Vec<_> = opts.prompt[2]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        vec![
            outcome_unknown_result(&t("t1")),
            outcome_unknown_result(&t("t2"))
        ],
        "each call is paired with the result the close-out wrote"
    );

    // The new episode's log still holds exactly one terminal per run and one result per call.
    let log = LogImage::read(&broca_crash_harness::wal_path(
        &kill.state_root,
        &scenarios::session(),
    ));
    let terminals = log
        .records()
        .iter()
        .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
        .count();
    assert_eq!(terminals, 2);
}

#[tokio::test]
async fn a_cut_between_the_outcome_unknown_results_and_the_seal_resumes_without_duplicates() {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let kill = kill_after_intent_of(&handle, "t2").await;
    let prefix = kill.durable.record_count();

    // Every boundary of the first resume's close-out, each resumed again and checked: both
    // sides of the resume record, of each result, of the batch commit, and of the seal.
    let nested = handle.every_boundary_inside_resume(&kill).await;
    assert_eq!(
        nested.len(),
        6,
        "resume record, two results, batch and turn commits, seal"
    );

    // Every close-out record is durable and the Interrupted `RunFinished` is not: the resume
    // after this cut must add no second result and still end with exactly one terminal.
    let before_seal = nested
        .iter()
        .find(|cut| {
            matches!(
                cut.durable.records().last(),
                Some(WalRecord::TurnFinished { .. })
            )
        })
        .expect("a cut just before the seal");
    let resumed = handle.resume(before_seal).await;
    handle.check(before_seal, &resumed);
    let records = resumed.log.records();
    assert_eq!(
        results_after(&records, prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ],
        "the second resume adds no result for a call the first already closed"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
            .count(),
        1,
        "still one terminal"
    );
    assert_eq!(
        broca_crash_harness::resume_records(&records),
        2,
        "each resume writes its own resume record"
    );
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
}

#[tokio::test]
async fn a_runner_sigkilled_inside_a_tool_dispatch_resumes_into_the_close_out() {
    let mut suite = Suite::new("resume close-out");
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let (kill, resumed) = handle
        .kill_resume_check(
            KillPoint::InFlight(InFlight::ToolDispatch(t("t1"))),
            KillTrigger::ProcessKill,
        )
        .await;
    // The runner died with both intents durable and no result: t1 was inside its dispatch and
    // t2, dispatched alongside it, had run but its result waits for the batch's barrier.
    let durable = kill.durable.records();
    let intents = durable
        .iter()
        .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
        .count();
    assert_eq!(intents, 2, "{}", kill.label());
    assert!(!durable
        .iter()
        .any(|record| matches!(record, WalRecord::ToolResult { .. })));
    assert_eq!(resumed.reason, RunFinishReason::Interrupted);
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
    assert_eq!(
        results_after(&resumed.log.records(), kill.durable.record_count()),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ]
    );
    suite.record([kill]);
    suite.finish();
}
