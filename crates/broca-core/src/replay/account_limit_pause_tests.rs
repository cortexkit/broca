use super::*;

fn pause() -> WalRecord {
    serde_json::from_str(include_str!(
        "../../../broca-wal/tests/fixtures/account_limit_pause.jsonl"
    ))
    .unwrap()
}

fn framed(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

#[test]
fn account_limit_pause_replay_checkpoint_and_resume_retain_one_request_boundary() {
    let started = projection_boundary_tests::run_started(1, "r");
    let frames = vec![
        started,
        framed(2, WalRecord::StepStarted { step_id: StepId(0) }),
        framed(3, pause()),
    ];
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let prior = replay(&records).unwrap();
    assert!(matches!(
        prior.resolution,
        ReplayResolution::AuthRequired {
            reason: PauseReason::AccountLimit,
            pending: None,
            ..
        }
    ));
    let pause = prior.account_limit_pause.as_ref().unwrap();
    assert_eq!(pause.failed_attempt_seq, 2);
    assert_eq!(pause.resume_not_before_ms, 61_000);
    assert_eq!(prior.completed_steps, 0);
    let reconciled = reconcile_run_state(records.iter(), "r").unwrap();
    assert_eq!(reconciled.account_limit_pause, prior.account_limit_pause);
    let checkpoint = crate::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let mut restored: crate::AdmissionCheckpointV1 = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        restored.last_episode.as_ref().unwrap().account_limit_pause,
        prior.account_limit_pause
    );
    restored
        .fold_frames(&[framed(4, WalRecord::StepStarted { step_id: StepId(0) })])
        .unwrap();
    assert!(restored.last_episode.unwrap().account_limit_pause.is_none());
    let mut continued = records;
    continued.push(WalRecord::StepStarted { step_id: StepId(0) });
    let resumed = replay(&continued).unwrap();
    assert!(matches!(
        resumed.resolution,
        ReplayResolution::Resume { pending: None }
    ));
    assert!(resumed.account_limit_pause.is_none());
    assert_eq!(
        serde_json::to_vec(&prior.prompt).unwrap(),
        serde_json::to_vec(&resumed.prompt).unwrap()
    );
}

#[test]
fn account_limit_pause_cannot_hide_an_indeterminate_tool_intent() {
    let mut records = deferred_tests::batch(&["c"]);
    records.push(provider_held_tests::intent("c"));
    records.push(pause());
    assert!(matches!(replay(&records).unwrap().resolution,
        ReplayResolution::Interrupted { indeterminate } if indeterminate == [ToolCallId("c".into())]));
}
