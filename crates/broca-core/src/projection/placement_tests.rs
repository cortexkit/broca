use super::*;
use crate::replay::deferred_tests;
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::{BudgetConfig, BudgetKind, BudgetNudge},
    StepId,
};

fn framed(records: &[WalRecord]) -> Vec<FramedRecord> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect()
}

#[test]
fn projection_seed_restores_absolute_placement_and_due_budget_facts() {
    let mut records = deferred_tests::batch(&["a"]);
    if let WalRecord::RunStarted { input, config, .. } = &mut records[0] {
        *input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "task".into(),
            }],
        )]
        .into();
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![BudgetNudge {
                at_tool_calls: 1,
                text: "nudge".into(),
            }],
            finalize_at_tool_calls: 9,
            finalize_text: "finish".into(),
        });
    }
    records.insert(1, WalRecord::StepStarted { step_id: StepId(1) });
    let mut terminal = deferred_tests::terminal("a", None);
    if let WalRecord::ToolResult {
        outcome, result, ..
    } = &mut terminal
    {
        *outcome = broca_wal::DispatchOutcome::Completed;
        *result = deferred_tests::result("a", "done");
    }
    records.push(terminal);
    records.extend(deferred_tests::finished().into_iter().take(2));
    let split = records.len();
    let prefix = framed(&records);
    let seed = crate::ProjectionSeed {
        watermark_seq: split as u64,
        prefix_len: 3,
        completed_steps: 1,
        last_step_finish_reason: Some(FinishReason::ToolCalls),
        last_run_id: Some(RunId("r".into())),
        admission_checkpoint: crate::AdmissionCheckpointV1::from_frames(&prefix).unwrap(),
        ..crate::ProjectionSeed::empty()
    };
    records.push(WalRecord::BudgetFired {
        tags: None,
        run_id: RunId("r".into()),
        kind: BudgetKind::Nudge,
        threshold: 1,
        ordinal: 3,
        caller_text: Some("nudge".into()),
    });
    records.push(WalRecord::StepStarted { step_id: StepId(2) });
    let mut answer = deferred_tests::batch(&[]).remove(1);
    if let WalRecord::ModelStepFinished {
        step_id,
        assistant_message,
        finish_reason,
        ..
    } = &mut answer
    {
        *step_id = StepId(2);
        Arc::make_mut(assistant_message).content = vec![ContentBlock::Text {
            text: "answer".into(),
        }];
        *finish_reason = None;
    }
    records.push(answer);
    records.push(deferred_tests::finished().remove(2));
    let all = framed(&records);
    let full = project_control_with_mids(&all, &mut Default::default()).unwrap();
    let mut restored = ControlProjectionState::from_projection_seed(&seed).unwrap();
    let tail = project_control_with_mids(&all[split..], &mut restored).unwrap();
    let expected: Vec<_> = full
        .iter()
        .filter(|frame| frame.cursor.wal_seq > split as u64)
        .collect();
    assert_eq!(tail.len(), expected.len());
    for (tail, full) in tail.iter().zip(expected) {
        assert_eq!(tail.cursor, full.cursor);
        assert_eq!(tail.message_ordinal, full.message_ordinal);
        assert_eq!(
            serde_json::to_value(&tail.unit).unwrap(),
            serde_json::to_value(&full.unit).unwrap()
        );
    }
    assert!(tail.iter().any(
        |frame| matches!(frame.unit, ControlUnit::AssistantMessage { .. })
            && frame.mid().as_deref() == Some("m4")
    ));
    assert_eq!(restored.placement().next_ordinal(), 5);
    assert_eq!(restored.placement().completed_steps(), 2);
    assert_eq!(restored.placement().budget().fired_nudge_indices, [0]);
}

#[test]
fn a_budgeted_seed_without_firing_facts_requires_full_replay() {
    let mut frames = framed(&deferred_tests::batch(&["a"]));
    if let WalRecord::RunStarted { config, .. } = &mut frames[0].record {
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![],
            finalize_at_tool_calls: 1,
            finalize_text: "finish".into(),
        });
    }
    let mut seed = crate::ProjectionSeed {
        admission_checkpoint: crate::AdmissionCheckpointV1::from_frames(&frames).unwrap(),
        last_run_id: Some(RunId("r".into())),
        ..crate::ProjectionSeed::empty()
    };
    seed.admission_checkpoint
        .last_episode
        .as_mut()
        .unwrap()
        .budget_replay_state = None;
    assert!(ControlProjectionState::from_projection_seed(&seed).is_none());
}
