use super::*;
use crate::replay::deferred_tests;
use broca_protocol::{ContentBlock, Message, Role};
use broca_wal::{DispatchOutcome, WalRecord};
use cortexkit_role_llm_runner::live::LiveEvent;

fn tool_turn() -> Vec<WalRecord> {
    let mut records = deferred_tests::batch(&["a"]);
    if let WalRecord::RunStarted { input, ts_ms, .. } = &mut records[0] {
        *input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "task".into(),
            }],
        )]
        .into();
        *ts_ms = Some(9_999_999);
    }
    records.insert(
        1,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
    );
    records.push(WalRecord::ToolDispatchIntent {
        dispatched_at_ms: Some(1234),
        batch_id: broca_protocol::BatchId("b".into()),
        tool_call_id: broca_protocol::ToolCallId("a".into()),
        tool_name: Some("edit".into()),
        args: serde_json::json!({"n":1}).into(),
        module_id: None,
        schema_pin: None,
        hooks: None,
        approval_digest: None,
        call_key: None,
    });
    let mut terminal = deferred_tests::terminal("a", None);
    if let WalRecord::ToolResult {
        outcome,
        result,
        finished_at_ms,
        ..
    } = &mut terminal
    {
        *outcome = DispatchOutcome::Completed;
        *result = deferred_tests::result("a", "done");
        *finished_at_ms = Some(1250);
    }
    records.push(terminal);
    records.extend(deferred_tests::finished().into_iter().take(2));
    records.push(WalRecord::StepStarted {
        step_id: broca_protocol::StepId(2),
    });
    let mut answer = deferred_tests::batch(&[]).remove(1);
    if let WalRecord::ModelStepFinished {
        step_id,
        assistant_message,
        finish_reason,
        ..
    } = &mut answer
    {
        *step_id = broca_protocol::StepId(2);
        Arc::make_mut(assistant_message).content = vec![ContentBlock::Text {
            text: "answer".into(),
        }];
        *finish_reason = None;
    }
    records.push(answer);
    records.push(deferred_tests::finished().remove(2));
    records
}

fn frames(records: &[WalRecord]) -> Vec<FramedRecord> {
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

fn reopen_stored_frames(records: &[WalRecord]) -> Vec<FramedRecord> {
    use broca_wal::{FileWal, Wal};
    let root = broca_test_support::scratch_root().join(format!(
        "live-tool-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let path = root.join("session.wal");
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(root.join("wal-feature-floor"));
    for record in records {
        wal.append(1, record, true).unwrap();
    }
    drop(wal);
    FileWal::open(&path).unwrap().replay().unwrap()
}

#[test]
fn held_status_changes_share_the_status_read_fold_and_replay_cursors() {
    use crate::replay::provider_held_tests as held;
    let mut records = held::prefix(false);
    let mut second = held::held(false);
    if let WalRecord::ToolCallProviderHeld {
        tool_call_id,
        call_key,
        ..
    } = &mut second
    {
        *tool_call_id = broca_protocol::ToolCallId("b".into());
        *call_key = "lineage:5".into();
    }
    records.push(second.clone());
    records.push(second); // Re-observing identical metadata does not change the list.
    records.push(WalRecord::RunPaused {
        ts_ms: Some(43),
        run_id: RunId("r".into()),
        reason: broca_protocol::PauseReason::ToolHeld,
        error: None,
    });
    let mut other_batch = held::terminal("a", DispatchOutcome::Completed);
    if let WalRecord::ToolResult { batch_id, .. } = &mut other_batch {
        *batch_id = broca_protocol::BatchId("older-batch".into());
    }
    records.push(other_batch);
    records.push(held::terminal("a", DispatchOutcome::Completed));
    records.push(WalRecord::ToolNoDispatch {
        batch_id: broca_protocol::BatchId("b".into()),
        tool_call_id: broca_protocol::ToolCallId("b".into()),
        reason: broca_wal::NoDispatchReason::Denied {
            reason: "not sent".into(),
        },
        dispatched_at_ms: None,
        hooks: None,
        tags: None,
    });
    let framed = frames(&records);
    let mut live_state = ControlProjectionState::default();
    let mut live = Vec::new();
    let mut lists = Vec::new();
    for (index, record) in framed.iter().enumerate() {
        let units =
            project_control_with_mids(std::slice::from_ref(record), &mut live_state).unwrap();
        for unit in &units {
            if let ControlUnit::Status { status, .. } = &unit.unit {
                assert_eq!(
                    status.held_calls(),
                    crate::reconcile_run_state(&records[..=index], "r")
                        .unwrap()
                        .held_calls
                );
                lists.push(
                    status
                        .held_calls()
                        .iter()
                        .map(|call| call.call_key().to_owned())
                        .collect::<Vec<_>>(),
                );
            }
            assert!(!matches!(unit.event, LiveEvent::RunPaused(_)));
        }
        live.extend(units);
    }
    assert_eq!(
        lists,
        [
            vec!["lineage:4"],
            vec!["lineage:4", "lineage:5"],
            vec!["lineage:5"],
            vec![]
        ]
    );
    let replay =
        project_control_with_mids(&reopen_stored_frames(&records), &mut Default::default())
            .unwrap();
    let comparable = |units: Vec<PlacedControlUnit>| {
        units
            .into_iter()
            .map(|unit| (unit.cursor, unit.unit, unit.message_ordinal, unit.event))
            .collect::<Vec<_>>()
    };
    assert_eq!(comparable(live), comparable(replay));
}

#[test]
fn message_boundaries_complete_every_stored_message_without_moving_old_cursors() {
    let records = tool_turn();
    let projected = project_control_with_mids(&frames(&records), &mut Default::default()).unwrap();
    let old_cursors: Vec<_> = projected
        .iter()
        .filter(|frame| !matches!(frame.unit, ControlUnit::MessageBoundary { .. }))
        .map(|frame| (frame.cursor.wal_seq, frame.cursor.sub_index))
        .collect();
    assert_eq!(
        old_cursors,
        [
            (1, 0),
            (2, 0),
            (3, 0),
            (3, 1),
            (3, 2),
            (6, 0),
            (9, 0),
            (10, 0),
            (10, 1),
            (11, 0)
        ]
    );
    let mut lifecycle = std::collections::BTreeMap::<String, Vec<&str>>::new();
    for frame in &projected {
        match &frame.event {
            LiveEvent::MessageStarted(message) => lifecycle
                .entry(message.mid.clone())
                .or_default()
                .push("start"),
            LiveEvent::MessageFinished(message) => lifecycle
                .entry(message.mid.clone())
                .or_default()
                .push("finish"),
            _ => {}
        }
    }
    assert_eq!(
        lifecycle.keys().cloned().collect::<Vec<_>>(),
        ["m0", "m1", "m2", "m3"]
    );
    assert!(lifecycle
        .values()
        .all(|events| events == &["start", "finish"]));
    let replay = crate::replay(&records).unwrap();
    assert_eq!(
        replay
            .prompt
            .iter()
            .filter_map(Message::mid)
            .collect::<Vec<_>>(),
        ["m0", "m1", "m2", "m3"]
    );
}

#[test]
fn tool_events_use_recorded_observations_not_run_or_projection_clocks() {
    let records = tool_turn();
    let projected = project_control_with_mids(&frames(&records), &mut Default::default()).unwrap();
    let start = projected
        .iter()
        .find_map(|frame| match &frame.event {
            LiveEvent::ToolStarted(event) => Some(event),
            _ => None,
        })
        .unwrap();
    let finish = projected
        .iter()
        .find_map(|frame| match &frame.event {
            LiveEvent::ToolFinished(event) => Some(event),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        (
            start.mid.as_str(),
            start.tool_call_id.as_str(),
            start.name.as_str(),
            start.arguments.as_str(),
            start.started_at
        ),
        ("m1", "a", "edit", r#"{"n":1}"#, Some(1234))
    );
    assert_eq!(
        (
            finish.mid.as_str(),
            finish.tool_call_id.as_str(),
            finish.outcome.as_str(),
            finish.output_bytes,
            finish.finished_at
        ),
        ("m1", "a", "ok", 4, Some(1250))
    );
    let mut state = ControlProjectionState::default();
    let incremental: Vec<_> = frames(&records)
        .iter()
        .flat_map(|frame| {
            project_control_with_mids(std::slice::from_ref(frame), &mut state).unwrap()
        })
        .collect();
    let encoded = |units: &[PlacedControlUnit]| {
        units
            .iter()
            .map(|frame| {
                (
                    frame.cursor,
                    serde_json::to_value(&frame.unit).unwrap(),
                    frame.event.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(encoded(&projected), encoded(&incremental));
}

#[test]
fn historical_call_events_leave_both_unrecorded_times_absent() {
    let mut records = tool_turn();
    for record in &mut records {
        match record {
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms, ..
            } => *dispatched_at_ms = None,
            WalRecord::ToolResult { finished_at_ms, .. } => *finished_at_ms = None,
            _ => {}
        }
    }
    let projected = project_control_with_mids(&frames(&records), &mut Default::default()).unwrap();
    let tools: Vec<_> = projected
        .iter()
        .filter(|frame| {
            matches!(
                frame.event,
                LiveEvent::ToolStarted(_) | LiveEvent::ToolFinished(_)
            )
        })
        .collect();
    assert_eq!(tools.len(), 2);
    for frame in tools {
        let value = serde_json::to_value(&frame.event).unwrap();
        assert!(value.get("started_at").is_none(), "{value}");
        assert!(value.get("finished_at").is_none(), "{value}");
        assert_eq!(value["mid"], "m1");
    }
}

#[test]
fn imported_input_calls_get_lifecycle_events_without_inventing_times() {
    let mut start = deferred_tests::batch(&["a"]).remove(0);
    let call = match deferred_tests::batch(&["a"]).remove(1) {
        WalRecord::ModelStepFinished {
            assistant_message, ..
        } => assistant_message.tool_calls().next().unwrap().clone(),
        _ => unreachable!(),
    };
    if let WalRecord::RunStarted {
        input,
        ts_ms,
        origin,
        ..
    } = &mut start
    {
        *input = vec![
            Message::new(Role::Assistant, vec![ContentBlock::ToolCall(call)]),
            Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(
                    deferred_tests::result("a", "old result").as_ref().clone(),
                )],
            ),
        ]
        .into();
        *ts_ms = Some(9_999_999);
        *origin = broca_wal::RunOrigin::SeededFrom {
            origin_kind: broca_wal::SeedOriginKind::Import,
            source_ref: "history".into(),
            origin_device_pseudonym: None,
            base_seq: None,
        };
    }
    let records = [start, deferred_tests::finished().remove(2)];
    let projected = project_control_with_mids(&frames(&records), &mut Default::default()).unwrap();
    let tools: Vec<_> = projected
        .iter()
        .filter_map(|frame| match &frame.event {
            LiveEvent::ToolStarted(_) | LiveEvent::ToolFinished(_) => {
                Some(serde_json::to_value(&frame.event).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0]["type"], "tool_started");
    assert_eq!(tools[1]["type"], "tool_finished");
    assert!(tools.iter().all(|event| event["mid"] == "m0"
        && event.get("started_at").is_none()
        && event.get("finished_at").is_none()));
}

#[test]
fn declined_deferred_call_finishes_once_with_its_recorded_closing_time() {
    let mut records = tool_turn();
    let mut prepared = deferred_tests::prepared("a", "b");
    if let WalRecord::ToolCallDeferred { deferred_at_ms, .. } = &mut prepared {
        *deferred_at_ms = Some(1200);
    }
    let mut declined = deferred_tests::approval("a", "key", broca_wal::DeferredDecision::Declined);
    if let WalRecord::ApprovalRecorded { closed_at_ms, .. } = &mut declined {
        *closed_at_ms = Some(1300);
    }
    records[4] = prepared;
    records[5] = declined;
    let projected =
        project_control_with_mids(&reopen_stored_frames(&records), &mut Default::default())
            .unwrap();
    let starts: Vec<_> = projected
        .iter()
        .filter_map(|frame| match &frame.event {
            LiveEvent::ToolStarted(event) => Some(event),
            _ => None,
        })
        .collect();
    let finishes: Vec<_> = projected
        .iter()
        .filter_map(|frame| match &frame.event {
            LiveEvent::ToolFinished(event) => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(finishes.len(), 1);
    assert_eq!(starts[0].started_at, Some(1200));
    assert_eq!(
        (
            finishes[0].mid.as_str(),
            finishes[0].outcome.as_str(),
            finishes[0].finished_at
        ),
        ("m1", "denied", Some(1300))
    );
}

#[test]
fn no_dispatch_uses_one_durable_observation_for_both_lifecycle_events() {
    let mut records = tool_turn();
    records[4] = WalRecord::ToolNoDispatch {
        dispatched_at_ms: Some(1400),
        batch_id: broca_protocol::BatchId("b".into()),
        tool_call_id: broca_protocol::ToolCallId("a".into()),
        reason: broca_wal::NoDispatchReason::Denied {
            reason: "no".into(),
        },
        hooks: None,
        tags: None,
    };
    records.remove(5);
    let projected = project_control_with_mids(&frames(&records), &mut Default::default()).unwrap();
    let tools: Vec<_> = projected
        .iter()
        .filter_map(|frame| match &frame.event {
            LiveEvent::ToolStarted(_) | LiveEvent::ToolFinished(_) => {
                Some(serde_json::to_value(&frame.event).unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0]["started_at"], 1400);
    assert_eq!(tools[1]["finished_at"], 1400);
    assert_eq!(tools[1]["outcome"], "denied");
}
