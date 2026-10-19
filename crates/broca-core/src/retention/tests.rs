use super::*;
use crate::projection_plan::{
    plan_projection, ProjectionPlan, ProjectionPlanError, ProjectionSnapshot,
};
use crate::{AdmissionCheckpointV1, ProjectionSeed};
use broca_protocol::{
    AssistantMessage, BindIdentity, FinishReason, Message, MessageId, RunFinishReason, RunId,
    StepId, Usage,
};
use broca_wal::{FramedRecord, RunConfig, RunOrigin, WalRangeRead, WalRecord};

pub(crate) const MARKER: &str = "retention-content-must-not-be-projected-5d8e7b";

pub(crate) fn started(run: &str, retention: Option<u64>) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
        session_class: None,
        retention,
        hooks: None,
        title: Some(MARKER.into()),
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: Some(1000),
        run_id: RunId(run.into()),
        session: BindIdentity::new("/retention-test", "runner", "session"),
        config: Box::new(RunConfig {
            render_tags: false,
            model: broca_protocol::ModelHandle::new("stub", "m"),
            tools: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }),
        input: vec![Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: MARKER.into(),
            }],
        )]
        .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

pub(crate) fn frames(retention: Option<u64>) -> Vec<FramedRecord> {
    [
        started("r1", retention),
        WalRecord::StepStarted { step_id: StepId(1) },
        WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: MARKER.into(),
                }],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        },
        WalRecord::TurnFinished { step_id: StepId(1) },
        WalRecord::RunFinished {
            ts_ms: Some(2000),
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, record)| FramedRecord {
        seq: index as u64 + 1,
        fence: 1,
        record,
    })
    .collect()
}

fn snapshot(seed: Option<ProjectionSeed>, watermark: u64) -> ProjectionSnapshot {
    ProjectionSnapshot {
        attribution_complete: true,
        seed,
        watermark,
        lineage_id: None,
        next_ord: 0,
        previous_control_head: None,
    }
}

fn retained_seed(records: &[FramedRecord]) -> ProjectionSeed {
    ProjectionSeed {
        admission_checkpoint: AdmissionCheckpointV1::from_frames(&records[..2]).unwrap(),
        watermark_seq: 2,
        ..ProjectionSeed::empty()
    }
}

fn assert_no_content(plan: ProjectionPlan) {
    assert!(
        matches!(plan, ProjectionPlan::NoChange { .. }),
        "content-bearing plan: {plan:?}"
    );
    assert!(!format!("{plan:?}").contains(MARKER));
}

#[test]
fn retention_projection_has_no_content_rows() {
    let records = frames(Some(3600));
    assert_no_content(plan_projection(snapshot(None, 0), &records, None).unwrap());
}

#[test]
fn retention_checkpoint_tail_has_no_content_body() {
    let records = frames(Some(3600));
    let seed = retained_seed(&records);
    assert!(
        format!("{seed:?}").contains(MARKER),
        "planted checkpoint control"
    );
    assert_no_content(plan_projection(snapshot(Some(seed), 2), &records[2..], None).unwrap());
}

#[test]
fn retention_last_title_tail_has_no_title() {
    let input = vec![Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: MARKER.into(),
        }],
    )];
    let request = serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": &input
    }))
    .unwrap();
    let sends = [
        WalRecord::Steered {
            tags: None,
            retention: None,
            ts_ms: Some(1500),
            title: Some(MARKER.into()),
            delivery: Some(broca_protocol::Delivery::Steer),
            hooks: None,
            input: input.clone().into(),
            send_id: None,
            request_identity_digest: None,
        },
        WalRecord::Queued {
            retention: None,
            ts_ms: Some(1500),
            title: Some(MARKER.into()),
            keep_warm: None,
            conversation_key: None,
            submission_id: broca_protocol::SubmissionId("queued".into()),
            request: Box::new(request),
            request_identity_digest: None,
            principal: None,
            send_id: None,
            delivery: None,
            hooks: None,
        },
    ];
    for send in sends {
        let mut records = frames(Some(3600));
        // Keep the real completed model turn: without that boundary a title-only
        // batch could be a no-op even if the retention fence were removed.
        records.insert(
            1,
            FramedRecord {
                seq: 2,
                fence: 1,
                record: send,
            },
        );
        for (index, frame) in records.iter_mut().enumerate() {
            frame.seq = index as u64 + 1;
        }
        let seed = ProjectionSeed {
            admission_checkpoint: AdmissionCheckpointV1::from_frames(&records[..1]).unwrap(),
            watermark_seq: 1,
            ..ProjectionSeed::empty()
        };
        assert_no_content(plan_projection(snapshot(Some(seed), 1), &records[1..], None).unwrap());
        if let WalRecord::RunStarted { retention, .. } = &mut records[0].record {
            *retention = None;
        }
        let seed = ProjectionSeed {
            admission_checkpoint: AdmissionCheckpointV1::from_frames(&records[..1]).unwrap(),
            watermark_seq: 1,
            ..ProjectionSeed::empty()
        };
        let ProjectionPlan::Write(prepared) =
            plan_projection(snapshot(Some(seed), 1), &records[1..], None).unwrap()
        else {
            panic!("the same forever tail must have a real title write");
        };
        assert_eq!(prepared.last_title.as_deref(), Some(MARKER));
    }
}

#[test]
fn retention_lineage_reproject_never_resets_or_writes_stale_meta() {
    let records = frames(Some(3600));
    let mut stale = snapshot(Some(ProjectionSeed::empty()), 99);
    stale.lineage_id = Some("different-old-lineage".into());
    let mut range = WalRangeRead::without_anchors(records.clone());
    range.lineage_id = Some(crate::projection_plan_tests::incoming_lineage());
    assert_no_content(plan_projection(stale.clone(), &records, Some(&range)).unwrap());
    // A mismatched tail has no proof of the incoming first run. Never reuse the
    // stale forever checkpoint as permission to persist the replacement's content.
    let tail = &records[2..];
    let error = plan_projection(stale, tail, Some(&range)).unwrap_err();
    assert!(matches!(
        error,
        ProjectionPlanError::InconsistentSnapshot(_)
    ));
}

#[test]
fn retention_import_projection_has_no_content_rows() {
    let mut records = frames(Some(3600));
    if let WalRecord::RunStarted { origin, config, .. } = &mut records[0].record {
        *origin = RunOrigin::SeededFrom {
            origin_kind: broca_wal::SeedOriginKind::Import,
            source_ref: format!("fixture:{MARKER}"),
            origin_device_pseudonym: None,
            base_seq: None,
        };
        config.provider_options = serde_json::json!({"import_request_digest": MARKER});
    }
    assert_no_content(plan_projection(snapshot(None, 0), &records, None).unwrap());
}

#[test]
fn retention_operator_reproject_from_watermark_has_no_content_rows() {
    let records = frames(Some(3600));
    let seed = retained_seed(&records);
    let mut range = WalRangeRead::without_anchors(records[2..].to_vec());
    range.lineage_id = Some(crate::projection_plan_tests::incoming_lineage());
    let mut current = snapshot(Some(seed), 2);
    current.lineage_id = range.lineage_id.map(|id| id.to_string());
    assert_no_content(plan_projection(current, &records[2..], Some(&range)).unwrap());
}

#[test]
fn unknown_retention_tail_refuses_content_projection() {
    let records = frames(Some(3600));
    let mut legacy = retained_seed(&records);
    legacy.admission_checkpoint.lineage_retention = None;
    assert!(matches!(
        plan_projection(snapshot(Some(legacy), 2), &records[2..], None),
        Err(ProjectionPlanError::InconsistentSnapshot(
            "content projection requires the first RunStarted retention policy from the WAL"
        ))
    ));
    let truncated = AdmissionCheckpointV1::from_frames(&records[1..2]).unwrap();
    assert_eq!(truncated.lineage_retention, None);
}

#[test]
fn forever_lineage_still_projects_messages_checkpoint_and_title() {
    let records = frames(None);
    let ProjectionPlan::Write(prepared) =
        plan_projection(snapshot(None, 0), &records, None).unwrap()
    else {
        panic!("forever positive control must plan content");
    };
    assert!(prepared
        .message_json
        .iter()
        .any(|json| json.contains(MARKER)));
    assert!(prepared.checkpoint_json.contains(MARKER));
    assert_eq!(prepared.last_title.as_deref(), Some(MARKER));
    // A checkpoint derived from the first WAL run permits a suffix-only projection.
    let seed = retained_seed(&records);
    assert!(matches!(
        plan_projection(snapshot(Some(seed), 2), &records[2..], None).unwrap(),
        ProjectionPlan::Write(_)
    ));
}

#[test]
fn only_first_run_started_fixes_retention_policy() {
    let mut records = frames(Some(3600));
    records.push(FramedRecord {
        seq: 6,
        fence: 1,
        record: started("r2", None),
    });
    assert!(is_retention_lineage(&records));
    let checkpoint = AdmissionCheckpointV1::from_frames(&records).unwrap();
    assert_eq!(
        checkpoint.lineage_retention,
        Some(LineageRetention::Retained { seconds: 3600 })
    );
    if let WalRecord::RunStarted { retention, .. } = &mut records[0].record {
        *retention = None;
    }
    if let WalRecord::RunStarted { retention, .. } = &mut records[5].record {
        *retention = Some(3600);
    }
    assert!(!is_retention_lineage(&records));
    assert_eq!(
        AdmissionCheckpointV1::from_frames(&records)
            .unwrap()
            .lineage_retention,
        Some(LineageRetention::Forever)
    );
}

#[test]
fn complete_checkpoint_policy_agrees_for_zero_and_one_based_origins() {
    for base in [0, 1] {
        for (retention, expected) in [
            (None, LineageRetention::Forever),
            (Some(3600), LineageRetention::Retained { seconds: 3600 }),
        ] {
            let mut records = frames(retention);
            for (index, frame) in records.iter_mut().enumerate() {
                frame.seq = base + index as u64;
            }
            let complete = AdmissionCheckpointV1::from_frames(&records).unwrap();
            assert_eq!(complete.lineage_retention, Some(expected));
            assert_eq!(
                projection_policy(&records[1..], Some((expected, base))),
                Some(expected),
                "a suffix beginning at one must not erase a zero-based first-run proof"
            );
            let mut incremental = AdmissionCheckpointV1::empty();
            incremental.fold_frames(&records[..2]).unwrap();
            incremental.fold_frames(&records[2..]).unwrap();
            assert_eq!(incremental.lineage_retention, Some(expected));
            assert_eq!(
                incremental, complete,
                "the full-WAL oracle and incremental constructor must agree"
            );
        }
    }
}

#[test]
fn complete_prefix_recovers_policy_without_projection_metadata() {
    for base in [0, 1] {
        for retention in [None, Some(3600)] {
            let mut records = frames(retention);
            for (index, frame) in records.iter_mut().enumerate() {
                frame.seq = base + index as u64;
            }
            // Missing or invalid store checkpoints must not make a complete replay unknown.
            let plan = plan_projection(snapshot(None, 2), &records, None).unwrap();
            if retention.is_some() {
                assert_no_content(plan);
            } else {
                let ProjectionPlan::Write(prepared) = plan else {
                    panic!("forever prefix must repair metadata");
                };
                assert!(prepared
                    .checkpoint_json
                    .contains("\"lineage_retention\":{\"kind\":\"forever\"}"));
                assert!(prepared
                    .message_json
                    .iter()
                    .any(|json| json.contains(MARKER)));
            }
        }
    }
}

#[test]
fn unstarted_checkpoint_cannot_authorize_content_after_a_skipped_first_run() {
    let records = frames(Some(3600));
    let mut skipped_run = records[2..].to_vec();
    // A later non-retained run is not the first run of the lineage.
    skipped_run[0].record = started("later", None);
    for tail in [&records[2..], skipped_run.as_slice()] {
        assert!(plan_projection(snapshot(Some(ProjectionSeed::empty()), 0), tail, None).is_err());
    }
    // A zero-based tail may start at one but still contains no first RunStarted.
    let zero_based_tail: Vec<_> = records[2..]
        .iter()
        .cloned()
        .map(|mut frame| {
            frame.seq -= 2;
            frame
        })
        .collect();
    assert!(plan_projection(
        snapshot(Some(ProjectionSeed::empty()), 0),
        &zero_based_tail,
        None
    )
    .is_err());
}

/// Marker-bearing deferred arguments, approval prompt and late result. The run's
/// only durable debt is the unanswered question for the Prepared call at seq 4.
pub(crate) fn deferred_records(retention: Option<u64>, lineage: &str) -> Vec<FramedRecord> {
    use crate::replay::deferred_tests::{batch, finished, prepared, result, terminal};
    let mut records = batch(&["call"]);
    records[0] = started("r1", retention);
    let mut call = prepared("call", "b");
    if let WalRecord::ToolCallDeferred { args_jcs, ask, .. } = &mut call {
        *args_jcs = serde_json::json!({"text": MARKER}).to_string();
        ask.prompt = MARKER.into();
    }
    records.push(call);
    records.push(terminal(
        "call",
        Some(broca_wal::WithdrawElicitation {
            call_key: format!("{lineage}:4"),
            elicitation_id: Some("question".into()),
        }),
    ));
    records.extend(finished());
    records.push(WalRecord::LateResult {
        call_key: format!("{lineage}:4"),
        outcome: broca_wal::LateOutcome::Completed,
        result: result("call", MARKER),
    });
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record,
        })
        .collect()
}

#[test]
fn retained_deferred_plan_keeps_only_content_free_withdrawal_flags() {
    let lineage = crate::projection_plan_tests::incoming_lineage();
    let records = deferred_records(Some(3600), &lineage.to_string());
    let view =
        crate::replay::project_deferred_calls(None, &records, Some(&lineage.to_string()), None)
            .unwrap();
    let summary = serde_json::to_string(&view).unwrap();
    assert!(summary.contains(MARKER));
    assert!(
        summary.contains("args_jcs") && summary.contains("late_result"),
        "the existing summary is not content-free"
    );
    let mut range = WalRangeRead::without_anchors(records.clone());
    range.lineage_id = Some(lineage);
    let plan = plan_projection(snapshot(None, 0), &records, Some(&range)).unwrap();
    let ProjectionPlan::RetentionMetadata { metadata, .. } = &plan else {
        panic!("retained debt must remain discoverable");
    };
    assert_eq!(metadata.withdrawals_owed_runs, ["r1"]);
    assert_eq!(metadata.lineage_id, lineage.to_string());
    assert!(
        !format!("{plan:?}").contains(MARKER),
        "no prompt, args, title, checkpoint or late result may enter the retained plan"
    );

    let mut forever = records.clone();
    if let WalRecord::RunStarted { retention, .. } = &mut forever[0].record {
        *retention = None;
    }
    let ProjectionPlan::Write(prepared) =
        plan_projection(snapshot(None, 0), &forever, Some(&range)).unwrap()
    else {
        panic!("forever projection must keep its content");
    };
    assert!(prepared.checkpoint_json.contains(MARKER));
    assert_eq!(prepared.last_title.as_deref(), Some(MARKER));
    assert!(prepared.deferred_calls_json.unwrap().contains(MARKER));
    assert_eq!(prepared.withdrawals_owed_runs, ["r1"]);
}

#[test]
fn retained_prepared_call_without_lineage_is_refused() {
    let mut records = frames(Some(3600));
    records.insert(
        3,
        FramedRecord {
            seq: 4,
            fence: 1,
            record: crate::replay::deferred_tests::prepared("call", "batch"),
        },
    );
    for (index, frame) in records.iter_mut().enumerate() {
        frame.seq = index as u64 + 1;
    }
    for retention in [Some(3600), None] {
        if let WalRecord::RunStarted {
            retention: policy, ..
        } = &mut records[0].record
        {
            *policy = retention;
        }
        assert!(matches!(
            plan_projection(snapshot(None, 0), &records, None),
            Err(ProjectionPlanError::InconsistentSnapshot(
                "Prepared call projection requires a WAL lineage identity"
            ))
        ));
    }
}
