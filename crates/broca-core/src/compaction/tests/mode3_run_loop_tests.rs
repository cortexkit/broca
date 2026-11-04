use super::*;
use crate::compaction_v2::{
    self as v2,
    protocol::{
        notice::NoticeRequest,
        view::{Edit, EditOp, View},
    },
};

#[derive(Default)]
struct Plane {
    unavailable: bool,
    view: Option<View>,
    calls: Mutex<Vec<(String, Value)>>,
}

#[async_trait::async_trait]
impl CompactionPlane for Plane {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        panic!("the retired v1 provider protocol must not be called")
    }
    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, v2::runtime::CallError> {
        self.calls.lock().unwrap().push((method.into(), params));
        if self.unavailable {
            return Err(v2::runtime::CallError::Unavailable);
        }
        serde_json::value::to_raw_value(&match method {
            "compaction.setup" => json!({"answer":"ready"}),
            "compaction.notice" => json!({"answer":"received"}),
            "compaction.ride" => match &self.view {
                Some(view) => json!({"answer":"view", "view":view}),
                None => json!({"answer":"nothing"}),
            },
            _ => panic!("unexpected compaction method {method}"),
        })
        .map_err(|_| v2::runtime::CallError::Unavailable)
    }
}

fn cut(version: u64) -> View {
    let through = crate::message_mid(true, Some(&fixture_lineage_id().to_string()), 1).unwrap();
    View::seal(version, vec![Edit::new(version, EditOp::Cut { through })])
}

async fn drive(
    shared: &Shared,
    plane: Arc<Plane>,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    drive_as(shared, plane, "run", crate::StartMode::FreshOrResume).await
}

async fn drive_as(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    mode: crate::StartMode,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    drive_events(shared, plane, run, mode, model_events()).await
}

async fn drive_events(
    shared: &Shared,
    plane: Arc<Plane>,
    run: &str,
    mode: crate::StartMode,
    events: Vec<broca_protocol::NormalizedEvent>,
) -> (Result<crate::RunSummary, crate::RunError>, PrefixBound) {
    let writer = shared.writer();
    let (mut request, mut control, _) = history_request();
    request.run_id = broca_protocol::RunId(run.into());
    request.context_limit = Some(100_000);
    control.admitted_context_limit = request.context_limit;
    control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
    control.compaction_plane = Some(plane);
    control.compaction_views = None;
    let model = PrefixBound(crate::StubProvider::new(vec![events]));
    let (out, _events) = tokio::sync::mpsc::channel(256);
    let result = crate::run_loop_on_writer(
        request,
        &writer,
        mode,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await;
    writer.shutdown().await.unwrap();
    (result, model)
}

#[tokio::test]
async fn live_reply_deltas_follow_the_writing_runs_clean_flag() {
    use broca_protocol::{DisplayEvent, MessageId, NormalizedEvent, ProviderControl};
    for (mode3, render) in [(false, false), (false, true), (true, false)] {
        let shared = Shared::default();
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.context_limit = Some(100_000);
        control.admitted_context_limit = request.context_limit;
        control.legacy_compaction_fixture = false;
        control.compaction_views = None;
        let mut manifest = control.manifest.as_deref().unwrap().clone();
        let mut plan = json!({"render_tags": render, "composition": {}});
        if mode3 {
            plan["compaction_item"] = json!({"provider":"provider", "preset":"head", "params":{}});
            control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
            control.compaction_plane = Some(Arc::new(Plane::default()));
        } else {
            control.compaction_plane = None;
        }
        manifest.plan = OpaqueJson::from_json_text(plan.to_string()).unwrap();
        control.manifest = Some(Arc::new(manifest));
        let text = |delta: &str| DisplayEvent::TextDelta {
            mid: None,
            part: Some(1),
            message_id: MessageId("a".into()),
            tool_call_id: None,
            delta: delta.into(),
        };
        let reasoning = DisplayEvent::ReasoningDelta {
            mid: None,
            part: Some(0),
            message_id: MessageId("a".into()),
            delta: "§8§ thought".into(),
        };
        let tool = DisplayEvent::ToolInputDelta {
            mid: None,
            part: Some(2),
            tool_call_id: broca_protocol::ToolCallId("call".into()),
            delta: "{\"text\":\"§9§ args\"}".into(),
        };
        let mut script = vec![NormalizedEvent::Display(reasoning.clone())];
        script.extend(
            ["§3", "2§ I have", " read it"].map(|delta| NormalizedEvent::Display(text(delta))),
        );
        script.push(NormalizedEvent::Display(tool.clone()));
        script.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: MessageId("a".into()),
                    content: vec![ContentBlock::Text {
                        text: "§32§ I have read it".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]);
        let model = crate::StubProvider::new(vec![script]);
        let (out, mut events) = tokio::sync::mpsc::channel(256);
        let result = crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            &shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        assert_eq!(result.reason, RunFinishReason::Completed);
        drop(out);
        let mut display = Vec::new();
        while let Some(event) = events.recv().await {
            if let crate::OutputEvent::Display(event) = event {
                display.push(event);
            }
        }
        assert_eq!(display.first(), Some(&reasoning));
        assert_eq!(display.last(), Some(&tool));
        let text_deltas: Vec<_> = display
            .iter()
            .filter_map(|event| match event {
                DisplayEvent::TextDelta { delta, .. } => Some(delta.as_str()),
                _ => None,
            })
            .collect();
        if render || mode3 {
            assert_eq!(text_deltas.concat(), "I have read it", "mode3={mode3}");
        } else {
            assert_eq!(text_deltas, ["§3", "2§ I have", " read it"]);
        }
        assert!(shared.records().iter().any(|record| matches!(record,
            WalRecord::ModelStepFinished { assistant_message, .. }
            if assistant_message.content == vec![ContentBlock::Text { text: "§32§ I have read it".into() }]
        )), "stored reply stays raw");
        writer.shutdown().await.unwrap();
    }
}

async fn drive_frozen_legacy(shared: &Shared) -> PrefixBound {
    let writer = shared.writer();
    let (request, mut control, _) = history_request();
    control.legacy_compaction_fixture = false;
    control.compaction_plane = Some(Arc::new(Plane::default()));
    let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
    let (out, _events) = tokio::sync::mpsc::channel(256);
    crate::run_loop_on_writer(
        request,
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    model
}

#[tokio::test]
async fn compaction_v2_legacy_plan_without_setup_stays_raw_and_never_calls_v1() {
    let shared = Shared::default();
    let model = drive_frozen_legacy(&shared).await;
    assert_eq!(model.0.calls().len(), 1);
    let bytes = crate::canonical_request_bytes(&model.0.calls()[0]);
    assert!(String::from_utf8_lossy(&bytes).contains("first question"));
    assert!(!shared.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionNotice { .. } | WalRecord::CompactionMessage { .. }
    )));
}

#[tokio::test]
async fn compaction_v2_legacy_pending_view_due_is_dropped_without_changing_recorded_request_bytes()
{
    let historical = Shared::default();
    let (model, _) = run_history(&historical).await;
    let expected = crate::canonical_request_bytes(&model.0.calls()[0]);
    let records = historical.records();
    let finished = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let mut prefix = records[..finished].to_vec();
    for record in &mut prefix {
        if let WalRecord::CompactionMessage { view_due, .. } = record {
            *view_due = Some(
                serde_json::from_value(json!({"version":99,"digest":"a".repeat(64)})).unwrap(),
            );
        }
    }
    let original_notices = notice_seqs(&prefix);
    let frozen = Shared::with_records(prefix);
    let model = drive_frozen_legacy(&frozen).await;
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert_eq!(notice_seqs(&frozen.records()), original_notices);
}

#[tokio::test]
async fn compaction_v2_unfinished_old_plan_refuses_without_writes_but_completed_history_can_extend()
{
    let shared = Shared::default();
    drive(&shared, Arc::new(Plane::default())).await.0.unwrap();
    let mut records = shared.records();
    for record in &mut records {
        if let WalRecord::RunStarted {
            compaction_plan_version,
            ..
        } = record
        {
            *compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION - 1);
        }
    }
    let pending = Shared::with_records(
        records
            .iter()
            .take_while(|record| !matches!(record, WalRecord::RunFinished { .. }))
            .cloned()
            .collect(),
    );
    let before = pending.records();
    let (refused, model) = drive(&pending, Arc::new(Plane::default())).await;
    assert!(refused
        .unwrap_err()
        .to_string()
        .contains("compaction_plan_version"));
    assert!(model.0.calls().is_empty());
    assert_eq!(pending.records(), before);
    let finished = Shared::with_records(records);
    let (result, model) = drive_as(
        &finished,
        Arc::new(Plane::default()),
        "next",
        crate::StartMode::AppendEpisode,
    )
    .await;
    result.unwrap();
    assert_eq!(model.0.calls().len(), 1);
    assert!(
        matches!(finished.records().iter().rev().find(|record| matches!(record, WalRecord::RunStarted { .. })).unwrap(), WalRecord::RunStarted { compaction_plan_version:Some(version), .. } if *version == cortexkit_compaction_view::PLAN_VERSION)
    );
}

#[tokio::test]
async fn compaction_v2_unavailable_setup_ends_before_the_model_and_retries_next_send() {
    let shared = Shared::default();
    let plane = Arc::new(Plane {
        unavailable: true,
        ..Default::default()
    });
    let (result, model) = drive(&shared, plane.clone()).await;
    assert_eq!(
        result.unwrap().reason,
        broca_protocol::RunFinishReason::Error
    );
    assert!(model.0.calls().is_empty());
    assert!(shared.records().iter().any(|record| matches!(record, WalRecord::RunFinished { error:Some(error), .. } if error.provider_code.as_deref() == Some("compaction_unavailable"))));
    assert_eq!(
        plane
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "compaction.setup")
            .count(),
        1
    );
    let resumed = Shared::with_records(
        shared
            .records()
            .into_iter()
            .take_while(|record| !matches!(record, WalRecord::RunFinished { .. }))
            .collect(),
    );
    let ready = Arc::new(Plane::default());
    assert!(drive(&resumed, ready.clone()).await.0.is_ok());
    assert_eq!(
        ready
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "compaction.setup")
            .count(),
        1
    );
}

async fn live_view() -> (Vec<WalRecord>, Vec<u8>) {
    let shared = Shared::default();
    let (result, model) = drive(
        &shared,
        Arc::new(Plane {
            view: Some(cut(1)),
            ..Default::default()
        }),
    )
    .await;
    result.unwrap();
    let calls = model.0.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].prompt.len(), 1);
    assert!(
        matches!(&calls[0].prompt[0].content[0], ContentBlock::Text { text } if text == "§3§ second question")
    );
    (shared.records(), crate::canonical_request_bytes(&calls[0]))
}

#[tokio::test]
async fn compaction_v2_crash_after_accepted_before_ack_applies_before_restart_setup() {
    let (records, expected) = live_view().await;
    let start = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    let accepted = records
        .iter()
        .find(|record| matches!(record, WalRecord::CompactionViewV2(_)))
        .unwrap()
        .clone();
    let mut prefix = records[..=start].to_vec();
    prefix.push(accepted);
    let shared = Shared::with_records(prefix);
    let plane = Arc::new(Plane::default());
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    let calls = plane.calls.lock().unwrap();
    let setup = &calls
        .iter()
        .find(|(method, _)| method == "compaction.setup")
        .unwrap()
        .1;
    assert_eq!(setup["applied_view"]["version"], 1);
}

#[tokio::test]
async fn compaction_v2_crash_after_ride_applied_before_request_reuses_identical_view() {
    let (records, expected) = live_view().await;
    let applied = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionViewAppliedV2 { .. }))
        .unwrap();
    let shared = Shared::with_records(records[..=applied].to_vec());
    let plane = Arc::new(Plane {
        view: Some(cut(2)),
        ..Default::default()
    });
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert!(!plane
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(method, _)| method == "compaction.ride"));
}

#[tokio::test]
async fn compaction_v2_crash_after_marker_before_notice_reports_marker_after_setup() {
    let (records, expected) = live_view().await;
    let marker = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionMarker { .. }))
        .unwrap();
    let shared = Shared::with_records(records[..=marker].to_vec());
    let plane = Arc::new(Plane::default());
    let (result, model) = drive(&shared, plane.clone()).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    let calls = plane.calls.lock().unwrap();
    assert_eq!(calls[0].0, "compaction.setup");
    let notice: NoticeRequest = serde_json::from_value(
        calls
            .iter()
            .find(|(method, _)| method == "compaction.notice")
            .unwrap()
            .1
            .clone(),
    )
    .unwrap();
    assert!(notice.events.iter().any(|event| matches!(
        event,
        v2::protocol::notice::Event::MarkerWritten { version: 1 }
    )));
}

/// A template's `part_tag` slot renders the number the session's next request shows: the
/// newest accepted view when it still fits, else the applied one. Message ids are mode-3 ids,
/// and a part is named by its entry part id or by its `<mid>#<id>` read key.
#[tokio::test]
async fn compaction_v2_next_request_numbers_follow_the_view_the_next_request_renders() {
    use v2::protocol::part::PartBody;
    let numbers_of = |records: &[WalRecord]| {
        let frames: Vec<_> = records
            .iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect();
        let state = v2::session::load(&bind(), &frames).unwrap();
        let transcript =
            v2::transcript::project(&frames, &fixture_lineage_id().to_string()).unwrap();
        let (config, _) = v2::host::frozen(&frames).unwrap();
        let conditions = v2::host::conditions(&config, &transcript.entries, false).unwrap();
        (
            v2::shown::next_request(&state, &transcript.entries, &conditions).unwrap(),
            transcript.entries,
        )
    };
    let (mut records, _) = live_view().await;
    let (applied, entries) = numbers_of(&records);
    let question = entries
        .iter()
        .find(|entry| {
            entry.parts.iter().any(
                |part| matches!(&part.body, PartBody::Text { text } if text == "second question"),
            )
        })
        .unwrap();
    assert!(
        question.mid.contains('.'),
        "a mode-3 message id: {}",
        question.mid
    );
    let part = question.parts[0].id.clone();
    assert_eq!(v2::shown::lookup(&applied, &question.mid, &part), Some(3));
    assert_eq!(
        v2::shown::lookup(&applied, &question.mid, &format!("{}#{part}", question.mid)),
        Some(3)
    );
    let first = &entries[0];
    assert_eq!(
        v2::shown::lookup(&applied, &question.mid, &format!("{}#{part}", first.mid)),
        None,
        "a read key naming another message matches nothing"
    );
    assert_eq!(
        v2::shown::lookup(&applied, &first.mid, &first.parts[0].id),
        None,
        "the applied view cuts the first message"
    );

    // A newer accepted view without the cut renders at the next boundary, so its numbers count.
    let newer = View::seal(2, vec![]);
    records.push(WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
        version: 2,
        checksum: newer.checksum.clone(),
        edits: v2::storage::encode(None, &newer.into()),
    }));
    let (pending, _) = numbers_of(&records);
    assert_eq!(
        v2::shown::lookup(&pending, &first.mid, &first.parts[0].id),
        Some(1)
    );
    assert_eq!(v2::shown::lookup(&pending, &question.mid, &part), Some(3));
}

/// The provider measures a session's fill only from `conditions.last_prompt_tokens`. It is
/// absent before any request has reported its input, then every later notice and ride states
/// the newest reported prompt: uncached input plus cache reads plus cache writes.
#[tokio::test]
async fn compaction_v2_notices_and_rides_state_the_newest_reported_prompt_size() {
    use broca_protocol::{NormalizedEvent, ProviderControl};
    let reported = vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("a".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(700),
                cached_input_tokens: Some(200),
                cache_write_tokens: Some(100),
                output_tokens: Some(5),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ];
    let prompt_sizes = |calls: &[(String, Value)]| -> Vec<Option<u64>> {
        calls
            .iter()
            .filter(|(method, _)| method == "compaction.notice" || method == "compaction.ride")
            .map(|(_, params)| params["conditions"]["last_prompt_tokens"].as_u64())
            .collect()
    };
    let shared = Shared::default();
    let plane = Arc::new(Plane::default());
    let (first, _) = drive_events(
        &shared,
        plane.clone(),
        "run-1",
        crate::StartMode::FreshOrResume,
        reported,
    )
    .await;
    first.unwrap();
    let before = plane.calls.lock().unwrap().len();
    let sizes = prompt_sizes(&plane.calls.lock().unwrap());
    assert!(!sizes.is_empty(), "the first run sends a notice");
    assert_eq!(sizes[0], None, "no request has reported its input yet");

    let (second, _) = drive_as(
        &shared,
        plane.clone(),
        "run-2",
        crate::StartMode::FreshOrResume,
    )
    .await;
    second.unwrap();
    let calls = plane.calls.lock().unwrap();
    let sizes = prompt_sizes(&calls[before..]);
    assert!(!sizes.is_empty(), "the second run sends a notice or ride");
    assert!(
        sizes.iter().all(|size| *size == Some(1_000)),
        "every later notice and ride states the newest reported prompt: {sizes:?}"
    );
}

#[tokio::test]
async fn compaction_v2_crash_mid_step_does_not_adopt_a_newer_accepted_view() {
    let (records, expected) = live_view().await;
    let marker = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionMarker { .. }))
        .unwrap();
    let mut prefix = records[..=marker].to_vec();
    let newer = View::seal(2, vec![]);
    prefix.push(WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
        version: 2,
        checksum: newer.checksum.clone(),
        edits: v2::storage::encode(None, &newer.into()),
    }));
    let frames: Vec<_> = prefix
        .iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record: record.clone(),
        })
        .collect();
    let shown = v2::shown::at_step(
        &frames,
        &bind(),
        &fixture_lineage_id().to_string(),
        "run",
        1,
    )
    .unwrap();
    assert_eq!(shown.keys().copied().collect::<Vec<_>>(), [3]);
    assert_eq!(shown[&3].payload_position, 2);
    let shared = Shared::with_records(prefix);
    let (result, model) = drive(&shared, Arc::new(Plane::default())).await;
    result.unwrap();
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        expected
    );
    assert!(!shared.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionViewAppliedV2 { version: 2, .. }
    )));
}

#[tokio::test]
async fn compaction_v2_no_plan_globals_are_frozen_outside_input_and_resume_byte_identically() {
    async fn send(shared: &Shared) -> PrefixBound {
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.input = vec![
            text(Role::System, "global instructions"),
            text(Role::User, "question"),
        ];
        control.manifest = None;
        control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
        let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
        let (out, _events) = tokio::sync::mpsc::channel(256);
        crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            shared,
            None,
            control,
            &out,
        )
        .await
        .unwrap();
        writer.shutdown().await.unwrap();
        model
    }
    let live = Shared::default();
    let model = send(&live).await;
    let calls = model.0.calls();
    assert_eq!(calls[0].prompt[0].role, Role::System);
    assert_eq!(calls[0].prompt[0].ordinal, None);
    assert_eq!(calls[0].prompt[1].ordinal, Some(0));
    let records = live.records();
    let WalRecord::RunStarted { input, config, .. } = records
        .iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(input.len(), 1);
    assert_eq!(input[0].role, Role::User);
    assert_eq!(config.global_system.as_ref().unwrap()[0].role, Role::System);
    let before_model = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let resumed = Shared::with_records(records[..before_model].to_vec());
    let model = send(&resumed).await;
    assert_eq!(
        crate::canonical_request_bytes(&model.0.calls()[0]),
        crate::canonical_request_bytes(&calls[0])
    );
}

#[tokio::test]
async fn compaction_v2_every_result_kind_carries_delivery_without_changing_stored_output() {
    use crate::delivery_order::OrderedSend;
    use broca_protocol::{DeliveryMode, DeliveryOrder, SubmissionId, ToolOutput};
    let outputs = [
        ToolOutput::Text {
            text: "value".into(),
        },
        ToolOutput::Json {
            value: json!({"value":1}),
        },
        ToolOutput::Parts {
            parts: vec![broca_protocol::ToolOutputPart::Text {
                text: "value".into(),
            }],
        },
        ToolOutput::Denied {
            reason: "denied".into(),
        },
        ToolOutput::error("failed"),
    ];
    for compaction in [true, false] {
        for output in outputs.iter().cloned() {
            let shared = Shared::default();
            let writer = shared.writer();
            let (mut request, mut control, _) = history_request();
            request.input = vec![text(Role::User, "question")];
            request.context_limit = Some(100_000);
            control.admitted_context_limit = request.context_limit;
            request.tools = vec![serde_json::from_value(
                json!({"name":"edit","description":"edit","input_schema":{"type":"object"}}),
            )
            .unwrap()];
            request.policy_allowed.insert("edit".into());
            control.new_log_compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
            control.compaction_plane = Some(Arc::new(Plane::default()));
            if !compaction {
                control.manifest = None;
            }
            let inbox = crate::SteerInbox::new();
            inbox.seed_ordered(vec![OrderedSend {
                admitted_at_ms: Some(1),
                submission_id: SubmissionId("reminder".into()),
                order: DeliveryOrder {
                    modes: vec![DeliveryMode::ToolResult],
                    dedupe_key: None,
                    expires_at_ms: None,
                    template: None,
                },
                principal: None,
                input: vec![text(Role::User, "module note")],
            }]);
            control.steer = Some(inbox);
            let model = PrefixBound(crate::StubProvider::new(vec![
                tool_call_events("edit", 1),
                model_events(),
            ]));
            let tool_output = output.clone();
            let tools = crate::StubToolPlane::new().with_tool("edit", move |_| {
                crate::StubReply::Output(tool_output.clone())
            });
            let (out, _events) = tokio::sync::mpsc::channel(256);
            crate::run_loop_on_writer(
                request,
                &writer,
                crate::StartMode::FreshOrResume,
                &model,
                &tools,
                &crate::AllowAll,
                &shared,
                None,
                control,
                &out,
            )
            .await
            .unwrap();
            writer.shutdown().await.unwrap();
            let calls = model.0.calls();
            assert_eq!(calls.len(), 2);
            assert!(serde_json::to_string(&calls[1].prompt)
                .unwrap()
                .contains("module note"));
            let records = shared.records();
            assert_eq!(
                records
                    .iter()
                    .filter(|record| matches!(record, WalRecord::RunStarted { .. }))
                    .count(),
                1
            );
            assert!(records.iter().any(|record| matches!(record, WalRecord::ToolResult { result, .. } if result.output == output)));
            let frames: Vec<_> = records
                .iter()
                .enumerate()
                .map(|(index, record)| FramedRecord {
                    seq: index as u64 + 1,
                    fence: 1,
                    record: record.clone(),
                })
                .collect();
            let transcript =
                v2::transcript::project(&frames, &fixture_lineage_id().to_string()).unwrap();
            let tool = transcript
                .entries
                .iter()
                .flat_map(|entry| &entry.parts)
                .find_map(|part| match &part.body {
                    v2::protocol::part::PartBody::Tool(tool) => Some(tool),
                    _ => None,
                })
                .unwrap();
            assert_eq!(tool.delivered[0].text.as_deref(), Some("module note"));
            assert_eq!(tool.delivered[0].sender, "broca");
            assert!(!tool
                .output
                .as_deref()
                .unwrap_or_default()
                .contains("module note"));
            if !compaction {
                assert!(
                    !String::from_utf8_lossy(&crate::canonical_request_bytes(&calls[1]))
                        .contains('§')
                );
                assert!(v2::shown::at_step(
                    &frames,
                    &bind(),
                    &fixture_lineage_id().to_string(),
                    "run",
                    2
                )
                .unwrap()
                .is_empty());
            }
        }
    }
}

#[tokio::test]
async fn compaction_v2_recorded_request_rebuild_uses_the_applied_view_not_a_later_acceptance() {
    let shared = Shared::default();
    let (_, model) = drive(
        &shared,
        Arc::new(Plane {
            view: Some(cut(1)),
            ..Default::default()
        }),
    )
    .await;
    let live = model.0.calls()[0].prompt.clone();
    let mut records = shared.records();
    let attempt_at = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
        .unwrap();
    let newer = View::seal(2, vec![]);
    records.insert(
        attempt_at,
        WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
            version: 2,
            checksum: newer.checksum.clone(),
            edits: v2::storage::encode(None, &newer.into()),
        }),
    );
    records.insert(attempt_at + 1, serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"request_body_len":1,"request_body_sha256":"recorded-digest","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap());
    let request = crate::cache_warm::last_recorded_request_with_lineage(
        &records,
        Some(&fixture_lineage_id().to_string()),
    )
    .unwrap();
    assert_eq!(request.opts.prompt, live);
    assert_eq!(request.request_body_sha256, "recorded-digest");
}

#[tokio::test]
async fn compaction_v2_missing_context_limit_refuses_fresh_admission_and_stops_an_invalid_resume() {
    async fn without_limit(
        shared: &Shared,
        mode3: bool,
    ) -> (
        Result<crate::RunSummary, crate::RunError>,
        PrefixBound,
        Arc<Plane>,
    ) {
        let writer = shared.writer();
        let (mut request, mut control, _) = history_request();
        request.context_limit = None;
        control.legacy_compaction_fixture = false;
        control.new_log_compaction_plan_version =
            mode3.then_some(cortexkit_compaction_view::PLAN_VERSION);
        let plane = Arc::new(Plane::default());
        control.compaction_plane = Some(plane.clone());
        let model = PrefixBound(crate::StubProvider::new(vec![model_events()]));
        let (out, _events) = tokio::sync::mpsc::channel(256);
        let result = crate::run_loop_on_writer(
            request,
            &writer,
            crate::StartMode::FreshOrResume,
            &model,
            &crate::StubToolPlane::new(),
            &crate::AllowAll,
            shared,
            None,
            control,
            &out,
        )
        .await;
        writer.shutdown().await.unwrap();
        (result, model, plane)
    }
    let fresh = Shared::default();
    let (result, model, plane) = without_limit(&fresh, true).await;
    let error = result.unwrap_err();
    assert!(matches!(
        error,
        crate::RunError::CompactionContextLimitMissing
    ));
    assert_eq!(
        error.admission_error_class(),
        broca_protocol::ErrorClass::Permanent
    );
    assert!(fresh.records().is_empty());
    assert!(model.0.calls().is_empty());
    assert!(plane.calls.lock().unwrap().is_empty());
    let legacy = Shared::default();
    let (result, model, _) = without_limit(&legacy, false).await;
    result.unwrap();
    assert_eq!(
        model.0.calls().len(),
        1,
        "legacy admission must not acquire a v2 prerequisite"
    );
    let valid = Shared::default();
    drive(&valid, Arc::new(Plane::default())).await.0.unwrap();
    let mut prefix: Vec<_> = valid
        .records()
        .into_iter()
        .take_while(|record| !matches!(record, WalRecord::ModelStepFinished { .. }))
        .collect();
    for record in &mut prefix {
        if let WalRecord::RunStarted { config, .. } = record {
            config.context_limit = None;
        }
    }
    let invalid = Shared::with_records(prefix);
    let (result, model, plane) = without_limit(&invalid, true).await;
    assert_eq!(result.unwrap().reason, RunFinishReason::Error);
    assert!(model.0.calls().is_empty());
    assert!(plane.calls.lock().unwrap().is_empty());
    assert!(invalid.records().iter().any(|record| matches!(record, WalRecord::RunFinished { error:Some(error), .. } if error.provider_code.as_deref() == Some("compaction_unavailable") && error.message.contains("context_limit"))));
}

#[tokio::test]
async fn compaction_v2_context_folds_refreezes_and_tracks_accepted_attempts_before_step_completion()
{
    let shared = Shared::default();
    drive(&shared, Arc::new(Plane::default())).await.0.unwrap();
    let mut start = shared
        .records()
        .into_iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap();
    if let WalRecord::RunStarted { config, .. } = &mut start {
        config.auth_selection = Some(routed_account_selection("old"));
    }
    let attempt: WalRecord = serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"request_body_len":1,"request_body_sha256":"request","response_body_len":0,"response_body_sha256":"response","outcome":"complete","http_status":200,"ts_ms":10})).unwrap();
    let mut next = start.clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut next {
        *run_id = broca_protocol::RunId("next".into());
    }
    let mut records = vec![
        start,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        attempt.clone(),
        serde_json::from_value(
            json!({"type":"run_finished","reason":"error","usage":{},"retries_used":0}),
        )
        .unwrap(),
        next,
        WalRecord::RunConfigRefrozen {
            ts_ms: Some(11),
            renderer_version: 1,
            frozen_render_config: json!({"service_tier":"priority"}),
        },
        WalRecord::RunAuthRefrozen {
            ts_ms: Some(12),
            auth_selection: routed_account_selection("new"),
        },
    ];
    let frames = |records: &[WalRecord]| {
        records
            .iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record: record.clone(),
            })
            .collect::<Vec<_>>()
    };
    let projected = frames(&records);
    let previous = v2::host::previous(&projected).unwrap();
    let (config, manifest) = v2::host::frozen(&projected).unwrap();
    let current = v2::host::context(&config, manifest);
    assert_ne!(previous.account, current.account);
    assert_eq!(current.model.service_tier.as_deref(), Some("fast"));
    assert_eq!(
        v2::conditions::ride_causes(
            Some(&previous),
            &current,
            true,
            false,
            0,
            &v2::host::conditions(&config, &[], false).unwrap()
        ),
        vec![
            v2::protocol::ride::RideCause::TierChange,
            v2::protocol::ride::RideCause::AccountChange
        ]
    );
    records.push(WalRecord::StepStarted {
        step_id: broca_protocol::StepId(1),
    });
    records.push(attempt);
    assert_eq!(v2::host::previous(&frames(&records)).unwrap(), current);
}
