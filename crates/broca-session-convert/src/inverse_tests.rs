use broca_wal::{FramedRecord, WalRecord};

use super::inverse::{comparable, first_difference, invert, without_nulls};
use crate::convert;

fn log(lines: &[&str]) -> Vec<FramedRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|e| panic!("fixture line {i}: {e}")),
        })
        .collect()
}

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"bash","description":"run","input_schema":{"type":"object"},"execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}}"#;

fn assert_round_trip(old: &[FramedRecord]) {
    let converted = convert(old, None).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    let rebuilt: Vec<_> = invert(&converted.records, &converted.parts)
        .unwrap()
        .iter()
        .map(without_nulls)
        .collect();
    let original = comparable(old);
    assert_eq!(rebuilt.len(), original.len());
    for (i, (a, b)) in original.iter().zip(&rebuilt).enumerate() {
        assert_eq!(first_difference(a, b, ""), None, "record {i}");
    }
}

#[test]
fn a_tool_step_and_its_run_round_trip_exactly() {
    assert_round_trip(&log(&[
        &RUN.replace("RUN", "run-1"),
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{"input_tokens":3},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"unknown"}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        r#"{"type":"turn_finished","step_id":1}"#,
        r#"{"type":"input_token_cap_frozen","limit":5}"#,
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
        &RUN.replace("RUN", "run-2"),
    ]));
}

#[test]
fn changed_dispatch_arguments_round_trip() {
    assert_round_trip(&log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{},"retries_used":0}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls -la"}}"#,
    ]));
}

const STEP: &str = r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"text","text":"ok"}]},"usage":{},"retries_used":0}"#;
const STEER: &str = r#"{"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"also this"}]}],"written_at_ms":7}"#;

/// What a read's display facts are decided from round-trips: a steer's sender and request
/// identity, a queued send's sender, principal and request identity, and why a call was never
/// sent. The facts the conversion records from them name the steer's sender on the message it
/// placed and the refused call's reason on its turn, as today's read serves them.
#[test]
fn what_display_facts_are_decided_from_round_trips() {
    use broca_session_store::record::{Record, Unanswered};
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"also this"}]}],"written_at_ms":7,"sent_by":{"kind":"reserved","module_id":"occipital"},"request_identity_digest":"digest-steer"}"#,
        r#"{"type":"queued","submission_id":"q1","sent_by":{"kind":"reserved","module_id":"occipital"},"principal":{"kind":"reserved","module_id":"prefrontal-core"},"request_identity_digest":"digest-queued","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[]}}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_no_dispatch","batch_id":"b1","tool_call_id":"c1","reason":{"kind":"denied","reason":"not allowed"}}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        r#"{"type":"step_started","step_id":2}"#,
    ]);
    assert_round_trip(&old);
    let converted = convert(&old, Some("lineage-1")).unwrap();
    let turn = converted
        .records
        .iter()
        .find_map(|record| match record {
            Record::TurnFacts(turn) => Some(turn),
            _ => None,
        })
        .expect("the tool turn has facts");
    assert_eq!(turn.ordinal, 2);
    assert_eq!(turn.calls.len(), 1);
    assert_eq!(turn.calls[0].unanswered, Some(Unanswered::Denied));
    let steer = converted
        .records
        .iter()
        .find_map(|record| match record {
            Record::EntryFacts(facts) if facts.ordinal == 4 => Some(facts),
            _ => None,
        })
        .expect("the steer's message has facts");
    assert_eq!(steer.sender.as_deref(), Some("occipital"));
}

/// A steer arrives during a model step and is placed at the next step boundary: its entry moves
/// there, names the steer's record, and both round-trip.
#[test]
fn a_steer_is_recorded_where_it_is_placed_and_round_trips() {
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"step_started","step_id":1}"#,
        STEER,
        STEP,
        r#"{"type":"step_started","step_id":2}"#,
    ]);
    let converted = convert(&old, None).unwrap();
    let names: Vec<_> = converted.records.iter().map(|r| r.name()).collect();
    assert_eq!(
        names,
        [
            "SetupStub",
            "RunStart",
            "Input",
            "EntryFacts",
            "StepStart",
            "Steer",
            "Assistant",
            "StepEnd",
            "EntryFacts",
            "Input",
            "StepStart",
            "EntryFacts",
        ]
    );
    let broca_session_store::record::Record::Input(steer) = &converted.records[9] else {
        panic!("the placed steer");
    };
    assert_eq!(
        steer.receipt_seq,
        Some(6),
        "it names the steer's record, at seq 6, after the input's entry facts"
    );
    assert_round_trip(&old);
}

/// A steer no boundary placed never reached the conversation, so it gets no transcript copy;
/// its record keeps the messages either way.
#[test]
fn an_unplaced_steer_gets_no_copy_and_round_trips() {
    let old = log(&[&RUN.replace("RUN", "r"), STEER]);
    let converted = convert(&old, None).unwrap();
    assert!(!converted.records.iter().any(
        |r| matches!(r, broca_session_store::record::Record::Input(i) if i.receipt_seq.is_some())
    ));
    assert_round_trip(&old);
}

/// A copy that no longer equals the message its record kept refuses.
#[test]
fn a_placed_copy_that_differs_from_its_record_refuses() {
    use broca_protocol::content::ContentBlock;
    use broca_session_store::record::Record;
    let old = log(&[
        &RUN.replace("RUN", "r"),
        STEER,
        r#"{"type":"step_started","step_id":1}"#,
    ]);
    let mut converted = convert(&old, None).unwrap();
    let Some(Record::Input(copy)) = converted
        .records
        .iter_mut()
        .find(|r| matches!(r, Record::Input(i) if i.receipt_seq.is_some()))
    else {
        panic!("the placed copy");
    };
    copy.message.content = vec![ContentBlock::Text {
        text: "changed".into(),
    }];
    let error = invert(&converted.records, &converted.parts).unwrap_err();
    assert!(error.contains("steered"), "{error}");
}

#[test]
fn a_changed_value_is_named_by_path_only() {
    let a = serde_json::json!({"x": {"y": [1, {"z": "secret"}]}});
    let b = serde_json::json!({"x": {"y": [1, {"z": "other"}]}});
    let path = first_difference(&a, &b, "").unwrap();
    assert_eq!(path, ".x.y[1].z (value)");
    assert!(!path.contains("secret"));
}

#[test]
fn an_unclaimed_transcript_record_refuses() {
    let converted = convert(
        &log(&[
            &RUN.replace("RUN", "r"),
            r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[]},"usage":{},"retries_used":0}"#,
        ]),
        None,
    )
    .unwrap();
    // Without its step end, the assistant message has no anchor to claim it.
    let records: Vec<_> = converted
        .records
        .into_iter()
        .filter(|r| r.name() != "StepEnd")
        .collect();
    let error = invert(&records, &converted.parts).unwrap_err();
    assert!(error.contains("unclaimed"), "{error}");
}

/// Records of the current compaction contract become typed records and round-trip: an accepted
/// view with its stored edits, the view a step applied, and the marker of the provider's cut.
#[test]
fn current_compaction_records_round_trip() {
    use broca_session_store::record::Record;
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"compaction_view_v2","version":3,"checksum":"c3","edits":[{"kind":"full","edit":{"op":"drop","mid":"m1"}},{"kind":"previous","index":0}]}"#,
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"compaction_view_applied_v2","step_id":1,"version":3,"checksum":"c3"}"#,
        r#"{"type":"compaction_marker","harness":"h","session":"s","view_version":3,"first_kept_mid":"m2","first_kept_ordinal":2,"checkpoint":7}"#,
        STEP,
    ]);
    let converted = convert(&old, None).unwrap();
    let kinds: Vec<_> = converted
        .records
        .iter()
        .filter(|record| {
            matches!(
                record,
                Record::View(_) | Record::ViewApplied(_) | Record::Marker(_)
            )
        })
        .map(|record| record.name())
        .collect();
    assert_eq!(kinds, ["View", "ViewApplied", "Marker"]);
    assert_round_trip(&old);
}

/// A tool batch carrying a queued send's text: `delivery` is the delivery record, written after
/// the batch finishes, as today's loop writes it.
fn ride_log(delivery: &str) -> Vec<FramedRecord> {
    log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"delivery_ordered","submission_id":"sub-q","order":{"modes":["tool_result","turn_system","steer"]}}"#,
        r#"{"type":"queued","submission_id":"sub-q","send_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"note"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"written_at_ms":5}"#,
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{"cmd":"ls"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
        r#"{"type":"tool_batch_started","batch_id":"b1","step_id":1,"planned_calls":["c1"]}"#,
        r#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"c1","args":{"cmd":"ls"}}"#,
        r#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"c1","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}"#,
        r#"{"type":"tool_batch_finished","batch_id":"b1"}"#,
        delivery,
        r#"{"type":"turn_finished","step_id":1}"#,
    ])
}

/// The ride envelope today's writer records since llm-runner 0.8.11 becomes the append's own
/// envelope, and goes back into today's `ride` member whole.
#[test]
fn a_ride_envelope_converts_onto_the_append_and_round_trips() {
    use broca_session_store::record::{Record, RideEnvelope, RideForm};
    let old = ride_log(
        r#"{"type":"send_delivered","submission_id":"sub-q","run_id":"r","mode":"tool_result","tool_call_id":"c1","appended_text":"\n\nnote","written_at_ms":5,"ride":{"call_key":"lineage:7","send_id":"q","sender":"broca","forms":["tool_result","turn_system"],"form":"append"}}"#,
    );
    let converted = convert(&old, None).unwrap();
    let appends: Vec<_> = converted
        .records
        .iter()
        .filter_map(|record| match record {
            Record::ResultAppend(append) => Some(append),
            _ => None,
        })
        .collect();
    assert_eq!(appends.len(), 1);
    assert_eq!(appends[0].send_id.as_deref(), Some("q"));
    assert_eq!(
        appends[0].ride,
        Some(RideEnvelope {
            call_key: "lineage:7".into(),
            sender: "broca".into(),
            forms: vec!["tool_result".into(), "turn_system".into()],
            form: RideForm::Append,
        })
    );
    assert_round_trip(&old);
}

/// A ride recorded before envelopes existed converts with no envelope and no send id, and
/// round-trips without gaining a `ride` member.
#[test]
fn a_ride_without_an_envelope_converts_without_one_and_round_trips() {
    use broca_session_store::record::Record;
    let old = ride_log(
        r#"{"type":"send_delivered","submission_id":"sub-q","run_id":"r","mode":"tool_result","tool_call_id":"c1","appended_text":"\n\nnote","written_at_ms":5}"#,
    );
    let converted = convert(&old, None).unwrap();
    let append = converted
        .records
        .iter()
        .find_map(|record| match record {
            Record::ResultAppend(append) => Some(append),
            _ => None,
        })
        .expect("the delivery's append");
    assert_eq!(
        (append.send_id.as_ref(), append.ride.as_ref()),
        (None, None)
    );
    assert_round_trip(&old);
}

/// A steer's and a queued send's images round-trip. Today's log keeps their bytes in its own
/// media store, so the converted records name no stored object; and where the new storage stored
/// them (`media`), that is the new storage's own bookkeeping, which today's records don't carry:
/// records naming stored objects invert to the same records of today's log.
#[test]
fn a_sends_images_round_trip_and_where_the_store_keeps_them_stays_its_own() {
    use broca_session_store::record::{Record, SendMedia};
    let image = r#"{"type":"media","kind":"image","mime":"image/png","byte_size":3,"sha256":"0000000000000000000000000000000000000000000000000000000000000001","width":1,"height":1}"#;
    let steer = format!(
        r#"{{"type":"steered","input":[{{"role":"user","content":[{{"type":"text","text":"also this"}},{image}]}}],"written_at_ms":7}}"#
    );
    let queued = format!(
        r#"{{"type":"queued","submission_id":"q1","request":{{"model":{{"provider_module_id":"p","model_id":"m"}},"input":[{{"role":"user","content":[{{"type":"text","text":"later"}},{image}]}}]}}}}"#
    );
    let old = log(&[
        &RUN.replace("RUN", "r"),
        r#"{"type":"step_started","step_id":1}"#,
        &steer,
        &queued,
        STEP,
        r#"{"type":"step_started","step_id":2}"#,
    ]);
    assert_round_trip(&old);
    let mut converted = convert(&old, None).unwrap();
    let stored = SendMedia {
        message: 0,
        block: 1,
        part: None,
        log: broca_session_store::codec::Id128([1; 16]),
        generation: broca_session_store::codec::Id128([2; 16]),
        seq: 9,
    };
    let mut named = 0;
    for record in &mut converted.records {
        let media = match record {
            Record::QueuedSend(send) => &mut send.media,
            Record::Steer(steer) => &mut steer.media,
            _ => continue,
        };
        assert!(media.is_empty(), "a converted send names no stored object");
        media.push(stored.clone());
        named += 1;
    }
    assert_eq!(named, 2);
    let rebuilt: Vec<_> = invert(&converted.records, &converted.parts)
        .unwrap()
        .iter()
        .map(without_nulls)
        .collect();
    let original = comparable(&old);
    assert_eq!(rebuilt.len(), original.len());
    for (i, (a, b)) in original.iter().zip(&rebuilt).enumerate() {
        assert_eq!(first_difference(a, b, ""), None, "record {i}");
    }
}

/// A refresh, a policy change and a flush keep records of their own, and the run start that
/// applied the refresh is followed, after its input, by the application today's fold decides.
/// Going back, the application is dropped: today's log keeps none, and decides it again on
/// replay.
#[test]
fn plan_changes_round_trip_and_record_their_application() {
    use broca_session_store::fold::operations::Operations;
    use broca_session_store::record::{Record, Stream};
    let manifest = |identity: &str| {
        serde_json::to_value(broca_wal::PlanManifest {
            rich_ui: None,
            plan: broca_wal::OpaqueJson::from_json_text(format!(r#"{{"id":"{identity}"}}"#))
                .unwrap(),
            plan_identity: identity.into(),
            composition_digest: format!("composition-{identity}"),
            fetches: Vec::new(),
            tools: std::sync::Arc::new(Vec::new()),
            system_text: format!("system of {identity}"),
            system_digest: "s".into(),
            join_version: 1,
            role_versions: Default::default(),
            declarations: Vec::new(),
        })
        .unwrap()
    };
    let run = |run: &str, identity: &str| {
        let mut value: serde_json::Value = serde_json::from_str(&RUN.replace("RUN", run)).unwrap();
        value["manifest"] = manifest(identity);
        value.to_string()
    };
    let refresh = serde_json::json!({
        "type": "session_refresh_accepted", "generation": 1, "policy": "on_prefix_rebuild",
        "manifest": manifest("plan-2"), "run_in_progress": false,
        "applies_from_next_run": false,
    })
    .to_string();
    let old = log(&[
        &run("r1", "plan-1"),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
        &refresh,
        r#"{"type":"session_refresh_policy_changed","generation":1,"policy":"immediately","send_id":"policy-1"}"#,
        r#"{"type":"session_prefix_flushed","send_id":"flush-1","pending_generation":1}"#,
        &run("r2", "plan-2"),
    ]);
    let converted = convert(&old, None).unwrap();
    let ledger: Vec<_> = converted
        .records
        .iter()
        .filter(|record| record.stream() == Stream::Ledger)
        .map(|record| record.name())
        .collect();
    assert_eq!(
        ledger,
        [
            "RunStart",
            "RunEnd",
            "Billed",
            "SessionRefresh",
            "RefreshPolicy",
            "PrefixFlush",
            "RunStart",
            "RefreshApplied"
        ]
    );
    let second = converted
        .records
        .iter()
        .rposition(|record| matches!(record, Record::RunStart(_)))
        .unwrap();
    assert!(matches!(converted.records[second + 1], Record::Input(_)));
    let Record::RefreshApplied(applied) = &converted.records[second + 2] else {
        panic!("{:?}", converted.records[second + 2]);
    };
    let today = broca_fetch_plan::session_change::SessionChangeState::fold(&old);
    let effective = today.effective.unwrap();
    assert_eq!(
        (applied.generation, applied.applied_by.as_str()),
        (1, "fold")
    );
    assert_eq!(applied.manifest_digest, effective.manifest_digest);
    assert_eq!(
        applied.composition_digest.as_deref(),
        Some("composition-plan-2")
    );
    let mut operations = Operations::default();
    for record in converted
        .records
        .iter()
        .filter(|record| record.stream() == Stream::Ledger)
    {
        operations.apply(record).unwrap();
    }
    let plan = operations.plan.unwrap();
    assert_eq!((plan.accepted_generation, plan.applied_generation), (1, 1));
    assert_eq!(plan.pending, None);
    assert!(!plan.flushed_since_accept && !plan.rebuild_since_accept);
    assert_eq!(plan.effective.unwrap().generation, 1);
    assert_round_trip(&old);
}

/// A session that enrolled in the current compaction contract on the new storage can't be
/// converted back: today's storage keeps one layout for a log's whole life.
#[test]
fn an_enrolled_session_does_not_convert_back() {
    let converted = convert(&log(&[RUN]), None).unwrap();
    let mut records = converted.records.clone();
    records.push(broca_session_store::record::Record::CompactionEnrolled(
        broca_session_store::record::CompactionEnrolled {
            through_ordinal: 1,
            tag_floor: 0,
            tag_high: 1,
            overlay: broca_session_store::record::EnrollmentOverlayRef {
                log: broca_session_store::codec::Id128([1; 16]),
                generation: broca_session_store::codec::Id128([2; 16]),
                seq: 3,
            },
        },
    ));
    let refused = invert(&records, &converted.parts).unwrap_err();
    assert!(
        refused.contains("enrolled in the current compaction contract"),
        "{refused}"
    );
    invert(&converted.records, &converted.parts).expect("the same log without it converts back");
}

/// A moved session's `Moved` record has no record of today's: the session converts back to the
/// log it was moved from, record for record.
#[test]
fn a_moved_session_converts_back_to_its_log() {
    let converted = convert(&log(&[RUN]), None).unwrap();
    let mut records = converted.records.clone();
    records.push(broca_session_store::record::Record::Moved(
        broca_session_store::record::Moved {
            move_id: "store-move:test".into(),
            source_activity_ms: 5,
        },
    ));
    assert_eq!(
        invert(&records, &converted.parts).unwrap(),
        invert(&converted.records, &converted.parts).unwrap()
    );
}
