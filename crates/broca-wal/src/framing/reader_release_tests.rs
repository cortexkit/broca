//! Records written by the builds that introduce `auth-selection-method/v1` and
//! `tool-schema-pins/v1`, read by a build that knows both names. A fresh admission that
//! routing chose an account for freezes that credential's auth method beside its vault id, and
//! a resumed run re-freezes the method it already carried. Each
//! record here is pinned as the bytes those builds write: it must decode on this build,
//! re-encode to exactly the same bytes and keep its feature requirements. A reader whose
//! feature table lacks the two names (every build before this one) must refuse it by name.

use super::*;

#[test]
fn compaction_v2_records_and_plan_version_round_trip_exact_bytes() {
    let records: Vec<WalRecord> = include_str!("../../tests/fixtures/compaction_v2_records.jsonl")
        .lines().map(|pinned| {
            let record: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            assert_eq!(record.required_features(), [COMPACTION_V2_FEATURE]);
            let bytes = encode(7, 3, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let payload = format!("{{\"requires\":[\"compaction-v2/v1\"],\"record\":{pinned}}}");
            assert_eq!(&bytes[HEADER_LEN..], payload.as_bytes());
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { framed, consumed } if consumed == bytes.len() && framed.record == record));
            let value = serde_json::to_value(&record).unwrap();
            let from_value: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(&from_value).unwrap(), value);
            record
        }).collect();
    let WalRecord::CompactionViewV2(view) = &records[0] else {
        panic!("view decoded as another record")
    };
    assert_eq!(view.version, 2);
    assert_eq!(view.checksum, "w7-provider-checksum");
    assert!(matches!(
        view.edits[0],
        crate::StoredViewEdit::Previous { index: 1 }
    ));
    let crate::StoredViewEdit::Full { edit } = &view.edits[1] else {
        panic!("full edit lost")
    };
    assert_eq!(
        edit.get(),
        r#"{"since":2, "vendor":{"z":1e+02,"a":"\u0061"},"text":"summary"}"#
    );
    assert!(matches!(
        view.edits[2],
        crate::StoredViewEdit::Previous { index: 0 }
    ));
    assert!(
        matches!(&records[1], WalRecord::CompactionMarker { harness, session, view_version: 2, first_kept_mid, first_kept_ordinal: 7, checkpoint: 3 } if harness == "runner" && session == "s" && first_kept_mid == "m7")
    );
    assert!(
        matches!(&records[2], WalRecord::CompactionViewAppliedV2 { step_id: 4, version: 2, checksum } if checksum == "w7-provider-checksum")
    );
    assert!(matches!(
        records[3],
        WalRecord::RunStarted {
            compaction_plan_version: Some(1),
            ..
        }
    ));
}

#[test]
fn a_reader_without_compaction_v2_refuses_all_four_shapes_by_name() {
    let older: Vec<_> = KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != COMPACTION_V2_FEATURE)
        .collect();
    for pinned in include_str!("../../tests/fixtures/compaction_v2_records.jsonl").lines() {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        let bytes = encode(1, 1, &record).unwrap();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == ["compaction-v2/v1"])
        );
    }
}

#[test]
fn compaction_plan_version_unions_with_other_record_requirements_even_when_zero() {
    let mut record: WalRecord = serde_json::from_str(PINNED_SCHEMA_PIN_RUN_STARTED).unwrap();
    let mut expected = record.required_features().to_vec();
    expected.push(COMPACTION_V2_FEATURE);
    expected.sort_unstable();
    if let WalRecord::RunStarted {
        compaction_plan_version,
        ..
    } = &mut record
    {
        assert_eq!(*compaction_plan_version, None);
        *compaction_plan_version = Some(0);
    }
    assert_eq!(record.required_features(), expected);
    let bytes = encode(1, 1, &record).unwrap();
    assert_eq!(bytes[4], GATED_FRAMING_VERSION);
    assert!(
        matches!(decode_next(&bytes), DecodeStep::Record { framed, .. } if framed.record == record)
    );
}

/// A `RunStarted` whose plan manifest froze the catalog's schema pin for `read`, as the build
/// that writes `tool-schema-pins/v1` serializes it: the pins follow the provider's other
/// declarations inside `declarations_frozen`, keyed by tool name.
const PINNED_SCHEMA_PIN_RUN_STARTED: &str = r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"manifest":{"plan":"{}","plan_identity":"0000000000000000000000000000000000000000000000000000000000000000","composition_digest":"0000000000000000000000000000000000000000000000000000000000000000","fetches":[{"provider":"aft","catalog_digest":"0000000000000000000000000000000000000000000000000000000000000000","tools":[{"name":"read","description":"Read a file","input_schema":{"properties":{},"type":"object"},"execution_mode":"parallel"}],"declarations_frozen":{"late_results":true,"schema_pins":{"read":{"schema_digest":"634bdb1fe768fce0974f9c89e6f15f889470a8288277c6433e4711ab6a5b9335","semantics":7}}}}],"tools":[{"name":"read","description":"Read a file","input_schema":{"properties":{},"type":"object"},"execution_mode":"parallel"}],"system_text":"","system_digest":"0000000000000000000000000000000000000000000000000000000000000000","join_version":1,"role_versions":{}}}"#;

/// The routed `RunStarted` and the `RunAuthRefrozen` the account-chooser build writes with a
/// frozen auth method, byte for byte as that build's
/// `auth_selection_method_is_must_understand_on_both_selection_records` test (also in
/// `framing.rs` here) pins them.
const PINNED_AUTH_METHOD_RECORDS: &[&str] = &[
    r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"work-claude","account_ref":"acct-1","account_decision":{"path":"routed","admission_id":"r","decision_id":"acd-1"},"auth_method":"oauth"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
    r#"{"type":"run_auth_refrozen","ts_ms":5,"auth_selection":{"credential_id":"work-claude","account_ref":"acct-1","auth_method":"oauth"}}"#,
];

/// A prior reader's feature table, modelled as this build's table without the two names.
fn table_without_either_name() -> Vec<&'static str> {
    KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != AUTH_SELECTION_METHOD_FEATURE && *name != TOOL_SCHEMA_PINS_FEATURE)
        .collect()
}

#[test]
fn a_frozen_schema_pin_keeps_its_bytes_and_gates_its_frame() {
    let record: WalRecord = serde_json::from_str(PINNED_SCHEMA_PIN_RUN_STARTED).unwrap();
    assert_eq!(
        serde_json::to_string(&record).unwrap(),
        PINNED_SCHEMA_PIN_RUN_STARTED
    );
    let WalRecord::RunStarted {
        manifest: Some(manifest),
        ..
    } = &record
    else {
        panic!("the pinned record carries a manifest")
    };
    let crate::FetchOutcome::Answered {
        declarations_frozen: Some(declarations),
        ..
    } = &manifest.fetches[0].outcome
    else {
        panic!("the pinned fetch froze its declarations")
    };
    let pin = &declarations.schema_pins.as_ref().expect("the pin decodes")["read"];
    assert_eq!(
        pin.schema_digest,
        "634bdb1fe768fce0974f9c89e6f15f889470a8288277c6433e4711ab6a5b9335"
    );
    assert_eq!(pin.semantics, 7);
    assert!(record
        .required_features()
        .contains(&TOOL_SCHEMA_PINS_FEATURE));

    let bytes = encode(1, 1, &record).unwrap();
    assert_eq!(bytes[4], GATED_FRAMING_VERSION);
    match decode_next(&bytes) {
        DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
        _ => panic!("this build reads a manifest with frozen schema pins"),
    }
    // Every other feature the record needs is one an older build already knows, so it names
    // exactly the pin feature when it refuses the log.
    assert!(
        matches!(decode_next_with(&bytes, &table_without_either_name()), DecodeStep::NewerWriter { missing, .. } if missing == [TOOL_SCHEMA_PINS_FEATURE])
    );
}

#[test]
fn a_build_without_either_name_refuses_every_record_that_carries_one() {
    let older = table_without_either_name();
    for (pinned, feature) in [
        (PINNED_AUTH_METHOD_RECORDS[0], AUTH_SELECTION_METHOD_FEATURE),
        (PINNED_AUTH_METHOD_RECORDS[1], AUTH_SELECTION_METHOD_FEATURE),
        (PINNED_SCHEMA_PIN_RUN_STARTED, TOOL_SCHEMA_PINS_FEATURE),
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        let bytes = encode(1, 1, &record).unwrap();
        assert!(
            matches!(decode_next(&bytes), DecodeStep::Record { .. }),
            "this build reads {feature}"
        );
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [feature]),
            "an older build refuses {feature} by name"
        );
    }
}
