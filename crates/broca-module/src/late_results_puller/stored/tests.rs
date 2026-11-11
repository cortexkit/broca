use super::*;
use broca_session_store::record::{CancelPoint, CancelRequest};

fn fixture(held: bool) -> (StoredLineage, RegistrationRow, LateEntry) {
    let intent = Record::ToolIntent(ToolIntent {
        run_id: "run".into(),
        batch_id: "run-b1".into(),
        tool_call_id: ToolCallId("call".into()),
        tool_name: Some("test".into()),
        module_id: Some("tools".into()),
        args_digest: broca_session_store::codec::Digest([0; 32]),
        args: None,
        call_key: Some("lineage:1".into()),
        approval_digest: None,
        dispatched_at_ms: Some(1),
        schema_pin: None,
        hooks: None,
        key_derived: None,
    });
    let mut records = vec![(1, intent)];
    if held {
        records.push((
            2,
            Record::ToolHeld(ToolHeld {
                batch_id: "run-b1".into(),
                tool_call_id: ToolCallId("call".into()),
                call_key: "lineage:1".into(),
                module: "tools".into(),
                reason: "running".into(),
                detail_digest: None,
                held_at_ms: 2,
                sequential: false,
            }),
        ));
    }
    (
        StoredLineage::Readable { records, run: None },
        RegistrationRow {
            call: LateResultRegistration {
                lineage: "lineage".into(),
                call_key: "lineage:1".into(),
                module: "tools".into(),
                bind: BindIdentity::new("/project", "h", "s"),
                scope: None,
            },
            state: RegistrationState::Registered,
        },
        LateEntry::new(
            "result",
            "reserved:broca",
            "ref",
            1,
            "reserved:broca",
            "lineage:1",
            "event",
            1,
        ),
    )
}

#[test]
fn an_intent_waits_until_its_store_held_record_is_durable() {
    let (view, row, entry) = fixture(false);
    assert_eq!(view.classify("tools", &entry, &row), Classified::Pending);
    assert_eq!(view.retirement_reason(&row.call.call_key), None);
    assert!(
        !view.needs_intake(&row.call.call_key),
        "an ordinary dispatch starts no puller"
    );
}

#[test]
fn a_store_cancel_settles_without_delivery_before_the_run_ends() {
    let (mut view, row, entry) = fixture(true);
    let StoredLineage::Readable { records, .. } = &mut view else {
        unreachable!()
    };
    records.push((
        3,
        Record::CancelRequest(CancelRequest {
            run_id: "run".into(),
            at: CancelPoint::Now,
        }),
    ));
    assert_eq!(view.classify("tools", &entry, &row), Classified::NoOp);
    assert_eq!(
        view.retirement_reason(&row.call.call_key),
        None,
        "cancelling is not a committed terminal"
    );
}
