use broca_core::compaction_v2::received::ReceivedView;
use broca_protocol::BindIdentity;
use broca_session_store::fold::operations::{AppliedView, Operations, ViewSlot};
use broca_session_store::record::{Record, ViewApplied};
use cortexkit_role_compaction::view::View;

/// A view sealed with build metadata, which its checksum covers.
fn built(version: u64, through: &str, new_signed: Option<&str>) -> ReceivedView {
    View::seal_with_policy(
        version,
        vec![],
        Some(through.into()),
        new_signed.map(str::to_owned),
    )
    .into()
}

/// A session whose step applied a view one provider handed over, and which then accepted a
/// newer one from another: `restore` rebuilds both views with their build metadata (a view
/// rebuilt without it would fail its checksum) and names who handed each over, and the copies a
/// seal carries restore to the same state.
#[test]
fn views_restore_with_their_build_metadata_and_applier_across_a_seal() {
    let bind = BindIdentity::new("/project", "test", "s");
    let applied = built(1, "m1", Some("strip"));
    let latest = built(2, "m2", None);
    let records = vec![
        (
            1,
            super::view_record(None, &applied, Some("ck-mc")).unwrap(),
        ),
        (
            2,
            Record::ViewApplied(ViewApplied {
                step_id: 1,
                version: 1,
                checksum: applied.view().checksum.to_string(),
            }),
        ),
        (
            3,
            super::view_record(Some(&applied), &latest, Some("another")).unwrap(),
        ),
    ];
    let operations = Operations {
        view_accepted: Some(ViewSlot {
            version: 2,
            checksum: latest.view().checksum.to_string(),
        }),
        view_applied: Some(AppliedView {
            step_id: 1,
            view: ViewSlot {
                version: 1,
                checksum: applied.view().checksum.to_string(),
            },
        }),
        ..Default::default()
    };
    let state = super::restore(&records, &bind, &operations).unwrap();
    assert_eq!(state.applied(), Some(applied.view()));
    assert_eq!(state.latest(), Some(latest.view()));
    assert_eq!(state.applied_by(), Some("ck-mc"));
    assert_eq!(state.accepted_by(), Some("another"));
    let carried: Vec<(u64, Record)> = super::carried_views(&state, &operations)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(seq, record)| (seq as u64 + 1, record))
        .collect();
    let stored: Vec<_> = carried
        .iter()
        .filter_map(|(_, record)| match record {
            Record::View(view) => Some((
                view.version,
                view.built_through.clone(),
                view.new_signed.clone(),
                view.applied_by.clone(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        stored,
        [
            (
                1,
                Some("m1".into()),
                Some("strip".into()),
                Some("ck-mc".into())
            ),
            (2, Some("m2".into()), None, Some("another".into())),
        ]
    );
    assert_eq!(
        super::restore(&carried, &bind, &operations).unwrap(),
        state,
        "the copies a seal carries restore the same state"
    );
}
