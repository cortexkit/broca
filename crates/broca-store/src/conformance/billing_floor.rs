//! Billing floors remain machine-local when a WAL replaces its projection.

use super::*;
use broca_core::{emit_lineage_usage_exports, imported_run_ids};

fn finish(seq: u64) -> FramedRecord {
    framed(
        seq,
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: Some(seq),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    )
}

fn episode(seq: u64, run_id: &str, session: &BindIdentity) -> Vec<FramedRecord> {
    vec![
        run_started(seq, run_id, session, "hello", ModelHandle::new("stub", "m")),
        finish(seq + 1),
    ]
}

/// Recording the imported-run suppression set before a failed WAL installation
/// leaves the old local episode billable. Fresh ids started after installation
/// must also stay billable because neither kind belongs to the imported set.
pub fn billing_floor_preserves_local_and_new_runs<S, F>(make_store: F)
where
    S: ExportIndex,
    F: Fn() -> S,
{
    let session = session("billing-floor-local");
    let local = vec![run_started(
        0,
        "z-local-resumed",
        &session,
        "hello",
        ModelHandle::new("stub", "m"),
    )];
    let mut incoming = local.clone();
    incoming.push(finish(1));
    incoming.extend(episode(2, "a-imported", &session));
    let store = make_store();
    store
        .record_imported_runs(&session, "cut-crash", &imported_run_ids(&incoming, &local))
        .unwrap();
    drop(store);

    // Restart after committing imported ids, while the old local WAL is still in
    // place because installation never reached the rename. Its open run resumes.
    let store = make_store();
    let mut resumed = local;
    resumed.push(finish(1));
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    store
        .emit_run(&resumed, &RunId("z-local-resumed".into()), &resolver)
        .unwrap();
    incoming.extend(episode(4, "fresh-after-activation", &session));
    emit_lineage_usage_exports(&store, &incoming, &resolver).unwrap();
    let page = store.read_page(None, u64::MAX).unwrap();
    let mut ids: Vec<_> = page
        .facts
        .iter()
        .map(|fact| fact.segment.as_session().unwrap().run_id.0.as_str())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["fresh-after-activation", "z-local-resumed"]);

    // The other session differs in its session name; its imported-id set must not
    // hide work belonging to this session even when the run id matches.
    let other = super::session("billing-floor-other-session");
    store
        .record_imported_runs(&other, "cut-other", &[RunId("unrelated-local".into())])
        .unwrap();
    store
        .emit_run(
            &episode(0, "unrelated-local", &session),
            &RunId("unrelated-local".into()),
            &resolver,
        )
        .unwrap();
    assert_eq!(store.read_page(None, u64::MAX).unwrap().facts.len(), 3);
}
