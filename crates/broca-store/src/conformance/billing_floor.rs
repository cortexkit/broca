//! Billing floors remain machine-local when a WAL replaces its projection.

use super::*;
use broca_core::{catch_up_usage_exports, emit_lineage_usage_exports, imported_run_ids};

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

fn index_terminal<S: RunIndex>(store: &S, session: &BindIdentity, run_id: &str) {
    store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: run_id.into(),
            session: session.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index run");
    store
        .index_run_terminal(
            run_id,
            RunIndexState::Completed,
            Some(Usage::default()),
            None,
            0,
        )
        .expect("index terminal run");
}

/// Reopening and dropping/rebuilding a projection cannot make imported episodes billable.
/// `make_store` must reopen the same database on each call.
pub fn billing_floor_blocks_emission<S, F>(make_store: F)
where
    S: ExportIndex + SessionStore,
    F: Fn() -> S,
{
    let session = session("billing-floor-emission");
    let mut records = episode(0, "source-one", &session);
    records.extend(episode(2, "source-two", &session));
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    let store = make_store();
    let high_water = ExportIndex::high_water(&store);
    let ids = imported_run_ids(&records, &[]);
    store
        .record_imported_runs(&session, "cut-one", &ids)
        .unwrap();
    store
        .record_imported_runs(&session, "cut-one", &ids)
        .unwrap();
    store
        .record_imported_runs(&session, "cut-two", &ids)
        .unwrap();
    store
        .record_imported_runs(&session, "cut-one", &[])
        .unwrap();
    Persister::project(&store, &session, &records).unwrap();
    Persister::project(&store, &session, &[]).unwrap();
    assert!(MaterializedStore::read_admission_snapshot(&store, &session)
        .unwrap()
        .is_none());
    drop(store);

    let store = make_store();
    Persister::project(&store, &session, &records).unwrap();
    for id in &ids {
        store.emit_run(&records, id, &resolver).unwrap();
    }
    // Before deleting a session, retention derives usage facts directly from its
    // WAL; it does not use the missing-terminal-run list to skip imported episodes.
    assert_eq!(
        emit_lineage_usage_exports(&store, &records, &resolver).unwrap(),
        2
    );
    assert_eq!(
        ExportIndex::high_water(&store),
        high_water,
        "imported runs minted facts"
    );
    assert!(store
        .read_page(None, u64::MAX)
        .unwrap()
        .facts
        .iter()
        .all(|fact| fact.segment.as_session().unwrap().session != session));
}

/// Catch-up enumerates terminal runs with no local facts and exports their WAL
/// usage. Imported episodes must be absent from that list because they were billed
/// on the source machine, not missed by this machine's exporter.
pub fn billing_floor_excludes_backfill<S, F>(make_store: F)
where
    S: ExportIndex + RunIndex,
    F: Fn() -> S,
{
    let session = session("billing-floor-backfill");
    let mut records = episode(0, "backfill-imported", &session);
    records.extend(episode(2, "backfill-local", &session));
    let store = make_store();
    store
        .record_imported_runs(
            &session,
            "cut-backfill",
            &[RunId("backfill-imported".into())],
        )
        .unwrap();
    index_terminal(&store, &session, "backfill-imported");
    index_terminal(&store, &session, "backfill-local");
    let missing = store.missing_terminal_runs().unwrap();
    assert_eq!(missing.len(), 1, "imported run remained in backfill outbox");
    assert_eq!(missing[0].run_id.0, "backfill-local");
    let wal = ConformanceWalStore::with_records(session.clone(), records.clone());
    let resolver = |_: &str, _: &str| (ChargeBasis::Unknown, 1);
    assert_eq!(catch_up_usage_exports(&store, &wal, &resolver).unwrap(), 1);
    assert_eq!(catch_up_usage_exports(&store, &wal, &resolver).unwrap(), 0);
    assert_eq!(
        emit_lineage_usage_exports(&store, &records, &resolver).unwrap(),
        2
    );
    let page = store.read_page(None, u64::MAX).unwrap();
    assert_eq!(page.facts.len(), 1);
    assert_eq!(
        page.facts[0].segment.as_session().unwrap().run_id.0,
        "backfill-local"
    );
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
