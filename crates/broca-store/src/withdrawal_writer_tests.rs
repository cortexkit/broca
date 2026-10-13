use super::*;
use broca_core::{RunIndex, WalStore};

fn debt_fixture() -> (
    std::path::PathBuf,
    SqliteStore,
    broca_core::FileWalStore,
    BindIdentity,
    String,
) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "withdrawal-writers-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("store.db")).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let session = crate::conformance::session("terminal-debt");
    let mut file = wal.open(&session).unwrap();
    let mut records = crate::conformance::deferred::records(&session);
    let run_id = match &records[0].record {
        broca_wal::WalRecord::RunStarted { run_id, .. } => run_id.0.clone(),
        _ => unreachable!(),
    };
    file.append(1, &records[0].record, true).unwrap();
    let lineage = file.lineage_id().unwrap().to_string();
    for frame in records.iter_mut().skip(1) {
        match &mut frame.record {
            broca_wal::WalRecord::ApprovalRecorded { call_key, .. } => {
                *call_key = call_key.replacen("5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a", &lineage, 1)
            }
            broca_wal::WalRecord::ToolResult {
                withdraw_elicitation,
                ..
            } => *withdraw_elicitation = None,
            _ => {}
        }
        file.append(1, &frame.record, true).unwrap();
    }
    drop(file);
    store
        .index_run_active(&broca_core::RunIndexRow {
            run_id: run_id.clone(),
            session: session.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
            title: None,
            withdrawals_owed: false,
        })
        .unwrap();
    (root, store, wal, session, run_id)
}

#[test]
fn maintenance_terminal_reconciliation_preserves_unprojected_reader_debt() {
    let (root, store, wal, _, run_id) = debt_fixture();
    let counts = store.reconcile_active_rows(&wal).unwrap();
    assert_eq!(counts.reconciled, 1);
    let row = store.lookup_run(&run_id).unwrap().unwrap();
    assert!(row.state.is_terminal());
    assert!(
        row.withdrawals_owed,
        "maintenance made an owed run terminal without discovery"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn setting_withdrawal_debt_for_a_missing_run_fails_loudly() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert!(
        store.index_run_withdrawals_owed("missing", true).is_err(),
        "missing rows must not silently lose debt"
    );
}
