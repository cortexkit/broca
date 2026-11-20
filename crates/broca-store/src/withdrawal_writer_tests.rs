use super::*;
use broca_core::RunIndex;

#[test]
fn setting_withdrawal_debt_for_a_missing_run_fails_loudly() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert!(
        store.index_run_withdrawals_owed("missing", true).is_err(),
        "missing rows must not silently lose debt"
    );
}
