use std::path::PathBuf;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use super::*;

const ROLE: &str = "classifier";
const DAY_MS: i64 = 24 * 60 * 60 * 1000;

fn scratch(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = broca_test_support::scratch_root().join(format!(
        "system-one-store-{name}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("system-one.db")
}

fn bootstrap_path(name: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(name);
    broca_owner_only::create_dir_all(&root).unwrap();
    root.join("system-one.db")
}

#[test]
fn classifier_cold_bootstrap_matches_legacy_schema() {
    let path = bootstrap_path("classifier-bootstrap-schema");
    drop(SystemOneStore::open(&path).unwrap());
    let conn = Connection::open(path).unwrap();
    let actual: String = conn
        .query_row(
            "SELECT group_concat(type || '|' || name || '|' || tbl_name || '|' || coalesce(sql, ''), char(10))
             FROM (SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let legacy = include_str!("../tests/fixtures/system-one-bootstrap.sqlite-master");
    assert_eq!(
        actual.lines().collect::<Vec<_>>(),
        legacy.lines().collect::<Vec<_>>()
    );
}

#[test]
fn interrupted_classifier_cold_bootstrap_leaves_no_schema() {
    let path = bootstrap_path("classifier-bootstrap-interrupted");
    crate::create_private_sqlite(&path).unwrap();
    let mut conn = Connection::open(&path).unwrap();
    conn.authorizer(Some(|context: AuthContext<'_>| match context.action {
        AuthAction::AlterTable {
            table_name: "batch",
            ..
        } => Authorization::Deny,
        _ => Authorization::Allow,
    }));
    let error = SystemOneStore::initialize_schema(&mut conn).unwrap_err();
    assert!(error.0.contains("not authorized"), "{error}");
    drop(conn);
    let conn = Connection::open(&path).unwrap();
    let objects: i64 = conn
        .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        objects, 0,
        "an interrupted column migration must roll back earlier CREATEs"
    );
    drop(conn);
    SystemOneStore::open(&path).unwrap();
}

fn claim<'a>(
    batch_id: &'a str,
    digest: &'a str,
    ceiling: Option<u64>,
    owner: &'a str,
    now: i64,
) -> BatchClaim<'a> {
    BatchClaim {
        role: ROLE,
        batch_id,
        model: "typesafe/jev-1.13.0",
        identity: digest,
        body_digest: digest,
        frozen_row: "{}",
        scope: test_scope(),
        ceiling_nanos: ceiling,
        owner,
        now_ms: now,
    }
}

fn answered(index: u64, text: &str, tokens: Option<u64>) -> StoredOutcome {
    StoredOutcome {
        index,
        outcome: Outcome::Answered(text.into()),
        input_tokens: tokens,
        output_tokens: None,
    }
}

fn resumed(claimed: Claimed) -> BatchRecord {
    match claimed {
        Claimed::Resumed(record) => record,
        other => panic!("expected a resumed batch, got {other:?}"),
    }
}

#[test]
fn a_resend_serves_the_stored_outcomes_byte_for_byte() {
    let store = SystemOneStore::open(scratch("resend")).unwrap();
    assert_eq!(
        store.claim(&claim("b", "d1", None, "p1", 1)).unwrap(),
        Claimed::Fresh
    );
    let text = r#"{"q":{"type":"noul","noul":0.1234567890123456789}}"#;
    assert!(store
        .record(ROLE, "b", "p1", &answered(1, text, Some(7)), 5, 2)
        .unwrap());
    store.release(ROLE, "b", "p1").unwrap();
    let record = resumed(store.claim(&claim("b", "d1", None, "p2", 3)).unwrap());
    assert_eq!(record.outcomes, vec![answered(1, text, Some(7))]);
    assert_eq!(record.spent_nanos, 5);
    assert_eq!(record.admitted_at_ms, 1, "first admission time is kept");
}

#[test]
fn a_different_body_is_reuse_and_writes_nothing() {
    let store = SystemOneStore::open(scratch("reuse")).unwrap();
    store.claim(&claim("b", "d1", Some(100), "p1", 1)).unwrap();
    store.release(ROLE, "b", "p1").unwrap();
    assert_eq!(
        store.claim(&claim("b", "d2", Some(1), "p2", 2)).unwrap(),
        Claimed::Reuse {
            recorded_identity: "d1".into()
        }
    );
    assert_eq!(
        store.ledger(ROLE, "b").unwrap(),
        Some((Some(100), 0)),
        "a refused reuse does not tighten the ceiling"
    );
    // Not left running by the refused claim either.
    resumed(store.claim(&claim("b", "d1", None, "p3", 3)).unwrap());
}

#[test]
fn budget_debits_saturate_without_turning_the_ledger_into_a_real() {
    let store = SystemOneStore::open_in_memory().unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    let maximum = i64::MAX as u64;
    for (index, debit) in [(0, 1), (1, maximum), (2, maximum)] {
        let outcome = StoredOutcome {
            index,
            outcome: Outcome::Failed("{}".into()),
            input_tokens: None,
            output_tokens: None,
        };
        assert!(store.record(ROLE, "b", "p1", &outcome, debit, 2).unwrap());
    }
    assert_eq!(store.ledger(ROLE, "b").unwrap(), Some((None, maximum)));
}

#[test]
fn a_running_batch_is_in_progress_and_untouched() {
    let store = SystemOneStore::open(scratch("running")).unwrap();
    store.claim(&claim("b", "d1", Some(100), "p1", 1)).unwrap();
    assert_eq!(
        store.claim(&claim("b", "d1", Some(1), "p2", 2)).unwrap(),
        Claimed::InProgress
    );
    assert_eq!(store.ledger(ROLE, "b").unwrap(), Some((Some(100), 0)));
}

#[test]
fn the_ceiling_tightens_durably_and_never_loosens() {
    let path = scratch("ceiling");
    let store = SystemOneStore::open(&path).unwrap();
    store
        .claim(&claim("b", "d1", Some(1_000), "p1", 1))
        .unwrap();
    store.release(ROLE, "b", "p1").unwrap();
    let record = resumed(store.claim(&claim("b", "d1", Some(10), "p1", 2)).unwrap());
    assert_eq!(record.ceiling_nanos, Some(10));
    store.release(ROLE, "b", "p1").unwrap();
    drop(store);
    let store = SystemOneStore::open(&path).unwrap();
    let record = resumed(
        store
            .claim(&claim("b", "d1", Some(1_000), "p1", 3))
            .unwrap(),
    );
    assert_eq!(
        record.ceiling_nanos,
        Some(10),
        "a looser re-send cannot raise it"
    );
    store.release(ROLE, "b", "p1").unwrap();
    let record = resumed(store.claim(&claim("b", "d1", None, "p1", 4)).unwrap());
    assert_eq!(
        record.ceiling_nanos,
        Some(10),
        "no ceiling sent keeps the recorded one"
    );
}

#[test]
fn a_ceiling_first_sent_on_a_resend_is_recorded() {
    let store = SystemOneStore::open(scratch("late-ceiling")).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    store.release(ROLE, "b", "p1").unwrap();
    let record = resumed(store.claim(&claim("b", "d1", Some(7), "p1", 2)).unwrap());
    assert_eq!(record.ceiling_nanos, Some(7));
}

#[test]
fn only_the_holder_can_record() {
    let store = SystemOneStore::open(scratch("fence")).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    assert!(!store
        .record(ROLE, "b", "p2", &answered(0, "{}", None), 9, 2)
        .unwrap());
    store.release(ROLE, "b", "p1").unwrap();
    assert!(!store
        .record(ROLE, "b", "p1", &answered(0, "{}", None), 9, 3)
        .unwrap());
    assert!(store.outcomes(ROLE, "b").unwrap().is_empty());
    assert_eq!(store.ledger(ROLE, "b").unwrap(), Some((None, 0)));
}

#[test]
fn a_new_outcome_replaces_a_stored_one() {
    let store = SystemOneStore::open(scratch("replace")).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    let transient = StoredOutcome {
        index: 0,
        outcome: Outcome::Failed(r#"{"code":"provider_error"}"#.into()),
        input_tokens: None,
        output_tokens: None,
    };
    store.record(ROLE, "b", "p1", &transient, 0, 2).unwrap();
    store
        .record(ROLE, "b", "p1", &answered(0, "{}", Some(3)), 4, 3)
        .unwrap();
    assert_eq!(
        store.outcomes(ROLE, "b").unwrap(),
        vec![answered(0, "{}", Some(3))]
    );
    assert_eq!(store.ledger(ROLE, "b").unwrap(), Some((None, 4)));
}

#[test]
fn startup_releases_batches_a_crashed_process_left_running() {
    let path = scratch("abandoned");
    let store = SystemOneStore::open(&path).unwrap();
    store.claim(&claim("b", "d1", None, "dead", 1)).unwrap();
    store
        .record(ROLE, "b", "dead", &answered(0, "{}", None), 0, 2)
        .unwrap();
    drop(store);
    let store = SystemOneStore::open(&path).unwrap();
    assert_eq!(
        store.claim(&claim("b", "d1", None, "p2", 3)).unwrap(),
        Claimed::InProgress
    );
    assert_eq!(store.release_abandoned().unwrap(), 1);
    let record = resumed(store.claim(&claim("b", "d1", None, "p2", 4)).unwrap());
    assert_eq!(
        record.outcomes.len(),
        1,
        "what it stored survives the release"
    );
}

#[test]
fn discarding_a_fresh_batch_leaves_no_record() {
    let store = SystemOneStore::open(scratch("discard")).unwrap();
    store.claim(&claim("b", "d1", Some(5), "p1", 1)).unwrap();
    store.discard_fresh(ROLE, "b", "p1").unwrap();
    assert_eq!(store.ledger(ROLE, "b").unwrap(), None);
    assert_eq!(
        store.claim(&claim("b", "d2", None, "p1", 2)).unwrap(),
        Claimed::Fresh,
        "a later call may use the id for any body"
    );
}

#[test]
fn retention_deletes_a_batch_exactly_seven_days_after_first_admission() {
    let store = SystemOneStore::open(scratch("retention")).unwrap();
    let admitted = 1_000_000;
    store
        .claim(&claim("old", "d1", None, "p1", admitted))
        .unwrap();
    store
        .record(ROLE, "old", "p1", &answered(0, "{}", None), 0, admitted)
        .unwrap();
    store.release(ROLE, "old", "p1").unwrap();
    // A later re-send does not move the clock: retention counts from first admission.
    resumed(
        store
            .claim(&claim("old", "d1", None, "p1", admitted + 6 * DAY_MS))
            .unwrap(),
    );
    store.release(ROLE, "old", "p1").unwrap();
    store
        .claim(&claim("new", "d1", None, "p1", admitted + 1))
        .unwrap();
    store.release(ROLE, "new", "p1").unwrap();

    let kept = store.sweep(admitted + 7 * DAY_MS - 1).unwrap();
    assert_eq!(
        kept,
        SweepCounts::default(),
        "one millisecond early deletes nothing"
    );
    assert_eq!(store.outcomes(ROLE, "old").unwrap().len(), 1);

    let swept = store.sweep(admitted + 7 * DAY_MS).unwrap();
    assert_eq!(
        swept,
        SweepCounts {
            batches: 1,
            outcomes: 1
        }
    );
    assert_eq!(store.ledger(ROLE, "old").unwrap(), None);
    assert!(store.outcomes(ROLE, "old").unwrap().is_empty());
    assert!(
        store.ledger(ROLE, "new").unwrap().is_some(),
        "a younger batch stays"
    );
    assert_eq!(
        store
            .claim(&claim("old", "d2", None, "p1", admitted + 7 * DAY_MS))
            .unwrap(),
        Claimed::Fresh,
        "after retention the id names a new batch"
    );
}

#[test]
fn retention_leaves_a_running_batch_for_the_next_sweep() {
    let store = SystemOneStore::open(scratch("retention-running")).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 0)).unwrap();
    assert_eq!(store.sweep(8 * DAY_MS).unwrap(), SweepCounts::default());
    store.release(ROLE, "b", "p1").unwrap();
    assert_eq!(store.sweep(8 * DAY_MS).unwrap().batches, 1);
}

#[test]
fn roles_do_not_share_batch_ids() {
    let store = SystemOneStore::open(scratch("roles")).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    let mut other = claim("b", "d2", None, "p1", 1);
    other.role = "extractor";
    assert_eq!(store.claim(&other).unwrap(), Claimed::Fresh);
}

#[cfg(unix)]
#[test]
fn the_store_is_owner_only_and_keeps_no_wal_sidecar() {
    use std::os::unix::fs::PermissionsExt;
    let path = scratch("perms");
    let store = SystemOneStore::open(&path).unwrap();
    store.claim(&claim("b", "d1", None, "p1", 1)).unwrap();
    store
        .record(ROLE, "b", "p1", &answered(0, "{}", None), 0, 2)
        .unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let mut siblings: Vec<String> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    siblings.sort();
    let owner_file = store.owner_path("p1").unwrap();
    let mut expected = vec![
        "system-one.db".to_string(),
        owner_file
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    ];
    expected.sort();
    assert_eq!(siblings, expected, "{siblings:?}");
    assert!(std::fs::read(&owner_file).unwrap().is_empty());
    assert_eq!(
        std::fs::metadata(owner_file).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

fn test_scope() -> &'static BatchScope {
    static SCOPE: std::sync::OnceLock<BatchScope> = std::sync::OnceLock::new();
    SCOPE.get_or_init(|| BatchScope::Run {
        agent_id: "agent".into(),
        run_id: "run".into(),
    })
}

#[test]
fn recording_the_same_pending_usage_fact_does_not_charge_twice() {
    let store = SystemOneStore::open_in_memory().unwrap();
    let context = FactContext {
        source_instance_id: "source".into(),
        provider: "provider".into(),
        model: "model".into(),
        auth_selection: None,
        account_ref: None,
        charge_basis: broca_core::ChargeBasis::MeteredApi,
        charge_basis_mapping_version: 5,
        charge_basis_overrides: "none".into(),
        spend_nanos: 4200,
    };
    let paid = PaidRecord {
        spend_nanos: 4200,
        now_ms: 2,
        fact: Some(&context),
    };
    let outcome = answered(0, "{}", Some(100));
    assert_eq!(
        store
            .claim(&claim("replay", "digest", None, "owner", 1))
            .unwrap(),
        Claimed::Fresh
    );
    assert!(store
        .record_paid(ROLE, "replay", "owner", &outcome, &paid)
        .unwrap());
    assert!(store
        .record_paid(ROLE, "replay", "owner", &outcome, &paid)
        .unwrap());
    assert!(store
        .record_paid(
            ROLE,
            "replay",
            "owner",
            &answered(0, "changed", Some(200)),
            &paid
        )
        .is_err());
    // The fact's own spend is what the outbox keeps, so a replay naming another is a conflict.
    let other_spend = FactContext {
        source_instance_id: context.source_instance_id.clone(),
        provider: context.provider.clone(),
        model: context.model.clone(),
        auth_selection: None,
        account_ref: None,
        charge_basis: context.charge_basis,
        charge_basis_mapping_version: context.charge_basis_mapping_version,
        charge_basis_overrides: context.charge_basis_overrides.clone(),
        spend_nanos: 4201,
    };
    assert!(store
        .record_paid(
            ROLE,
            "replay",
            "owner",
            &outcome,
            &PaidRecord {
                spend_nanos: 4201,
                fact: Some(&other_spend),
                ..paid
            }
        )
        .is_err());
    let pending = store.pending_facts().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].fact.usage.input_tokens, Some(100));
    assert_eq!(pending[0].spend_nanos, 4200);
    store.release(ROLE, "replay", "owner").unwrap();
    let record = resumed(
        store
            .claim(&claim("replay", "digest", None, "next", 3))
            .unwrap(),
    );
    assert_eq!(record.spent_nanos, 4200);
    assert_eq!(record.outcomes, vec![outcome]);
}

#[test]
fn paid_outcome_and_outbox_survive_reopen_and_publish_only_once() {
    let path = scratch("paid-recovery");
    let export_path = path.with_file_name("run-index.db");
    let export = crate::SqliteStore::open(&export_path).unwrap();
    let context = FactContext {
        source_instance_id: export.source_instance_id().into(),
        provider: "typesafe".into(),
        model: "jev-1.13.0".into(),
        auth_selection: Some("apikey:typesafe:paid".into()),
        account_ref: Some("paid".into()),
        charge_basis: broca_core::ChargeBasis::MeteredApi,
        charge_basis_mapping_version: 5,
        charge_basis_overrides: "none".into(),
        spend_nanos: 4200,
    };
    {
        let store = SystemOneStore::open(&path).unwrap();
        assert_eq!(
            store
                .claim(&claim("paid", "digest", None, "dead", 1))
                .unwrap(),
            Claimed::Fresh
        );
        assert!(store
            .record_paid(
                ROLE,
                "paid",
                "dead",
                &answered(0, "{}", Some(100)),
                &PaidRecord {
                    spend_nanos: 4200,
                    now_ms: 2,
                    fact: Some(&context)
                }
            )
            .unwrap());
    }
    let store = SystemOneStore::open(&path).unwrap();
    assert_eq!(store.release_abandoned().unwrap(), 1);
    let pending = store.pending_facts().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].fact.run_id.as_deref(), Some("run"));
    assert_eq!(pending[0].fact.usage.input_tokens, Some(100));
    use crate::export_index::SystemOneFactEmission;
    assert_eq!(
        export.emit_system_one_fact(&pending[0].fact).unwrap(),
        SystemOneFactEmission::Inserted
    );
    drop(export);
    // A crash after export committed but before the outbox was acknowledged replays the same
    // bytes. The unique export identity, not an in-memory sent flag, makes that replay harmless.
    let export = crate::SqliteStore::open(&export_path).unwrap();
    assert_eq!(
        export.emit_system_one_fact(&pending[0].fact).unwrap(),
        SystemOneFactEmission::AlreadyPresent
    );
    let page = export.read_export_page(None, 100).unwrap();
    assert_eq!(page.facts.len(), 1);
    assert_eq!(page.high_water, 1);
    store.acknowledge_fact(&pending[0].fact.fact_id).unwrap();
    assert!(store.pending_facts().unwrap().is_empty());
    let record = resumed(
        store
            .claim(&claim("paid", "digest", None, "new", 3))
            .unwrap(),
    );
    assert_eq!(record.outcomes.len(), 1);
    assert_eq!(record.spent_nanos, 4200);
    store.release(ROLE, "paid", "new").unwrap();
    // After retention the same batch id is new paid work, so its fact must not collide with
    // the earlier admission's immutable fact.
    store.sweep(RETENTION_MS + 2).unwrap();
    assert_eq!(
        store
            .claim(&claim("paid", "digest", None, "new", RETENTION_MS + 3))
            .unwrap(),
        Claimed::Fresh
    );
    assert!(store
        .record_paid(
            ROLE,
            "paid",
            "new",
            &answered(0, "{}", None),
            &PaidRecord {
                spend_nanos: 9000,
                now_ms: RETENTION_MS + 4,
                fact: Some(&FactContext {
                    spend_nanos: 9000,
                    ..context
                })
            }
        )
        .unwrap());
    let next = store.pending_facts().unwrap();
    assert_eq!(next.len(), 1);
    assert_ne!(next[0].fact.fact_id, pending[0].fact.fact_id);
    assert_eq!(next[0].fact.usage.input_tokens, None);
    assert_eq!(
        export.emit_system_one_fact(&next[0].fact).unwrap(),
        SystemOneFactEmission::Inserted
    );
    assert_eq!(export.read_export_page(None, 100).unwrap().facts.len(), 2);
}
