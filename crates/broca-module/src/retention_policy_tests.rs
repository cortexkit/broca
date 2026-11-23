use super::*;

fn policy(keep: Keep, backup: bool) -> RetentionPolicy {
    RetentionPolicy::new(keep, 0, backup, false)
}

#[test]
fn a_new_session_is_stored_by_backup_then_by_keep() {
    assert_eq!(
        class_for(&policy(Keep::days(30), true)),
        StorageClass::Durable
    );
    assert_eq!(
        class_for(&policy(Keep::forever(), true)),
        StorageClass::Durable
    );
    assert_eq!(
        class_for(&policy(Keep::forever(), false)),
        StorageClass::Ordinary
    );
    assert_eq!(
        class_for(&policy(Keep::days(7), false)),
        StorageClass::Ephemeral
    );
    assert_eq!(
        class_for(&policy(Keep::none(), false)),
        StorageClass::Ephemeral
    );
}

#[test]
fn an_assigned_policy_says_what_the_sessions_storage_already_does() {
    assert_eq!(
        assigned(StorageClass::Durable, None),
        policy(Keep::forever(), true)
    );
    assert_eq!(
        assigned(StorageClass::Ordinary, None),
        policy(Keep::forever(), false)
    );
    // Whole days stay exact; a part day rounds up, so the policy never says less than broca
    // keeps.
    assert_eq!(
        assigned(StorageClass::Ephemeral, Some(604_800)),
        policy(Keep::days(7), false)
    );
    assert_eq!(
        assigned(StorageClass::Ephemeral, Some(3_600)),
        policy(Keep::days(1), false)
    );
    assert_eq!(
        assigned(StorageClass::Ephemeral, Some(90_000)),
        policy(Keep::days(2), false)
    );
}

#[test]
fn a_frozen_policy_is_the_starters_and_a_damaged_one_is_refused() {
    let frozen = serde_json::json!({
        "keep": {"days": 30}, "archive_after_days": 0, "backup": false, "request_dumps": true,
    });
    let found = effective(Some(&frozen), StorageClass::Ordinary, None).unwrap();
    assert_eq!(found.source, RetentionSource::starter());
    assert_eq!(found.policy.keep, Keep::days(30));
    assert!(found.policy.request_dumps);
    let found = effective(None, StorageClass::Durable, None).unwrap();
    assert_eq!(found.source, RetentionSource::runner());
    assert_eq!(found.policy, policy(Keep::forever(), true));
    let damaged = serde_json::json!({"keep": "sometimes"});
    assert!(effective(Some(&damaged), StorageClass::Ordinary, None)
        .unwrap_err()
        .contains("retention_policy.keep"));
}

#[test]
fn nothing_is_due_while_busy_and_the_earliest_deadline_applies() {
    let days = policy(Keep::days(2), false);
    let none = policy(Keep::none(), false);
    let forever = policy(Keep::forever(), false);
    // A run active or paused, or a send queued, holds every deadline back.
    assert_eq!(
        deadline(Some(&none), Some(10), Some(5), true, Some(100)),
        None
    );
    assert_eq!(
        deadline(Some(&days), None, None, false, Some(100)),
        Some(100 + 2 * DAY_MS)
    );
    assert_eq!(
        deadline(Some(&none), None, None, false, Some(100)),
        Some(100 + MAX_UNRELEASED_HOLD_MS)
    );
    assert_eq!(deadline(Some(&forever), None, None, false, Some(100)), None);
    // A release is due at once when it came after the last activity, else at that activity.
    assert_eq!(
        deadline(Some(&forever), Some(500), None, false, Some(100)),
        Some(500)
    );
    assert_eq!(deadline(None, Some(50), None, false, Some(100)), Some(100));
    // An assigned policy keeps its older retention's deadline, and a release still ends it
    // sooner.
    assert_eq!(deadline(None, None, Some(900), false, Some(100)), Some(900));
    assert_eq!(
        deadline(None, Some(200), Some(900), false, Some(100)),
        Some(200)
    );
    assert_eq!(deadline(None, None, None, false, Some(100)), None);
}
