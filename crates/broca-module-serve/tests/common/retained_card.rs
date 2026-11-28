use broca_store::grants::{GrantsStore, HoldAdmission, HoldRequest, ScopeIdentity};
use broca_store::system_one::BatchScope;

/// Keep the answered card after its grant is revoked, even when the deadline timer sweeps.
/// `finish_unstarted` stops the seeded call from dispatching while leaving its card reference.
pub(super) fn retain_card(store: &GrantsStore, key: &str, scope: ScopeIdentity, now: i64) {
    let batch_id = format!("retained-card-{key}");
    let admitted = store
        .hold(HoldRequest {
            batch_id: batch_id.clone(),
            project_id: None,
            batch_scope: BatchScope::Run {
                agent_id: "agent".into(),
                run_id: scope.scope_ref.clone(),
            },
            scope,
            principal: "reserved:owner".into(),
            call_key: batch_id.clone(),
            agent_id: "agent".into(),
            provider: "typesafe".into(),
            model: "jev-1.13.0".into(),
            request: "{}".into(),
            args_digest: "retained-card".into(),
            frozen_row: "{}".into(),
            dedup_key: key.into(),
            admitted_at_ms: now,
        })
        .unwrap();
    assert!(matches!(admitted, HoldAdmission::Inserted(_)));
    assert!(store.finish_unstarted(&batch_id, "{}", now).unwrap());
}
