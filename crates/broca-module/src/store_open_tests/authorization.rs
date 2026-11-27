use super::*;
use crate::reader_admission::ReadAccess;
use crate::serve::{ServeError, ServeHandler};
use broca_protocol::{BindPrincipal, SendScope};
use broca_session_store::record::{Record, Retention, RetentionEvent};

fn accesses() -> [ReadAccess; 3] {
    [
        None,
        Some(BindPrincipal::Direct),
        Some(BindPrincipal::Reserved {
            module_id: "another".into(),
        }),
    ]
    .map(|principal| ReadAccess {
        principal,
        op: "session.read".into(),
        send: None,
    })
}

#[tokio::test]
async fn closed_authorization_read_without_a_display_head_loads_once() {
    let (root, runtime, registry, key) = paused_closed_read_fixture("closed-read-auth-only").await;
    let loads = || runtime.views().memory(|_| 0).loads.unwrap();
    let access = &accesses()[2];
    let first = registry.authorize_session_read(&key, access).await;
    assert_eq!(loads(), 1, "a cold closed session must be read once");
    let again = registry.authorize_session_read(&key, access).await;
    assert_eq!(format!("{again:?}"), format!("{first:?}"));
    assert_eq!(
        loads(),
        1,
        "the retained policy must answer a repeat without a display head"
    );
    assert!(registry.slot_state(&key).is_none());
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Evaluate the pre-cache authorization functions over the persisted session and compare both
/// cold and warm requests against them, including refused callers.
async fn assert_stored_answers(
    runtime: &StoreRuntime,
    registry: &LineageRegistry,
    key: &BindIdentity,
    class: StorageClass,
) {
    let stored = Session::view(
        runtime.backend(class),
        SessionAddr(broca_core::session_addr(key)),
        &triple(key),
        runtime.deployment(),
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(class, stored.head().cipher());
    let ownership =
        broca_session_loop::owner::ownership_with_plan(&stored.operations, parts).unwrap();
    assert!(ownership.episodes > 0);
    for _ in 0..2 {
        assert_eq!(
            registry.session_ownership_for_bind(key).await.as_ref(),
            Some(&ownership)
        );
        for access in accesses() {
            let expected = crate::reader_admission::admit(&stored, parts, &access).await;
            let served = registry.authorize_session_read(key, &access).await;
            assert_eq!(format!("{served:?}"), format!("{expected:?}"), "{access:?}");
            let scope = SendScope::default();
            let expected = broca_core::scope_admission::decide_owner_access(
                &ownership,
                access.principal.as_ref(),
                &scope,
            )
            .map_err(ServeError::ScopeRefused);
            let served = registry
                .authorize_session_owner(key, access.principal.as_ref(), &scope)
                .await;
            assert_eq!(format!("{served:?}"), format!("{expected:?}"), "{access:?}");
        }
    }
    assert!(registry.slot_state(key).is_none());
}

#[tokio::test]
async fn closed_authorization_matches_stored_expiring_released_and_paused_answers() {
    let class = StorageClass::Ephemeral;
    let (root, runtime, registry, key) = closed_read_fixture_in(
        "authorization-expiring",
        1,
        class,
        Some(TEN_YEARS),
        Vec::new(),
    )
    .await;
    assert_stored_answers(&runtime, &registry, &key, class).await;
    advance_clock(&root, &registry, FIXTURE_NOW_MS + 2 * TEN_YEARS * 1_000);
    assert!(matches!(
        registry.authorize_session_read(&key, &accesses()[0]).await,
        Err(ServeError::Expired { .. })
    ));
    assert!(matches!(
        registry
            .authorize_session_owner(&key, None, &SendScope::default())
            .await,
        Err(ServeError::Expired { .. })
    ));
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();

    let released_at = FIXTURE_NOW_MS + 10_000;
    let (root, runtime, registry, key) = closed_read_fixture_in(
        "authorization-released",
        1,
        class,
        Some(TEN_YEARS),
        vec![Record::Retention(Retention {
            event: RetentionEvent::Released,
            at_ms: released_at,
            policy: None,
        })],
    )
    .await;
    assert_stored_answers(&runtime, &registry, &key, class).await;
    let head = registry
        .head_for(&key, None, &SendScope::default())
        .await
        .unwrap();
    assert!(head.released.is_some());
    advance_clock(&root, &registry, released_at);
    assert!(matches!(
        registry.authorize_session_read(&key, &accesses()[0]).await,
        Err(ServeError::UnknownSession)
    ));
    assert!(matches!(
        registry
            .authorize_session_owner(&key, None, &SendScope::default())
            .await,
        Err(ServeError::UnknownSession)
    ));
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();

    let (root, runtime, registry, key) = paused_closed_read_fixture("authorization-paused").await;
    assert_stored_answers(&runtime, &registry, &key, StorageClass::Ordinary).await;
    let head = registry
        .head_for(&key, None, &SendScope::default())
        .await
        .unwrap();
    assert_eq!(head.last_run_state.unwrap().state, "paused");
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn closed_authorization_never_uses_a_stale_position() {
    let (root, runtime, registry, key) =
        closed_read_fixture("authorization-moved-position", 1).await;
    let initial = registry.session_ownership_for_bind(&key).await.unwrap();
    assert!(initial.scope.is_none());
    assert_eq!(
        registry.session_ownership_for_bind(&key).await,
        Some(initial.clone())
    );

    let class = StorageClass::Ordinary;
    let addr = SessionAddr(broca_core::session_addr(&key));
    let mut session = Session::load(
        runtime.backend(class),
        addr,
        &triple(&key),
        runtime.deployment(),
    )
    .await
    .unwrap();
    let records = session.open_log_records().await.unwrap();
    let mut start = records
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) => Some(start.clone()),
            _ => None,
        })
        .unwrap();
    let mut end = records
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end.clone()),
            _ => None,
        })
        .unwrap();
    let principal = BindPrincipal::Reserved {
        module_id: "later-owner".into(),
    };
    let scope = broca_protocol::SessionScope {
        owner: principal.clone(),
        scope_ref: "later".into(),
        scope_epoch: 2,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
        agent_id_frozen: false,
    };
    start.run_id = "scope-change".into();
    end.run_id.clone_from(&start.run_id);
    start.principal = Some(serde_json::to_value(principal).unwrap());
    start.scope = Some(serde_json::to_value(&scope).unwrap());
    session
        .append(&[Record::RunStart(start), Record::RunEnd(end)], true)
        .await
        .unwrap();
    session.close().await.unwrap();

    let loads = || runtime.views().memory(|_| 0).loads.unwrap();
    let before = loads();
    let updated = registry.session_ownership_for_bind(&key).await.unwrap();
    assert_eq!(updated.episodes, initial.episodes + 1);
    assert_eq!(updated.scope.as_ref(), Some(&scope));
    assert_eq!(loads(), before + 1, "the new position is read once");
    assert_eq!(
        registry.session_ownership_for_bind(&key).await,
        Some(updated)
    );
    assert_eq!(loads(), before + 1, "the new position is now retained");
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}
