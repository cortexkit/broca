//! A module that changed its id is reached under either id: an open refused because the old id
//! is unknown, or because a scope registered by the old owner is no longer live, is sent once
//! more under the other id. Every other refusal is returned as it came.

use super::*;

fn scope(owner: &str) -> subc_protocol::scope::ScopeSelector {
    subc_protocol::scope::ScopeSelector::new(
        subc_protocol::Principal::Reserved {
            module_id: owner.into(),
        },
        "head-1",
        Some(3),
    )
}

async fn open(
    conn: &SubcConnection,
    module_id: &str,
    scope: Option<&subc_protocol::scope::ScopeSelector>,
) -> ConnResult<RouteHandle> {
    conn.route_open_dispatch_single_attempt(
        module_id,
        &BindIdentity::new("/project", "broca", "worker"),
        scope,
        RouteShape {
            tool_provider_v1: true,
            ..Default::default()
        },
        false,
        None,
    )
    .await
}

/// The module and scope owner a `route.open` request names.
async fn next_open(daemon: &mut InProcessModuleEnd) -> (Frame, String, Option<String>) {
    let request = daemon.next_request().await;
    let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    let module = value["target"]["module_id"].as_str().unwrap().to_owned();
    let owner = value["scope"]["owner"]["module_id"]
        .as_str()
        .map(str::to_owned);
    (request, module, owner)
}

fn accept(daemon: &InProcessModuleEnd, request: &Frame) {
    assert!(daemon.reply(
        request,
        serde_json::to_vec(&ClientControlResponse::RouteOpen {
            route_channel: 7,
            route_epoch: 1,
        })
        .unwrap()
    ));
}

#[tokio::test]
async fn an_open_to_a_renamed_modules_old_id_reopens_under_its_new_id() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let (result, ()) = tokio::join!(open(&conn, "prefrontal-core", None), async {
        let (request, module, _) = next_open(&mut daemon).await;
        assert_eq!(module, "prefrontal-core");
        assert!(daemon.refuse(&request, "unknown_module"));
        let (request, module, _) = next_open(&mut daemon).await;
        assert_eq!(module, "prefrontal");
        accept(&daemon, &request);
    });
    result.unwrap();
}

#[tokio::test]
async fn a_scope_registered_by_the_old_owner_reopens_under_its_resynced_twin() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let frozen = scope("prefrontal-core");
    let (result, ()) = tokio::join!(open(&conn, "aft", Some(&frozen)), async {
        let (request, module, owner) = next_open(&mut daemon).await;
        assert_eq!(
            (module.as_str(), owner.as_deref()),
            ("aft", Some("prefrontal-core"))
        );
        assert!(daemon.refuse(&request, "scope_not_live"));
        let (request, module, owner) = next_open(&mut daemon).await;
        // Same module, same ref and epoch; only the owner moves to the module's new id.
        assert_eq!(
            (module.as_str(), owner.as_deref()),
            ("aft", Some("prefrontal"))
        );
        let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(value["scope"]["ref"], "head-1");
        assert_eq!(value["scope"]["scope_epoch"], 3);
        accept(&daemon, &request);
    });
    result.unwrap();
}

#[tokio::test]
async fn other_refusals_and_other_modules_are_not_retried() {
    for (module, owner, code) in [
        // A module that was never renamed has no other id.
        ("aft", None, "unknown_module"),
        // A renamed module's other refusals are its own answer.
        ("prefrontal-core", None, "permission_denied"),
        // A scope whose owner was never renamed has no twin.
        ("aft", Some("cingulate"), "scope_not_live"),
    ] {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let scope = owner.map(scope);
        let (result, ()) = tokio::join!(open(&conn, module, scope.as_ref()), async {
            let (request, _, _) = next_open(&mut daemon).await;
            assert!(daemon.refuse(&request, code));
        });
        let Err(ConnError::Rejected(refusal)) = result else {
            panic!("{module} {code}: expected the refusal, got {result:?}");
        };
        assert_eq!(refusal.code(), code);
        assert!(
            daemon.take_written_for_test().is_empty(),
            "{module} {code}: no second open"
        );
    }
}

#[tokio::test]
async fn a_refused_retry_returns_the_first_refusal() {
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
    let (result, ()) = tokio::join!(open(&conn, "prefrontal-core", None), async {
        let (request, _, _) = next_open(&mut daemon).await;
        assert!(daemon.refuse(&request, "unknown_module"));
        let (request, module, _) = next_open(&mut daemon).await;
        assert_eq!(module, "prefrontal");
        assert!(daemon.refuse(&request, "permission_denied"));
    });
    let Err(ConnError::Rejected(refusal)) = result else {
        panic!("expected the first refusal, got {result:?}");
    };
    assert_eq!(refusal.code(), "unknown_module");
}
