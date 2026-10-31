fn direct_selector() -> ScopeSelector {
    ScopeSelector::new(Principal::Reserved { module_id: "rig".into() }, "run", Some(1))
        .with_bind_proof(Some("EREREREREREREREREREREREREREREREREREREREREREQ".into()))
}

fn direct_plane(capabilities: Vec<String>) -> (Arc<ScriptedToolPlaneConnection>, SubcToolPlane) {
    let catalog = vec![catalog_entry("aft", &["echo"]), catalog_entry("other", &["echo"] )];
    let conn = ScriptedToolPlaneConnection::new(vec![catalog.clone(), catalog.clone(), catalog], vec![]);
    *conn.capabilities.lock().unwrap() = capabilities;
    let mut plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    plane.direct_carrier_scope = Some(direct_selector());
    (conn, plane)
}

fn proof_capabilities() -> Vec<String> {
    vec![CAP_SCOPES_V1.into(), subc_protocol::manifest::CAP_SCOPE_BIND_PROOF_V1.into()]
}

#[tokio::test]
async fn direct_carrier_routes_apply_proof_to_warm_dispatch_and_reopen() {
    let (conn, plane) = direct_plane(proof_capabilities());
    let bind = BindIdentity::new("/local/wal-root", "broca", "carrier");
    plane.warm_bind(&bind, Duration::from_secs(1)).await.unwrap();
    plane.ensure_route(&bind, None, "aft", &ToolOpenGuard::default()).await.unwrap();
    plane.ensure_route(&bind, None, "other", &ToolOpenGuard::default()).await.unwrap();
    plane.invalidate_route(&bind);
    plane.ensure_route(&bind, None, "aft", &ToolOpenGuard::default()).await.unwrap();
    let scopes = conn.opened_scopes();
    assert_eq!(scopes.len(), 3);
    assert!(scopes.iter().all(|(_, scope)| scope.as_ref() == Some(&direct_selector())));
}

#[tokio::test]
async fn direct_carrier_never_falls_back_without_scope_and_proof_capabilities() {
    for capabilities in [vec![], vec![CAP_SCOPES_V1.into()]] {
        let (conn, plane) = direct_plane(capabilities);
        let bind = BindIdentity::new("/local/wal-root", "broca", "carrier");
        let result = plane.warm_bind(&bind, Duration::from_secs(1)).await;
        assert!(matches!(result, Err(ToolPlaneError::ScopeRefused { code, .. }) if code == SCOPE_UNSUPPORTED));
        assert!(conn.opened_scopes().is_empty());
    }
}

#[tokio::test]
async fn direct_carrier_refusal_keeps_the_code_without_echoing_private_proof() {
    let (conn, plane) = direct_plane(proof_capabilities());
    let proof = direct_selector().bind_proof.unwrap();
    conn.script_route_open_sequence("aft", vec![RouteOpenScript::rejected_body(
        &subc_protocol::ErrorBody::new(subc_protocol::error_codes::SCOPE_BIND_PROOF_MISMATCH, &proof),
    )]);
    let bind = BindIdentity::new("/local/wal-root", "broca", "carrier");
    let error = plane.warm_bind(&bind, Duration::from_secs(1)).await.unwrap_err();
    assert!(matches!(&error, ToolPlaneError::ScopeRefused { code, .. } if code == subc_protocol::error_codes::SCOPE_BIND_PROOF_MISMATCH));
    assert!(!error.to_string().contains(&proof));
}

#[tokio::test]
async fn direct_carrier_mode_leaves_ordinary_unscoped_opens_unchanged() {
    let (conn, mut plane) = direct_plane(vec![]);
    plane.direct_carrier_scope = None;
    let bind = BindIdentity::new("/local/wal-root", "broca", "ordinary");
    plane.warm_bind(&bind, Duration::from_secs(1)).await.unwrap();
    assert_eq!(conn.opened_scopes(), vec![("ordinary".into(), None)]);
}
