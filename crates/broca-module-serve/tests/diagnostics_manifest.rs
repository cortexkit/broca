use broca_module_serve::manifest::manifest;

#[test]
fn diagnostics_manifest_declares_complete_flow_operations() {
    let manifest = manifest("broca");
    manifest.validate_flow_operations().unwrap();
    let value = serde_json::to_value(manifest).unwrap();
    let principal = broca_protocol::BindPrincipal::Reserved {
        module_id: "cingulate".into(),
    };
    let scope = broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "prefrontal-core".into(),
            },
            scope_ref: "flow".into(),
            scope_epoch: 1,
            flow_id: Some("flow".into()),
            sender: Some(principal.clone()),
            agent_id: Some("owners-agent".into()),
            run_id: Some("owners-run".into()),
        },
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
        owner_authorized: true,
    });
    let declared_operations: std::collections::BTreeSet<_> = value["provides"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|role| role["operations"].as_array())
        .flatten()
        .filter_map(|op| op["name"].as_str())
        .map(str::to_owned)
        .collect();
    let expected: std::collections::BTreeSet<_> = declared_operations
        .iter()
        .filter(|op| {
            matches!(
                broca_module_serve::management_admission::admit(op, Some(&principal), &scope),
                Ok(broca_module_serve::management_admission::Admission::Legacy
                    | broca_module_serve::management_admission::Admission::Diagnostics)
            )
        })
        .cloned()
        .collect();
    let advertised: std::collections::BTreeSet<String> =
        serde_json::from_value(value["flow_operations"].clone()).unwrap();
    assert_eq!(
        advertised, expected,
        "discovery must equal the real scope gate for every catalog operation"
    );
    assert!(advertised.contains("diagnostics.findings"));
    assert!(advertised.contains("session.send"));
    assert!(advertised.contains("classify.run"));
    assert!(!advertised.contains("health.check"));
    let basal = broca_protocol::BindPrincipal::Reserved {
        module_id: "basal".into(),
    };
    for method in ["permissions.list", "permissions.revoke"] {
        assert!(matches!(
            broca_module_serve::management_admission::admit(method, Some(&basal), &scope),
            Err(broca_module_serve::management_admission::Refusal::Operator)
        ));
    }
    assert_eq!(
        value["capabilities"]["provides"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|cap| cap.as_str() == Some("flow-scopes/v1"))
            .count(),
        1
    );
    let operations = value["provides"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|role| role["operations"].as_array())
        .flatten()
        .filter(|op| op["name"] == "diagnostics.findings")
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0]["kind"], "query");
}

#[test]
fn diagnostics_manifest_round_trip() {
    let manifest = manifest("broca");
    let value = serde_json::to_value(&manifest).unwrap();
    let decoded: subc_protocol::manifest::ModuleManifest =
        serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    if let Some(path) = std::env::var_os("CAPTURE_BROCA_DIAGNOSTICS_MANIFEST") {
        std::fs::write(
            path,
            format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
        )
        .unwrap();
        println!("captured one complete HELLO manifest");
    }
}
