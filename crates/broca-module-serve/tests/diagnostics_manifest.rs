use broca_module_serve::manifest::manifest;
use serde_json::json;

#[test]
fn diagnostics_manifest_declares_exact_flow_query() {
    let manifest = manifest("broca");
    manifest.validate_flow_operations().unwrap();
    let value = serde_json::to_value(manifest).unwrap();
    assert_eq!(value["flow_operations"], json!(["diagnostics.findings"]));
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
