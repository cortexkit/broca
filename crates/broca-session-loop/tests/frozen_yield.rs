use broca_session_loop::{MemoryParts, Parts};
use broca_session_store::record::PartKind;

const MANIFEST: &str = r#"{"plan":"{}","plan_identity":"plan","composition_digest":"composition","fetches":[{"provider":"tools","catalog_digest":"catalog","tools":[],"declarations_frozen":{"call_keepalive":true,"result_formats":{"work":"tool-result/v1"},"late_results":true}}],"tools":[],"system_text":"","system_digest":"system","join_version":1,"role_versions":{}}"#;

#[test]
fn stored_manifest_before_yield_tools_keeps_provider_answer_bytes() {
    let manifest: broca_wal::PlanManifest = serde_json::from_str(MANIFEST).unwrap();
    let mut parts = MemoryParts::default();
    let mut setup = Vec::new();
    let rest = broca_session_loop::run::manifest_parts(&manifest, &mut parts, &mut setup).unwrap();
    let answer = setup
        .iter()
        .find(|part| part.kind == PartKind::ProviderAnswer)
        .unwrap()
        .clone();
    let pinned = br#"{"catalog_digest":"catalog","declarations_frozen":{"call_keepalive":true,"late_results":true,"result_formats":{"work":"tool-result/v1"}},"provider":"tools","tools":[]}"#;
    assert_eq!(parts.parts[&answer.digest], pinned);
    let reference = broca_session_store::fold::operations::ManifestRef { parts: setup, rest };
    assert_eq!(
        *broca_session_loop::run::stored_manifest(&reference, &parts).unwrap(),
        manifest
    );

    // Setup digests cover the stored bytes, not a reader's typed re-serialization.
    let mut newer = parts.get(&answer).unwrap();
    newer["declarations_frozen"]["yield_tools"] = serde_json::json!(["work"]);
    let new_answer = parts.put(PartKind::ProviderAnswer, &newer).unwrap();
    let mut reference = reference;
    *reference
        .parts
        .iter_mut()
        .find(|part| part.kind == PartKind::ProviderAnswer)
        .unwrap() = new_answer;
    broca_session_loop::run::stored_manifest(&reference, &parts).unwrap();
}

#[test]
fn yield_tools_survives_stored_manifest_parts_round_trip() {
    let mut value: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
    value["fetches"][0]["declarations_frozen"]["yield_tools"] = serde_json::json!(["work"]);
    let manifest: broca_wal::PlanManifest = serde_json::from_value(value).unwrap();
    let mut parts = MemoryParts::default();
    let mut setup = Vec::new();
    let rest = broca_session_loop::run::manifest_parts(&manifest, &mut parts, &mut setup).unwrap();
    let reference = broca_session_store::fold::operations::ManifestRef { parts: setup, rest };
    let restored = broca_session_loop::run::stored_manifest(&reference, &parts).unwrap();
    assert_eq!(*restored, manifest);
    let broca_wal::FetchOutcome::Answered {
        declarations_frozen: Some(declarations),
        ..
    } = &restored.fetches[0].outcome
    else {
        panic!("answered provider")
    };
    assert_eq!(
        declarations
            .yield_tools
            .as_ref()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>(),
        ["work"]
    );
}
