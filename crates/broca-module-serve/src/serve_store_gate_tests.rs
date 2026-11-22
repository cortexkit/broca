use super::*;

#[test]
fn the_store_gate_serves_inline_import_output_references_and_run_event_bodies() {
    for method in [
        OP_IMPORT,
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REFS,
        cortexkit_role_llm_runner::ops::SESSION_OUTPUT_REF_READ,
    ] {
        assert!(served_on_new_storage(
            method,
            &serde_json::json!({"messages":[]})
        ));
    }
    assert!(served_on_new_storage(
        OP_IMPORT,
        &serde_json::json!({"source":{}})
    ));
    // Basal fetches each run event's body before it trusts the event; refusing it made every
    // program model call wait for its scheduled check instead.
    assert!(served_on_new_storage(
        "run.event.get",
        &serde_json::json!({})
    ));
}
