use broca_ck::{CkMessage, OpaqueSource};

#[test]
fn opaque_source_round_trips_the_pinned_wire_shape() {
    let source = OpaqueSource::Wire {
        wire: "openai_responses".into(),
    };
    let rendered = serde_json::to_value(&source).expect("serialize OpaqueSource::Wire");
    assert_eq!(
        rendered,
        serde_json::json!({"source": "wire", "wire": "openai_responses"})
    );

    let golden: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/ck_wire_golden.json")).unwrap();
    let last = golden.as_array().unwrap().last().unwrap().clone();
    let parsed: CkMessage =
        serde_json::from_value(last).expect("deserialize golden reasoning vector");
    let rerendered = serde_json::to_value(parsed).expect("re-serialize golden reasoning vector");
    assert_eq!(
        rerendered,
        golden.as_array().unwrap().last().unwrap().clone()
    );
}
