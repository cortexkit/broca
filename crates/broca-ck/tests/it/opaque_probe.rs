use broca_ck::OpaqueSource;

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
}
