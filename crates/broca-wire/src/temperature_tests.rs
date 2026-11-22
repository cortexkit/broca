use super::check_run_parameters;
use serde_json::json;

#[test]
fn temperature_invalid_values_name_the_exact_wire_path() {
    for value in [
        json!(-0.1),
        json!(2.1),
        json!(null),
        json!("0.1"),
        json!(false),
        json!([]),
        json!({}),
    ] {
        assert_eq!(
            check_run_parameters(&json!({"generation": {"temperature": value}})),
            Err("generation.temperature".into())
        );
    }
    assert_eq!(
        check_run_parameters(&json!({"generation": []})),
        Err("generation".into())
    );
    for value in [0.0, 0.25, 2.0] {
        assert!(check_run_parameters(
            &json!({"generation": {"temperature": value, "max_output_tokens": 42}})
        )
        .is_ok());
    }
}
