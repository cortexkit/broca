use super::*;

fn describe_with_capabilities(
    capabilities: &[String],
) -> cortexkit_role_llm_runner::describe::RoleDescribe {
    cortexkit_role_llm_runner::describe::RoleDescribe::new(Vec::new(), "test")
        .with_capabilities(capabilities.to_vec())
}

#[test]
fn undeclared_delivery_lists_match_published_refusal_vectors() {
    let raw = cortexkit_role_llm_runner::vectors::FILES
        .iter()
        .find_map(|(name, raw)| (*name == "delivery-order.json").then_some(*raw))
        .unwrap();
    let vectors: serde_json::Value = serde_json::from_str(raw).unwrap();
    let cases = vectors["undeclared_mode_lists"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let params: SendParams = serde_json::from_value(case["request"].clone()).unwrap();
        let capabilities: Vec<String> = serde_json::from_value(case["declares"].clone()).unwrap();
        let describe = describe_with_capabilities(&capabilities);
        let refusal = serde_json::to_value(
            delivery_list_refusal(params.delivery.as_ref(), &describe)
                .expect("a wholly undeclared list must be refused"),
        )
        .unwrap();
        assert_eq!(refusal["code"], case["refusal"]["code"]);
        assert_eq!(refusal["detail"], case["refusal"]["detail"]);
    }
}

#[test]
fn only_undeclared_delivery_modes_are_refused() {
    let params: SendParams = serde_json::from_value(
        serde_json::json!({"prompt":"reminder", "delivery":["turn_system"]}),
    )
    .unwrap();
    let before = serde_json::to_value(&params.delivery).unwrap();
    let describe = broca_wire::broca_role_describe("test").role;
    assert!(delivery_list_refusal(params.delivery.as_ref(), &describe).is_none());
    let describe = describe.with_delivery_order_modes(vec!["steer".into()]);
    let refusal = serde_json::to_value(
        delivery_list_refusal(params.delivery.as_ref(), &describe)
            .expect("a narrowed declaration must still refuse turn_system"),
    )
    .unwrap();
    assert_eq!(refusal["code"], broca_wire::DELIVERY_UNSUPPORTED);
    assert_eq!(refusal["detail"]["delivery"], before);
}

#[test]
fn one_declared_mode_keeps_the_original_delivery_list_accepted() {
    let params: SendParams = serde_json::from_value(
        serde_json::json!({"prompt":"reminder", "delivery":["turn_system","idle_start"]}),
    )
    .unwrap();
    let before = serde_json::to_value(&params.delivery).unwrap();
    let describe = broca_wire::broca_role_describe("test").role;
    assert!(delivery_list_refusal(params.delivery.as_ref(), &describe).is_none());
    assert_eq!(serde_json::to_value(&params.delivery).unwrap(), before);
    let none_declared = describe.with_delivery_order_modes(Vec::new());
    let refusal = serde_json::to_value(
        delivery_list_refusal(params.delivery.as_ref(), &none_declared).unwrap(),
    )
    .unwrap();
    assert_eq!(refusal["detail"]["delivery"], before);
}
