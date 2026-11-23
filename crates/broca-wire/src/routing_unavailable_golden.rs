use super::*;

/// The reply for each reason, byte for byte. The same files are compared with the reply
/// the real serve path sends (`account_selection_real_daemon`), so this pins the type and
/// that test pins the path.
#[test]
fn each_reason_serializes_to_its_golden_file() {
    for (body, file) in [
        (
            RoutingUnavailable::new("unreachable", None),
            "routing_unavailable.unreachable.json",
        ),
        (
            RoutingUnavailable::new("no_decision_within_budget", Some(1500)),
            "routing_unavailable.no_decision_within_budget.json",
        ),
        (
            RoutingUnavailable::new("invalid_pick", None),
            "routing_unavailable.invalid_pick.json",
        ),
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(file);
        let golden = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()));
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&body).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized refusal differs from {}",
            path.display()
        );
        assert_eq!(
            serde_json::from_slice::<RoutingUnavailable>(&golden).unwrap(),
            body
        );
    }
}
