use super::{bind_role, RouteRole};

#[test]
fn each_supported_role_selects_its_own_dispatch() {
    assert_eq!(bind_role(None, true), Ok(RouteRole::LlmRunner));
    for (name, expected) in [
        ("llm-runner", RouteRole::LlmRunner),
        ("classifier", RouteRole::Classifier),
        ("tool-provider", RouteRole::ToolProvider),
        ("permissions", RouteRole::Permissions),
    ] {
        let roles = [(name.into(), "v1".into())].into_iter().collect();
        assert_eq!(bind_role(Some(&roles), true), Ok(expected), "{name}");
    }
}
