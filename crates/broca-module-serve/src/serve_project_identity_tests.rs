use super::*;

#[test]
fn stray_legacy_bind_members_do_not_change_candidate_identity() {
    let clean: SubcBindIdentity = serde_json::from_value(serde_json::json!({
        "harness":"runner", "session":"seat"
    }))
    .unwrap();
    let stray: SubcBindIdentity = serde_json::from_value(serde_json::json!({
        "harness":"runner", "session":"seat",
        "project_root":"/forged/legacy/project", "project_id":"forged-project"
    }))
    .unwrap();
    let clean = to_broca_identity(&clean);
    let stray = to_broca_identity(&stray);
    assert_eq!(stray, clean);
    assert_eq!(
        broca_core::session_addr(&stray),
        broca_core::session_addr(&clean)
    );
    assert_eq!(
        clean.project_root,
        std::path::Path::new("broca-unscoped-v1")
    );
}

#[tokio::test]
async fn unscoped_namespace_bypasses_directory_validation_independently_of_cwd() {
    const CHILD: &str = "BROCA_NAMESPACE_GUARD_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let root = broca_test_support::scratch_root()
            .join(format!("namespace-cwd-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("serve::project_identity_tests::unscoped_namespace_bypasses_directory_validation_independently_of_cwd")
            .arg("--exact")
            .current_dir(&root)
            .env(CHILD, "1")
            .output().unwrap();
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let namespace = std::path::Path::new(broca_protocol::session::UNSCOPED_SESSION_ROOT);
    let session = BrocaBindIdentity::new(namespace, "runner", "seat");
    for exists in [false, true, false] {
        if exists {
            std::fs::create_dir(namespace).unwrap();
        } else if namespace.exists() {
            std::fs::remove_dir(namespace).unwrap();
        }
        assert_eq!(execute_class_root_refusal(&session).await, None);
    }
    let missing = BrocaBindIdentity::new(
        std::env::current_dir().unwrap().join("missing-checkout"),
        "runner",
        "seat",
    );
    assert!(execute_class_root_refusal(&missing).await.is_some());
    let relative = BrocaBindIdentity::new("broca-unscoped-v1-other", "runner", "seat");
    assert!(execute_class_root_refusal(&relative).await.is_some());
    for spelling in ["./broca-unscoped-v1", "broca-unscoped-v1/"] {
        let relative = BrocaBindIdentity::new(spelling, "runner", "seat");
        assert!(execute_class_root_refusal(&relative).await.is_some());
    }
}
