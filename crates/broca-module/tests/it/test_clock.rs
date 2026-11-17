//! The gate requires this test's PASS record to prove the daemon feature reached the
//! actor package. Reading the file also proves it reached the shared core clock.

#[test]
fn test_clock_feature_is_enabled_for_retention_tests() {
    let root = broca_test_support::scratch_root()
        .join(format!("retention-clock-sentinel-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("test-clock"), r#"{"offset_ms":3601000}"#).unwrap();
    let clock = broca_core::Clock::for_state_root(&root).unwrap();
    assert_eq!(*clock.subscribe().borrow(), 3_601_000);
    let real = broca_core::Clock::default().now_ms();
    assert!(clock.now_ms() >= real.saturating_add(3_601_000));
    std::fs::remove_dir_all(root).unwrap();
}
