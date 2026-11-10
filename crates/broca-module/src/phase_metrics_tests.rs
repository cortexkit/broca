//! Isolating phase-counter assertions from other tests' process-lifetime work.

pub(crate) fn isolated(test: &str) -> bool {
    const CHILD_TEST: &str = "BROCA_ISOLATED_PHASE_METRICS_TEST";
    if std::env::var(CHILD_TEST).as_deref() == Ok(test) {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD_TEST, test)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("spawn the isolated phase metrics test");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "isolated test {test} failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}
