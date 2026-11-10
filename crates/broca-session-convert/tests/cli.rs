#[test]
fn unknown_flags_are_refused_before_collecting_paths() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_session-convert"))
        .args(["move-check", "--out-file", "not-a-log"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unknown option --out-file"), "{stderr}");
}
