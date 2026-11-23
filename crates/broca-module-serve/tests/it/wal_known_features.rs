//! `ck-broca --wal-known-features` is read by placement tooling to decide whether a staged
//! build can read every storage format already written on the machine (the feature floor,
//! `broca_records::floor`). Its output is that contract: the known-name table, sorted, one name
//! per line, nothing else, exit 0.

#[test]
fn wal_known_features_prints_the_sorted_table_and_nothing_else() {
    let data_home = broca_test_support::scratch_root()
        .join(format!("broca-known-features-{}", std::process::id()));
    let output = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::ckdev_command(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        )
        .arg("--wal-known-features")
        // Point the data home at a scratch directory so that, if the probe ever reached logger
        // setup, it would write there and not into the operator's real log directory.
        .env("XDG_DATA_HOME", &data_home)
        // And keep an exported BROCA_STATE_ROOT from pointing it at the operator's real state.
        .env_remove("BROCA_STATE_ROOT")
        .output()
    })
    .unwrap();
    assert!(output.status.success(), "{output:?}");

    let mut expected = broca_records::floor::KNOWN_FEATURES.to_vec();
    expected.sort_unstable();
    let expected: String = expected.iter().map(|name| format!("{name}\n")).collect();
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
    // Also compare with a literal list: deriving both the probe output and the expectation
    // from `KNOWN_FEATURES` alone would let an unintended table change pass unnoticed. The
    // session store is the one storage format this build reads.
    assert_eq!(expected, "session-storage/v1\n");
    assert!(output.stderr.is_empty(), "{:?}", output.stderr);
    assert!(
        !data_home.exists(),
        "the probe must exit before booting anything"
    );
}
