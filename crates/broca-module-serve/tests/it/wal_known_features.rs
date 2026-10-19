//! `ck-broca --wal-known-features` is read by placement tooling to decide whether a staged
//! build can read every WAL feature already written on the machine. Its output is that
//! contract: the known-name table, sorted, one name per line, nothing else, exit 0.

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

    let mut expected = broca_wal::KNOWN_FEATURES.to_vec();
    expected.sort_unstable();
    let expected: String = expected.iter().map(|name| format!("{name}\n")).collect();
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
    // Also compare with a literal list: deriving both the probe output and the expectation
    // from `KNOWN_FEATURES` alone would let an unintended table change pass unnoticed. The
    // expected list covers the features this build can read, including step transforms,
    // the reader-only compaction and deferred-call records, step-boundary cancellation and
    // first-send session classes, hard input-token caps, provider text state, OpenRouter reasoning, runner tags,
    // and structured-output state including durable compiler-refusal downgrades.
    assert_eq!(
        expected,
        "agent-run-scopes/v1\narchive-index/v2\ncompaction-wait/v1\ncompaction/v1\ndeferred-call/v1\ndelivery-order/v1\ndispatch-module/v1\nflow-scopes/v1\ninput-token-cap/v1\nopenrouter-reasoning/v1\nplan-manifest/v1\nprovider-text-state/v1\nrestart-pause/v1\nretention/v1\nrun-cancel-at/v1\nrunner-tags/v1\nscope/v1\nsession-class/v1\nsteer-queue/v1\nstep-transforms/v1\nstructured-output-downgrade/v1\nstructured-output/v1\n"
    );
    assert!(output.stderr.is_empty(), "{:?}", output.stderr);
    assert!(
        !data_home.exists(),
        "the probe must exit before booting anything"
    );
}
