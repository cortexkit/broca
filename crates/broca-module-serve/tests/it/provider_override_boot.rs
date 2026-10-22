//! Malformed runtime endpoints refuse startup before any subc connection is attempted.
#[test]
fn malformed_provider_url_refuses_boot_with_provider_and_value() {
    let path = broca_test_support::scratch_root()
        .join(format!("broca-provider-boot-{}.jsonc", std::process::id()));
    // Valid --subc arguments let startup reach config validation without connecting.
    // The logger runs first and may prune log segments, so isolate the data home to
    // avoid touching the operator's logs. Do not pin a module id to make logging work:
    // unsupervised startup must still support the binary's fallback identity, allowing
    // the provider configuration error to be reported instead of a missing-id error.
    let data_home =
        broca_test_support::scratch_root().join(format!("broca-boot-data-{}", std::process::id()));
    for value in ["", "not-a-url", "ftp://example.com", "http://[bad"] {
        std::fs::write(
            &path,
            serde_json::json!({
                "providers": {"deepseek": {"base_url": value}}
            })
            .to_string(),
        )
        .unwrap();
        let output = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::ckdev_command(
                env!("CARGO_BIN_EXE_ck-broca"),
                broca_test_support::scratch_root(),
            )
            .args(["--subc", "unused-connection.json"])
            .env("BROCA_CONFIG_PATH", &path)
            .env("XDG_DATA_HOME", &data_home)
            // broca's state root defaults to that data home unless BROCA_STATE_ROOT is
            // inherited, which would point this spawn at the operator's real state.
            .env_remove("BROCA_STATE_ROOT")
            .output()
        })
        .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("providers.deepseek.base_url"), "{error}");
        assert!(error.contains(value), "{error}");
        assert!(error.contains("absolute HTTP(S) URL"), "{error}");
    }
    std::fs::remove_file(path).unwrap();
    let _ = std::fs::remove_dir_all(&data_home);
}

/// The isolation above is load-bearing, so it is asserted rather than trusted: a spawn with the
/// data home pinned must leave its logs UNDER THAT ROOT.
///
/// Without this, someone tidying the env lines would see three `.env` calls, judge one redundant
/// with the config path, and silently restore a test that writes to production.
#[test]
fn the_spawned_binary_writes_its_logs_under_the_pinned_data_home() {
    let data_home =
        broca_test_support::scratch_root().join(format!("broca-boot-iso-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_home);
    let path = broca_test_support::scratch_root()
        .join(format!("broca-boot-iso-{}.jsonc", std::process::id()));
    std::fs::write(
        &path,
        serde_json::json!({ "providers": {"deepseek": {"base_url": "not-a-url"}} }).to_string(),
    )
    .unwrap();

    let output = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::ckdev_command(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        )
        .args(["--subc", "unused-connection.json"])
        .env("BROCA_CONFIG_PATH", &path)
        .env("XDG_DATA_HOME", &data_home)
        .env_remove("BROCA_STATE_ROOT")
        .output()
    })
    .unwrap();
    assert!(!output.status.success(), "the boot must still refuse");

    // NON-VACUITY: the binary has to have reached the logger at all, or this test passes by
    // proving nothing. The refusal message is emitted after init, so its presence establishes
    // that the logger ran and therefore that a log directory was resolved SOMEWHERE.
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("providers.deepseek.base_url"),
        "the binary must have got past logger init to the override refusal: {error}"
    );

    let logs = data_home.join("cortexkit").join("broca").join("logs");
    assert!(
        logs.is_dir(),
        "the spawned binary initialised its logger but wrote no directory under the pinned \
         data home ({}); it resolved some OTHER root, which on an unpinned run is the \
         operator's real one",
        logs.display()
    );
    let _ = std::fs::remove_dir_all(&data_home);
    let _ = std::fs::remove_file(&path);
}

/// A `BROCA_CONFIG_PATH` naming no file refuses startup, naming the variable and the path.
/// Every broca.jsonc loader reads a missing file as "no overrides", so without this refusal a
/// typo boots green with every override silently gone.
#[test]
fn a_missing_named_config_file_refuses_boot() {
    let data_home = broca_test_support::scratch_root()
        .join(format!("broca-missing-config-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_home);
    let missing = broca_test_support::scratch_root()
        .join(format!("broca-missing-config-{}.jsonc", std::process::id()));
    let _ = std::fs::remove_file(&missing);
    let output = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::ckdev_command(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        )
        .args(["--subc", "unused-connection.json"])
        .env("BROCA_CONFIG_PATH", &missing)
        .env("XDG_DATA_HOME", &data_home)
        .env_remove("BROCA_STATE_ROOT")
        .output()
    })
    .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("BROCA_CONFIG_PATH") && error.contains(&missing.display().to_string()),
        "a missing named config file must refuse boot by name: {error}"
    );
    let _ = std::fs::remove_dir_all(&data_home);
}
