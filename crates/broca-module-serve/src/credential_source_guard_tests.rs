// Included in executor::tests to exercise production admission without reading real stores.

fn credential_store_child(name: &str, foreign: bool) -> bool {
    if std::env::var("BROCA_CREDENTIAL_STORE_CHILD").as_deref() == Ok(name) {
        return true;
    }
    let root = fresh_vault_root(name);
    let data = root.join("data");
    let config = root.join("config");
    std::fs::create_dir_all(config.join("cortexkit/broca")).unwrap();
    std::fs::create_dir_all(data.join("opencode")).unwrap();
    let handles = config.join("cortexkit/broca/vault-handles.json");
    std::fs::write(
        &handles,
        r#"{"deepseek":"tempting-file-token","apikey:deepseek":"tempting-file-token"}"#,
    )
    .unwrap();
    if foreign {
        let credential = r#"{"deepseek":{"type":"api","key":"tempting-file-token"},"openai":{"type":"api","key":"tempting-file-token"}}"#;
        std::fs::write(data.join("opencode/auth.json"), credential).unwrap();
        let home_store = root.join(".local/share/opencode");
        std::fs::create_dir_all(&home_store).unwrap();
        std::fs::write(home_store.join("auth.json"), credential).unwrap();
    }
    // A child owns all environment choices, so concurrently running tests cannot change
    // its home or populate its process-local last-known-good credential cache.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("executor::tests::{name}"),
            "--nocapture",
        ])
        .env("BROCA_CREDENTIAL_STORE_CHILD", name)
        .env("BROCA_CREDENTIAL_STORE_ROOT", &root)
        .env("HOME", &root)
        .env("USERPROFILE", &root)
        .env("XDG_DATA_HOME", &data)
        .env("LOCALAPPDATA", &data)
        .env("APPDATA", &config)
        .env("XDG_CONFIG_HOME", &config)
        .env("BROCA_VAULT_HANDLES_PATH", &handles)
        .current_dir(&root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "the credential child must execute its test"
    );
    std::fs::remove_dir_all(root).unwrap();
    false
}

async fn refuse_file_credentials() {
    let root = std::path::PathBuf::from(std::env::var_os("BROCA_CREDENTIAL_STORE_ROOT").unwrap());
    let daemon =
        CapturingVaultDaemon::start_with(&root, RouteOpenScript::RefuseForever("unknown_module"))
            .await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let decision = broca_test_support::wait_for(
        "credential admission with unavailable vault",
        executor.decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None),
        || format!("opens: {}", daemon.route_open_count()),
    )
    .await;
    assert!(
        matches!(decision, Err(RunError::Config(ref message)) if message.contains("unknown_module")),
        "a populated local store must not authorize a provider: {:?}",
        decision.err()
    );
    assert_eq!(daemon.route_open_count(), 1);
    assert!(
        daemon.requests().is_empty(),
        "the unavailable vault received no credential request"
    );
    assert!(executor
        .vault
        .as_ref()
        .unwrap()
        .cache
        .get("apikey:deepseek")
        .is_none());
    daemon.assert_healthy();
}

#[tokio::test]
async fn daemon_serve_path_never_resolves_a_vault_handles_file() {
    if credential_store_child(
        "daemon_serve_path_never_resolves_a_vault_handles_file",
        false,
    ) {
        refuse_file_credentials().await;
    }
}

#[tokio::test]
async fn shipped_sources_never_name_another_apps_credential_store() {
    if credential_store_child(
        "shipped_sources_never_name_another_apps_credential_store",
        true,
    ) {
        refuse_file_credentials().await;
    }
}
