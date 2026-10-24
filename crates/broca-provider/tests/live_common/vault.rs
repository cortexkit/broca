//! Shared live-test credential entry point. No filesystem or vault read precedes consent.

pub fn live_credential(id: &str) -> Option<String> {
    credential_when(std::env::var("BROCA_LIVE_TESTS").ok().as_deref(), || {
        let credential = broca_module_serve::CredentialId::parse(id)?;
        let subc = std::env::var_os("BROCA_LIVE_SUBC")
            .or_else(|| std::env::var_os("SUBC_SOCKET"))
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| {
                    std::path::PathBuf::from(home)
                        .join(".local/share/cortexkit/run/subc-connection.json")
                })
            })?;
        // Auth factories are synchronous, including ones called inside Tokio tests. A separate
        // thread owns the runtime so these reads never nest a runtime on the caller's thread.
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().ok()?;
            let identity = broca_protocol::BindIdentity::new(
                std::env::current_dir().ok()?,
                "broca-live-tests",
                "credentials",
            );
            runtime
                .block_on(broca_module_serve::devtools::credential(
                    &credential,
                    &subc,
                    &identity,
                ))
                .ok()
        })
        .join()
        .ok()?
    })
}

fn credential_when(consent: Option<&str>, read: impl FnOnce() -> Option<String>) -> Option<String> {
    if consent != Some("1") {
        return None;
    }
    read()
}

#[test]
fn live_credentials_require_explicit_consent_before_reading() {
    for consent in [None, Some(""), Some("0"), Some("true")] {
        assert!(credential_when(consent, || panic!("credential read without consent")).is_none());
    }
    let mut read = false;
    let value = credential_when(Some("1"), || {
        read = true;
        Some("synthetic-token".into())
    });
    assert!(read);
    assert!(
        value.as_deref() == Some("synthetic-token"),
        "token matched: false"
    );
}
