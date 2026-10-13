//! Proves that finding a credential is not permission to spend it.
//!
//! Issue #11: `cargo test --workspace` on a FRESH CLONE made 8 real, billed provider calls
//! against credentials the contributor had never configured for broca -- sourced from another
//! application's store at `~/.local/share/opencode/auth.json`. They had set nothing.
//!
//! THE FEATURE FLAG WAS NOT THE GATE IT LOOKED LIKE. `live` is off by default for this crate,
//! but broca-run, broca-subc and broca-module-serve all enable `broca-provider/live`, and cargo
//! UNIFIES features across a workspace build -- so `--workspace` compiles these tests with `live`
//! on regardless of this crate's own default. A feature cannot express consent when any dependent
//! can turn it on.
//!
//! So this asserts the property directly, and it is only meaningful ON A MACHINE THAT HAS THE
//! CREDENTIALS -- which is exactly the machine where the bug bills money. It self-skips elsewhere
//! rather than passing vacuously.

use crate::live_common;

/// With no opt-in, credential lookup must yield nothing even when the file is right there.
///
/// Reads the auth file directly to establish the PRECONDITION rather than assuming it: if the
/// machine has no opencode credentials, a None from the gated path proves nothing, because None
/// is also what absence returns. Two causes, one value -- so the test says which one it is in.
#[test]
fn a_present_credential_is_not_consent_to_spend_it() {
    let home = match std::env::var_os("HOME") {
        Some(h) => std::path::PathBuf::from(h),
        None => {
            eprintln!("SKIP a_present_credential_is_not_consent_to_spend_it: no HOME");
            return;
        }
    };
    let auth_path = home.join(".local/share/opencode/auth.json");
    let raw = match std::fs::read_to_string(&auth_path) {
        Ok(text) => text,
        Err(_) => {
            eprintln!(
                "SKIP a_present_credential_is_not_consent_to_spend_it: no credential store at {} \
                 -- this assertion is only load-bearing where the credentials exist",
                auth_path.display()
            );
            return;
        }
    };

    // The precondition: at least one api-type entry the harness would otherwise spend against.
    let parsed: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("SKIP a_present_credential_is_not_consent_to_spend_it: unparseable store");
            return;
        }
    };
    let spendable: Vec<String> = parsed
        .as_object()
        .map(|entries| {
            entries
                .iter()
                .filter(|(_, entry)| entry.get("type").and_then(|t| t.as_str()) == Some("api"))
                .filter(|(_, entry)| {
                    entry
                        .get("key")
                        .and_then(|k| k.as_str())
                        .is_some_and(|k| !k.is_empty())
                })
                .map(|(name, _)| name.clone())
                .collect()
        })
        .unwrap_or_default();

    if spendable.is_empty() {
        eprintln!(
            "SKIP a_present_credential_is_not_consent_to_spend_it: store has no api-type keys"
        );
        return;
    }

    // BROCA_LIVE_TESTS is deliberately NOT set or unset here. Mutating a process-global env var
    // races every other test in this binary, and the case worth protecting is the DEFAULT one --
    // a contributor who set nothing. Running under the ordinary environment is the honest test of
    // that, and CI sets no opt-in.
    if std::env::var("BROCA_LIVE_TESTS").is_ok_and(|v| !v.trim().is_empty()) {
        eprintln!(
            "SKIP a_present_credential_is_not_consent_to_spend_it: BROCA_LIVE_TESTS is set, so \
             this run has opted in and the default-deny property cannot be observed"
        );
        return;
    }

    for provider in &spendable {
        assert!(
            live_common::opencode_api_key(provider).is_none(),
            "opencode_api_key({provider:?}) returned a key with no BROCA_LIVE_TESTS opt-in.\n\
             \n\
             The credential store at {} holds a spendable key for {provider}, and the harness\n\
             would bill it on a bare `cargo test --workspace`. Finding a credential is not\n\
             permission to spend it -- see issue #11.",
            auth_path.display()
        );
    }
}
