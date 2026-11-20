use super::{attempt_credential, attempt_credential_for_send};
use serde_json::json;

#[test]
fn reservation_credential_facts_never_classify_prefix_or_provider() {
    for (credential_id, method) in [
        ("apikey:opencode-go", "apikey"),
        ("apikey:kimi-for-coding", "apikey"),
        ("apikey:cline-pass", "apikey"),
        ("oauth:xai", "oauth"),
    ] {
        let selection =
            serde_json::from_value(json!({"credential_id":credential_id,"auth_method":method}))
                .unwrap();
        let credential = attempt_credential(Some(&selection));
        assert_eq!(credential.auth_method.as_deref(), Some(method));
        assert_eq!(credential.credential_ref, None);
        let value = serde_json::to_value(credential).unwrap();
        for key in [
            "provider_ids",
            "plan_tier",
            "plan_tier_source",
            "kind",
            "billing_model",
        ] {
            assert!(
                value.get(key).is_none(),
                "{credential_id}: {key} must remain unreported"
            );
        }
    }
}

#[test]
fn reservation_identity_and_auth_method_use_only_frozen_selection_witnesses() {
    let mut selection = serde_json::from_value(json!({
        "credential_id":"oauth:misleading-provider:lookup-only", "resolved_credential_id":"vault-opaque-row", "auth_method":"future-vault-method"
    })).unwrap();
    let credential = attempt_credential(Some(&selection));
    assert_eq!(
        credential.credential_ref.as_deref(),
        Some("vault-opaque-row")
    );
    assert_eq!(
        credential.auth_method.as_deref(),
        Some("future-vault-method")
    );
    assert_eq!(credential.auth_selection.as_ref(), Some(&selection));
    selection.resolved_credential_id = None;
    selection.auth_method = None;
    let unknown = attempt_credential(Some(&selection));
    assert_eq!(unknown.credential_ref, None);
    assert_eq!(unknown.auth_method, None);
    assert_eq!(attempt_credential(None).auth_selection, None);
}

#[test]
fn refreshed_reservation_forgets_row_witness_but_keeps_served_account() {
    let selection = serde_json::from_value(json!({
        "credential_id":"lookup-not-a-witness", "resolved_credential_id":"frozen-row", "auth_method":"oauth", "account_ref":"frozen-account"
    })).unwrap();
    let frozen = attempt_credential(Some(&selection));
    for refreshed in [false, true] {
        let sent = attempt_credential_for_send(&frozen, Some("served-account".into()), refreshed);
        assert_eq!(
            sent.credential_ref.as_deref(),
            if refreshed { None } else { Some("frozen-row") }
        );
        assert_eq!(sent.account_ref.as_deref(), Some("served-account"));
        assert_eq!(sent.auth_selection.as_ref(), Some(&selection));
        assert_eq!(sent.auth_method.as_deref(), Some("oauth"));
        assert!(
            sent.provider_ids.is_none()
                && sent.plan_tier.is_none()
                && sent.plan_tier_source.is_none()
        );
    }
}
