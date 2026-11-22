//! The view helpers that outlive the asynchronous view protocol: the ordinal window a view's
//! work is bounded to, and the account label a view request carries.

use super::*;
use crate::compaction::tests::text;

fn stamped(mut prompt: Vec<Message>) -> Vec<Message> {
    broca_protocol::content::stamp_ordinals(&mut prompt);
    prompt
}

#[test]
fn ordinal_ranges_bound_the_work_to_the_requested_window() {
    let mut prompt = stamped(vec![text(Role::User, "old"); 150_000]);
    prompt[0].role = Role::System;
    let window = ordinal_range(&prompt, 149_800, 149_999);
    assert_eq!(window.len(), 199);
    assert_eq!(window[0].ordinal, Some(149_800));
    assert!(ordinal_range(&prompt, 150_000, u64::MAX).is_empty());
    assert_eq!(ordinal_range(&prompt, 10, 10).len(), 0);
}

fn routed_selection(account_ref: &str) -> broca_records::record::AuthSelection {
    broca_records::record::AuthSelection {
        billing_vault_facts: None,
        credential_id: format!("vault-cred-{account_ref}"),
        account_ref: Some(account_ref.to_owned()),
        resolved_credential_id: Some(format!("vault-cred-{account_ref}")),
        vault_request: None,
        account_decision: Some(broca_records::record::AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: broca_records::record::ACCOUNT_DECISION_ROUTED.to_owned(),
            admission_id: Some("admission".into()),
            decision_id: Some("decision".into()),
            routed_ref: None,
        }),
        auth_method: Some("oauth".into()),
    }
}

fn static_selection() -> broca_records::record::AuthSelection {
    broca_records::record::AuthSelection {
        billing_vault_facts: None,
        credential_id: "oauth:anthropic".into(),
        account_ref: Some("static-acct".into()),
        resolved_credential_id: Some("vault-static".into()),
        vault_request: None,
        account_decision: Some(broca_records::record::AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: broca_records::record::ACCOUNT_DECISION_SINGLE_CANDIDATE.to_owned(),
            admission_id: Some("admission".into()),
            decision_id: None,
            routed_ref: None,
        }),
        auth_method: None,
    }
}

/// The account label is a truncated hash of the vault-served account id: opaque, stable,
/// and pinned here so a change to the hash changes this test, not silent bytes.
#[test]
fn account_labels_are_truncated_hashes_of_the_vault_account() {
    // SHA-256("acct-test-1"), first 32 hex characters (128 bits).
    assert_eq!(
        account_label("acct-test-1"),
        "1a0b66a4c753580e7b1710d6e6057933"
    );
    assert_eq!(
        selection_account_label(&routed_selection("acct-test-1")),
        Some("1a0b66a4c753580e7b1710d6e6057933".into())
    );
    // The static credential's account reports the same way when routing chose nothing.
    assert!(selection_account_label(&static_selection()).is_some());
}
