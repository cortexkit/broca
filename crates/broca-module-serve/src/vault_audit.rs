//! `vault.audit`: set what the credential vault lists under broca's read grants against the
//! credentials broca's configuration routes to.
//!
//! It exists for the one credential fault the vault cannot see from its own side: a
//! credential broca still routes to that has DISAPPEARED from the vault, or that broca's
//! grants no longer cover. Inside the vault, a credential nobody reads looks the same whether
//! or not a consumer depends on it, so the detector sits on the consumer's side.
//!
//! The listing is `credential.list_scoped` under the daemon's own supervised identity, the
//! same principal every run reads credentials as. So the audit answers the question runs
//! depend on, "can broca read this", rather than a proxy for it.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use broca_subc::vault::ScopedList;
use broca_subc::VaultError;
use broca_wire::{VaultAuditMissing, VaultAuditResponse, VaultAuditRow};

use crate::{CredentialId, MethodConfig, VaultAddressing};

/// The daemon-owned capability the `vault.audit` op calls. Implemented by the process that
/// holds the vault client; the serve layer only forwards the answer.
pub trait VaultAuditor: Send + Sync {
    fn audit(&self) -> Pin<Box<dyn Future<Output = VaultAuditResponse> + Send + '_>>;
}

/// The credentials broca's configuration routes to, as vault credential id -> the broca
/// routing keys that resolve to it.
///
/// Two sources name a credential: each provider's configured default method
/// (`auth-methods.json`, giving `<method>:<provider>`), and each `vault.credential_ids` alias.
/// A routing key reached only by a per-request method or account pin is not configuration,
/// so it is not listed; give it an alias to have the audit watch it.
pub fn referenced_credentials(
    methods: &MethodConfig,
    addressing: &VaultAddressing,
) -> BTreeMap<String, Vec<String>> {
    let mut referenced: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut add = |key: String, vault_id: String| {
        let keys = referenced.entry(vault_id).or_default();
        if !keys.contains(&key) {
            keys.push(key);
        }
    };
    for (provider, method) in methods.defaults() {
        let key = CredentialId::new(method, provider, None);
        add(
            key.to_id_string(),
            addressing.vault_id_for(&key).into_owned(),
        );
    }
    for (key, vault_id) in addressing.aliases() {
        add(key.to_string(), vault_id.to_string());
    }
    for keys in referenced.values_mut() {
        keys.sort_unstable();
    }
    referenced
}

/// Build the audit answer from the vault's listing.
///
/// Every listed row is reported, whatever its state. Every referenced credential the listing
/// did not include is reported as missing. A listing that did not arrive is `Unanswered`,
/// never an empty `Checked`.
pub fn audit_listing(
    principal: Option<String>,
    listing: Result<ScopedList, VaultError>,
    referenced: &BTreeMap<String, Vec<String>>,
) -> VaultAuditResponse {
    let Some(principal) = principal else {
        return VaultAuditResponse::Unanswered {
            reason: "no supervised identity to list credentials by grant".to_string(),
        };
    };
    let list = match listing {
        Ok(list) => list,
        Err(error) => {
            return VaultAuditResponse::Unanswered {
                reason: format!("credential.list_scoped: {}", vault_error_label(&error)),
            }
        }
    };
    let rows: Vec<VaultAuditRow> = list
        .credentials
        .iter()
        .map(|row| VaultAuditRow {
            credential_id: row.id.clone(),
            credential_type: row.credential_type.clone(),
            state: row.state.clone(),
            account_id: row.account_id.clone(),
            referenced_by: referenced.get(&row.id).cloned().unwrap_or_default(),
        })
        .collect();
    let missing = referenced
        .iter()
        .filter(|(vault_id, _)| !list.credentials.iter().any(|row| &row.id == *vault_id))
        .map(|(vault_id, keys)| VaultAuditMissing {
            credential_id: vault_id.clone(),
            referenced_by: keys.clone(),
        })
        .collect();
    VaultAuditResponse::Checked {
        principal,
        view: list.view,
        rows,
        missing,
    }
}

/// A fixed label per vault error class. `Protocol` and `Transport` carry upstream text, which
/// is why `VaultError` has a hand-written `Debug` that redacts it; this keeps the same
/// discipline by naming the class and never the message. Exhaustive, so a new class cannot
/// reach the audit's output unlabelled.
pub fn vault_error_label(error: &VaultError) -> &'static str {
    match error {
        VaultError::NotFound => "not found (no grant covers this principal)",
        VaultError::NeedsReauth => "credential needs re-authentication",
        VaultError::RefreshFailed => "credential refresh failed",
        VaultError::RefreshUnsupported => "credential is not refreshable",
        VaultError::VaultLocked => "vault is locked",
        VaultError::Corrupt => "credential record is corrupt",
        VaultError::TooManyItems => "vault reported too many items",
        VaultError::TtlUnsatisfiable => "vault could not satisfy the requested token lifetime",
        VaultError::Protocol(_) => "vault protocol failure",
        VaultError::Transport(_) => "vault transport failure",
        VaultError::NoConsumerIdentity { .. } => {
            "no supervised identity to read a credential by grant"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VaultCredentialIds;
    use broca_subc::vault::{ScopedCredential, ScopedList};

    fn row(id: &str, credential_type: &str, state: &str) -> ScopedCredential {
        ScopedCredential {
            id: id.into(),
            categories: vec!["llm-provider".into()],
            credential_type: credential_type.into(),
            serves: Vec::new(),
            refresh_adapter: None,
            state: state.into(),
            record_version: 1,
            operations: vec!["read".into()],
            account_id: None,
            email: None,
            org_name: None,
            provider_ids: Vec::new(),
            auth_method: None,
        }
    }

    fn listing(rows: Vec<ScopedCredential>) -> ScopedList {
        ScopedList {
            credentials: rows,
            grants: 1,
            grant_tuples: Vec::new(),
            view: "view-digest".into(),
        }
    }

    fn methods(json: &str) -> MethodConfig {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-vault-audit-methods-{}-{}.json",
            std::process::id(),
            json.len()
        ));
        std::fs::write(&path, json).unwrap();
        let config = MethodConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        config
    }

    #[test]
    fn configuration_references_method_defaults_and_aliases_by_vault_id() {
        let methods = methods(r#"{"defaults":{"anthropic":"oauth","openai":"chatgpt"}}"#);
        let addressing = VaultAddressing::new(VaultCredentialIds::from_pairs([
            ("apikey:opencode", "apikey:opencode-go"),
            ("chatgpt:openai", "chatgpt:openai:gmail"),
        ]));
        let referenced = referenced_credentials(&methods, &addressing);
        assert_eq!(
            referenced,
            BTreeMap::from([
                (
                    "apikey:opencode-go".to_string(),
                    vec!["apikey:opencode".to_string()]
                ),
                (
                    "chatgpt:openai:gmail".to_string(),
                    vec!["chatgpt:openai".to_string()]
                ),
                (
                    "oauth:anthropic".to_string(),
                    vec!["oauth:anthropic".to_string()]
                ),
            ])
        );
    }

    /// THE FINDING THE AUDIT EXISTS FOR: a credential broca routes to that the vault did not
    /// list is named as missing, with the routing keys that depend on it.
    #[test]
    fn a_referenced_credential_the_vault_did_not_list_is_missing() {
        let referenced = BTreeMap::from([
            (
                "oauth:anthropic".to_string(),
                vec!["oauth:anthropic".to_string()],
            ),
            (
                "apikey:kimi-for-coding".to_string(),
                vec!["apikey:kimi-code-plan-global".to_string()],
            ),
        ]);
        let answer = audit_listing(
            Some("reserved:broca".into()),
            Ok(listing(vec![
                row("oauth:anthropic", "oauth", "needs_reauth"),
                row("apikey:openrouter", "apikey", "active"),
            ])),
            &referenced,
        );
        let VaultAuditResponse::Checked {
            principal,
            view,
            rows,
            missing,
        } = answer
        else {
            panic!("the vault answered, so the audit checked");
        };
        assert_eq!(principal, "reserved:broca");
        assert_eq!(view, "view-digest");
        assert_eq!(
            missing,
            vec![VaultAuditMissing {
                credential_id: "apikey:kimi-for-coding".into(),
                referenced_by: vec!["apikey:kimi-code-plan-global".into()],
            }]
        );
        // Every row is reported, a needs_reauth one with its state, and only referenced rows
        // name the keys routing to them.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].state, "needs_reauth");
        assert_eq!(rows[0].referenced_by, vec!["oauth:anthropic".to_string()]);
        assert_eq!(rows[1].credential_id, "apikey:openrouter");
        assert!(rows[1].referenced_by.is_empty());
    }

    /// A listing that did not arrive, or no identity to ask with, is unanswered: never an
    /// empty check that would read as "nothing is missing".
    #[test]
    fn an_unanswered_listing_is_never_an_empty_check() {
        let referenced = BTreeMap::from([(
            "oauth:anthropic".to_string(),
            vec!["oauth:anthropic".to_string()],
        )]);
        for (principal, listing) in [
            (
                Some("reserved:broca".to_string()),
                Err(VaultError::Transport(
                    "socket reset: secret-ish upstream text".into(),
                )),
            ),
            (
                Some("reserved:broca".to_string()),
                Err(VaultError::NotFound),
            ),
            (None, Ok(listing(Vec::new()))),
        ] {
            let answer = audit_listing(principal, listing, &referenced);
            let VaultAuditResponse::Unanswered { reason } = answer else {
                panic!("no listing must be unanswered, got {answer:?}");
            };
            assert!(
                !reason.contains("secret-ish"),
                "upstream text must not reach the answer: {reason}"
            );
        }
    }
}
