//! The live, request-only part of the llm-runner discovery answer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use broca_catalog::{CatalogCache, CatalogSnapshot};
use broca_subc::vault::ScopedList;
use cortexkit_role_llm_runner::serves::{check_serves, Serves, ServesEntry, ServesModels};
use sha2::{Digest, Sha256};

use crate::account_routing::is_readable_gateway;
use crate::auth_routing::{catalog_provider, CatalogProvider};
use crate::manifest::{BROCA_HARNESS, BROCA_HARNESSES, BROCA_UNCAPPED_HARNESS};

pub(crate) async fn describe(
    handler: &impl broca_module::serve::ServeHandler,
    source: Option<&Source>,
) -> broca_wire::RoleDescribeResponse {
    let mut describe = broca_wire::broca_role_describe_with_writers(
        env!("CARGO_PKG_VERSION"),
        handler.compaction_supported(),
        crate::RETENTION_WRITER_ENABLED,
        handler.hook_plane_available(),
    );
    describe.session_class.backup_classes = handler.backup_classes();
    if let Some(source) = source {
        describe.role.serves = Some(source.snapshot().await);
    }
    describe
}

// Correlated milestones distinguish a request that never reached serve from one parked at
// admission, dispatch, or egress. Debug-only, and no prompt or response bodies are recorded.
pub(crate) fn trace_describe(stage: &str, channel: u16, epoch: u32, corr: u64) {
    tracing::debug!(target: "wire.describe", stage, channel, epoch, corr, "role.describe progress");
}

/// Shares the admission listing and refreshes it when routing's request finds it stale.
#[async_trait::async_trait]
pub trait GatewayListing: Send + Sync {
    async fn listing(self: Arc<Self>) -> Option<Arc<ScopedList>>;
}

#[derive(Default)]
pub(crate) struct Source {
    pub catalog: Option<Arc<CatalogCache>>,
    pub vault: Option<Arc<dyn GatewayListing>>,
}

impl Source {
    pub async fn snapshot(&self) -> Serves {
        let catalog = self
            .catalog
            .as_ref()
            .and_then(|cache| cache.snapshot().ok());
        let listing = match (&catalog, &self.vault) {
            (Some(_), Some(vault)) => vault.clone().listing().await,
            _ => None,
        };
        let gateways_ready = self.vault.is_none() || listing.is_some();
        snapshot(catalog.as_ref(), gateways_ready, listing.as_deref())
    }
}

pub(crate) fn requested(params: &serde_json::Value) -> bool {
    params.get("serves").and_then(serde_json::Value::as_bool) == Some(true)
}

/// Build the wire value from complete, already loaded snapshots without contacting either
/// service. To match `role.describe`, supply the listing scoped to broca's own grants.
pub fn snapshot_from_loaded(catalog: &CatalogSnapshot, listing: &ScopedList) -> Serves {
    snapshot(Some(catalog), true, Some(listing))
}

fn snapshot(
    catalog: Option<&CatalogSnapshot>,
    gateways_ready: bool,
    listing: Option<&ScopedList>,
) -> Serves {
    let mut harnesses = BTreeMap::new();
    if let Some(catalog) = catalog {
        harnesses.insert(
            BROCA_UNCAPPED_HARNESS.to_owned(),
            vec![ServesEntry::new("openai", ServesModels::All)],
        );
        if gateways_ready {
            match capped_list(catalog, listing) {
                Ok(entries) => {
                    harnesses.insert(BROCA_HARNESS.to_owned(), entries);
                }
                Err(error) => tracing::warn!(%error, "broca serves list is not representable"),
            }
        }
    }
    let canonical = serde_jcs::to_vec(&harnesses).expect("serves contains only strings and lists");
    Serves::new(format!("{:x}", Sha256::digest(canonical))).with_harnesses(harnesses)
}

#[derive(Default)]
struct Models<'a> {
    admitted: Vec<&'a str>,
    refused: Vec<&'a str>,
}

type CatalogModels<'a> = BTreeMap<&'a str, Models<'a>>;

fn capped_list(
    catalog: &CatalogSnapshot,
    listing: Option<&ScopedList>,
) -> Result<Vec<ServesEntry>, String> {
    let mut models = CatalogModels::new();
    let mut api_key_models = CatalogModels::new();
    for (provider, model) in catalog.published_models() {
        let present_chat = catalog.contains_model(provider, model)
            && broca_catalog::live::refuse_non_chat_model(catalog, provider, model).is_ok();
        let catalog_route = present_chat && catalog.resolve_route(provider, model).is_ok();
        // These logins bring their own request protocol and endpoint. An absent wire.api is
        // not a refusal on either path; admission uses the same persona or Code Assist facts.
        let login_route = present_chat
            && (broca_catalog::family::credential_persona_route(provider, "oauth").is_some()
                || (provider == "google"
                    && catalog.code_assist_route(provider, model, None).is_ok()));
        record(&mut models, provider, model, catalog_route || login_route);
        record(&mut api_key_models, provider, model, catalog_route);
    }

    let mut entries = BTreeMap::new();
    for (&provider, model_lists) in &models {
        add_entry(&mut entries, provider, model_lists)?;
    }
    for (account_provider, catalog_provider) in [
        ("anthropic-platform", "anthropic"),
        ("openai-platform", "openai"),
    ] {
        let model_lists = api_key_models
            .get(account_provider)
            .or_else(|| api_key_models.get(catalog_provider));
        if let Some(model_lists) = model_lists {
            add_entry(&mut entries, account_provider, model_lists)?;
        } else {
            // The account id is known even in a catalog without the vendor; `all` then places
            // nothing until routing sees that vendor's catalog models.
            entries.insert(
                account_provider,
                ServesEntry::new(account_provider, ServesModels::All),
            );
        }
    }
    if let Some(listing) = listing {
        let gateways: BTreeSet<_> = listing
            .credentials
            .iter()
            .filter(|row| is_readable_gateway(row) && row.serves.iter().any(|id| id == "anthropic"))
            .flat_map(|row| row.provider_ids.iter().map(String::as_str))
            .collect();
        for provider in gateways {
            let served = match catalog_provider(listing, provider, |id| catalog.serves_provider(id))
            {
                CatalogProvider::Own => provider,
                CatalogProvider::Borrowed(ref served) if served == "anthropic" => "anthropic",
                _ => continue,
            };
            // The gateway supplies the address and key but uses the borrowed Anthropic model's
            // renderer and other catalog facts, just as a fresh run on the gateway does.
            if let Some(model_lists) = api_key_models.get(served) {
                add_entry(&mut entries, provider, model_lists)?;
            }
        }
    }
    let entries: Vec<_> = entries.into_values().collect();
    let declared = BROCA_HARNESSES.map(String::from);
    let probe = Serves::new("check").with_harnesses(BTreeMap::from([(
        BROCA_HARNESS.to_owned(),
        entries.clone(),
    )]));
    check_serves(&probe, Some(&declared)).map_err(|error| error.to_string())?;
    Ok(entries)
}

fn record<'a>(models: &mut CatalogModels<'a>, provider: &'a str, model: &'a str, admitted: bool) {
    let lists = models.entry(provider).or_default();
    if admitted {
        lists.admitted.push(model);
    } else {
        lists.refused.push(model);
    }
}

/// The runner contract's provider id rule (`check_serves`): a lowercase letter, then lowercase
/// letters, digits, `.`, `_` or `-`, at most 64 bytes.
fn contract_provider_id(provider: &str) -> bool {
    let mut bytes = provider.bytes();
    provider.len() <= 64
        && bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
}

fn add_entry<'a>(
    entries: &mut BTreeMap<&'a str, ServesEntry>,
    provider: &'a str,
    models: &Models<'_>,
) -> Result<(), String> {
    // ChatGPT logins are served only on the uncapped harness, and OpenCode's free models run
    // through Thalamus (Third-Party Harnesses), not broca. OpenCode Go's paid plan is served.
    if matches!(provider, "openai" | "opencode") || models.admitted.is_empty() {
        return Ok(());
    }
    // The contract only carries provider ids that start with a lowercase letter, and the
    // catalog has ids it can't carry (`302ai`). Such a provider can't be placed through
    // routing, so leave it out. One such id must not fail the whole harness's list.
    if !contract_provider_id(provider) {
        static NOTED: std::sync::Once = std::sync::Once::new();
        NOTED.call_once(|| {
            tracing::warn!(
                provider,
                "broca serves list leaves out a provider id the runner contract can't carry"
            );
        });
        return Ok(());
    }
    const MAX_MODEL_IDS: usize = 4_096;
    let entry = if models.refused.is_empty() {
        ServesEntry::new(provider, ServesModels::All)
    } else if models.refused.len() <= MAX_MODEL_IDS {
        ServesEntry::new(provider, ServesModels::All)
            .with_except(models.refused.iter().copied().map(String::from).collect())
    } else if models.admitted.len() <= MAX_MODEL_IDS {
        ServesEntry::new(
            provider,
            ServesModels::List(models.admitted.iter().copied().map(String::from).collect()),
        )
    } else {
        return Err(format!(
            "{provider} exceeds the model id bound in both list forms"
        ));
    };
    entries.insert(provider, entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_provider_id_the_contract_cannot_carry_is_left_out_without_failing_the_list() {
        let models = Models {
            admitted: vec!["chat"],
            refused: vec![],
        };
        let mut entries = BTreeMap::new();
        add_entry(&mut entries, "302ai", &models).unwrap();
        add_entry(&mut entries, "abacus", &models).unwrap();
        assert_eq!(entries.keys().copied().collect::<Vec<_>>(), ["abacus"]);
        let probe = Serves::new("check").with_harnesses(BTreeMap::from([(
            BROCA_HARNESS.to_owned(),
            entries.into_values().collect(),
        )]));
        check_serves(&probe, Some(&BROCA_HARNESSES.map(String::from))).unwrap();
        assert!(!contract_provider_id("302ai"));
        assert!(contract_provider_id("kimi-code-plan-global"));
    }

    #[test]
    fn an_overlong_exception_list_uses_explicit_admitted_models() {
        let ids: Vec<_> = (0..=4_096).map(|n| format!("unsupported-{n}")).collect();
        let models = Models {
            admitted: vec!["chat"],
            refused: ids.iter().map(String::as_str).collect(),
        };
        let mut entries = BTreeMap::new();
        add_entry(&mut entries, "fixture", &models).unwrap();
        assert_eq!(
            entries["fixture"],
            ServesEntry::new("fixture", ServesModels::List(vec!["chat".into()]))
        );
    }
}
