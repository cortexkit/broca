use std::sync::Arc;
use std::time::Duration;

use broca_module::executor::{CreatingPlanSelection, CreatingPlanSelectionError, RunExecutor};
use broca_subc::vault::ScopedList;
use cortexkit_role_llm_runner::head_selection::{self, PlanModel, PlanModelProblem};

use super::{latest_listing, AuthMethod, LiveLoopExecutor, VaultAuth, FALLBACK_METHOD};

impl LiveLoopExecutor {
    pub(super) fn plan_selection_vault(&self, plan: &serde_json::Value) -> Option<&Arc<VaultAuth>> {
        self.vault
            .as_ref()
            .filter(|_| plan.get("model").is_some() && self.provider_base_url.is_none())
    }

    pub(super) async fn creating_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> CreatingPlanSelection {
        let invalid = CreatingPlanSelectionError::Invalid;
        let Some(vault) = self.plan_selection_vault(plan) else {
            return self.validate_plan_selection(plan, None).map_err(invalid);
        };
        let listing = latest_listing(Arc::clone(vault)).await;
        let checked = self.validate_plan_selection(plan, listing.as_deref());
        // A malformed model is invalid even when the vault listing has not loaded.
        if listing.is_some() || !matches!(&checked, Err(PlanModelProblem::UnsupportedModel)) {
            return checked.map_err(invalid);
        }

        let listing = tokio::time::timeout(crate::VAULT_READY_TIMEOUT, async {
            // The shared cache has no refresh completion signal; poll while it is still empty.
            loop {
                if let Some((listing, _)) = vault.listing.get() {
                    break listing;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| CreatingPlanSelectionError::CredentialsUnavailable)?;
        self.validate_plan_selection(plan, Some(&listing))
            .map_err(invalid)
    }

    pub(super) fn validate_plan_selection(
        &self,
        plan: &serde_json::Value,
        listing: Option<&ScopedList>,
    ) -> Result<Option<PlanModel>, PlanModelProblem> {
        // A provider the catalog doesn't carry is checked against the rows of the catalog
        // provider its vault credentials serve, as admission resolves it.
        let catalog_model = |model: &PlanModel| -> Option<PlanModel> {
            use broca_module_serve::auth_routing::{catalog_provider, CatalogProvider};
            if self.provider_base_url.is_some() {
                return Some(model.clone());
            }
            let catalog = broca_catalog::catalog().ok()?;
            match catalog_provider(listing?, &model.provider, |id| catalog.serves_provider(id)) {
                CatalogProvider::Own => Some(model.clone()),
                CatalogProvider::Borrowed(served) => {
                    let mut served_model = model.clone();
                    served_model.provider = served;
                    Some(served_model)
                }
                CatalogProvider::Unresolved(_) => None,
            }
        };
        let selection = head_selection::check_plan(
            plan,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
            |model| {
                if self.provider_base_url.is_none() {
                    let vault = self.vault.as_ref()?;
                    let key = broca_module_serve::auth_routing::resolve_credential_id(
                        &model.provider,
                        Some(vault.methods.method_for(&model.provider)?),
                        None,
                        &vault.methods,
                        FALLBACK_METHOD,
                    );
                    let resolved = vault.addressing.vault_id_for(&key);
                    let listing = listing?;
                    // Every run asks routing for its account, so any active row routing could
                    // pick serves the plan. A subscription row needs its account id; an API
                    // key is offered by its credential id when it has none.
                    let routed = self.routing.is_some()
                        && listing.credentials.iter().any(|row| {
                            row.state == "active"
                                && row.operations.iter().any(|op| op == "read")
                                && (matches!(key.method, AuthMethod::ApiKey | AuthMethod::Gateway)
                                    || row.account_id.is_some())
                                && row.provider_ids.contains(&model.provider)
                                && row.auth_method.as_deref() == Some(key.method.as_token())
                        });
                    let available = routed
                        || resolved.ok().flatten().is_some_and(|id| {
                            listing.credentials.iter().any(|row| {
                                row.id == id && row.operations.iter().any(|op| op == "read")
                            })
                        });
                    if !available {
                        return None;
                    }
                }
                self.head_selection_wire(&catalog_model(model)?)
            },
        )?;
        if let Some(model) = &selection {
            if let Some(tier) = model.service_tier.filter(|tier| !tier.is_standard()) {
                let catalog =
                    broca_catalog::catalog().map_err(|_| PlanModelProblem::UnsupportedModel)?;
                let model = catalog_model(model).ok_or(PlanModelProblem::UnsupportedModel)?;
                broca_catalog::live::check_service_tier_level(
                    self.selection_family(&model)
                        .ok_or(PlanModelProblem::UnsupportedModel)?,
                    tier.as_str(),
                    &model.provider,
                    &format!("{}/{}", model.provider, model.model),
                    &catalog.priced_modes(&model.provider, &model.model),
                    &catalog.mode_wire_ids(&model.provider, &model.model),
                )
                .map_err(|_| PlanModelProblem::UnsupportedTier)?;
            }
        }
        Ok(selection)
    }
}
