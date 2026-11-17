use super::*;
use broca_core::tool_plane::{FrozenProviderDeclarations, PlanManifest};
use broca_fetch_plan::admit::{decode_transport_catalog, ToolCatalog, OPTIONAL_TOOL_BUDGET};
use std::sync::LazyLock;

// A responsive provider's change reaches every run starting a minute later, with at most
// one catalog read attempt per minute per (provider, preset), even during an outage.
// A route failure clears the throttle so a restarted provider can be read immediately.
const TRANSPORT_CACHE_TTL: Duration = Duration::from_secs(60);

type Key = (String, Option<String>);
#[derive(Default)]
struct Entry {
    state: Mutex<State>,
    refresh: tokio::sync::Mutex<()>,
}
#[derive(Default)]
struct State {
    latest: Option<Arc<Snapshot>>,
    last_success: Option<Instant>,
    last_attempt: Option<Instant>,
}
type Slot = Arc<Entry>;
static CACHE: LazyLock<Mutex<HashMap<Key, Slot>>> = LazyLock::new(Mutex::default);
static WARNINGS: LazyLock<Mutex<HashSet<(String, &'static str)>>> = LazyLock::new(Mutex::default);

struct Snapshot {
    catalog: ToolCatalog,
    declarations: FrozenProviderDeclarations,
}

pub(super) fn invalidate(provider: &str) {
    CACHE
        .lock()
        .unwrap()
        .retain(|(name, _), _| name != provider);
}

fn warn(provider: &str, cause: &'static str) {
    if WARNINGS
        .lock()
        .unwrap()
        .insert((provider.to_owned(), cause))
    {
        // A provider's error body may contain private data; log only the discovery cause.
        tracing::warn!(target: "tool_transport", provider, cause,
            "run transport discovery unavailable; retaining newest known declarations");
    }
}

fn transport_only(mut facts: FrozenProviderDeclarations) -> FrozenProviderDeclarations {
    facts.schema_pins = None;
    facts
}

fn drifted_tool<'a>(
    manifest: &'a PlanManifest,
    provider: &str,
    old: &FrozenProviderDeclarations,
    current: &Snapshot,
) -> Option<&'a str> {
    manifest
        .tools
        .iter()
        .filter(|tool| tool.module.as_deref() == Some(provider))
        .find_map(|tool| {
            let Some(now) = current
                .catalog
                .tools
                .iter()
                .find(|now| now.name == tool.name)
            else {
                return Some(tool.name.as_str());
            };
            let unchanged = match (
                old.schema_pins
                    .as_ref()
                    .and_then(|pins| pins.get(&tool.name))
                    .filter(|pin| !pin.schema_digest.is_empty()),
                current
                    .catalog
                    .schema_pins
                    .get(&tool.name)
                    .filter(|pin| !pin.schema_digest.is_empty()),
            ) {
                (Some(before), Some(after)) => before == after,
                _ => {
                    now.name == tool.name
                        && now.description == tool.description
                        && now.input_schema == tool.input_schema
                }
            };
            (!unchanged).then_some(tool.name.as_str())
        })
}

impl SubcToolPlane {
    pub(super) async fn refresh_run_transport(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        manifest: &PlanManifest,
    ) -> BTreeMap<String, FrozenProviderDeclarations> {
        let Ok(dispatch) = ToolManifest::from_shared_tools(Arc::clone(&manifest.tools))
            .with_plan_manifest(manifest)
        else {
            return BTreeMap::new();
        };
        let providers: BTreeSet<_> = manifest
            .tools
            .iter()
            .filter_map(|tool| tool.module.as_deref())
            .collect();
        let deadline =
            Instant::now() + OPTIONAL_TOOL_BUDGET.min(broca_fetch_plan::ADMISSION_DEADLINE);
        let Ok(plan) = serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
        else {
            return BTreeMap::new();
        };
        let mut overrides = BTreeMap::new();
        for provider in providers {
            let key = (
                provider.to_owned(),
                dispatch.provider_preset(provider).map(str::to_owned),
            );
            let slot = CACHE
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone();
            // Serialize refreshes for the same key, including across concurrent session starts.
            let refresh = tokio::time::timeout_at(deadline, slot.refresh.lock()).await;
            if let Ok(_guard) = refresh {
                let due = {
                    let mut state = slot.state.lock().unwrap();
                    let recent = [state.last_success, state.last_attempt]
                        .into_iter()
                        .flatten()
                        .any(|time| time.elapsed() < TRANSPORT_CACHE_TTL);
                    if !recent {
                        state.last_attempt = Some(Instant::now());
                    }
                    !recent
                };
                if due {
                    let item = plan["tool_items"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|item| item["provider"].as_str() == Some(provider));
                    let params = item
                        .and_then(|item| item["params"].as_object())
                        .cloned()
                        .unwrap_or_default();
                    let mut request =
                        cortexkit_role_tool_provider::catalog::CatalogRequest::new(params);
                    if let Some(preset) = &key.1 {
                        request = request.with_preset(preset.clone());
                    }
                    match tokio::time::timeout_at(
                        deadline,
                        self.read_transport(bind, scope, provider, &request, &dispatch),
                    )
                    .await
                    {
                        Ok(Ok(snapshot)) => {
                            let mut state = slot.state.lock().unwrap();
                            state.latest = Some(Arc::new(snapshot));
                            state.last_success = Some(Instant::now());
                        }
                        Ok(Err(_)) => warn(provider, "lookup_failed"),
                        Err(_) => warn(provider, "lookup_unfinished"),
                    }
                }
            } else {
                warn(provider, "lookup_unfinished");
            }
            let current = slot.state.lock().unwrap().latest.clone();
            let Some(current) = current else {
                continue;
            };
            let frozen = dispatch
                .provider_declarations(provider)
                .cloned()
                .unwrap_or_default();
            if let Some(tool) = drifted_tool(manifest, provider, &frozen, &current) {
                tracing::info!(target: "tool_transport", provider, tool,
                    "tool schema drift keeps provider transport declarations frozen for this run");
                continue;
            }
            let current = transport_only(current.declarations.clone());
            if current != transport_only(frozen) {
                overrides.insert(provider.to_owned(), current);
            }
        }
        overrides
    }

    async fn read_transport(
        &self,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        provider: &str,
        request: &cortexkit_role_tool_provider::catalog::CatalogRequest,
        dispatch: &ToolManifest,
    ) -> Result<Snapshot, ToolPlaneError> {
        let catalog = self.refresh_catalog_snapshot().await?;
        let entry = crate::provider_catalog::find_module(&catalog, provider).ok_or_else(|| {
            ToolPlaneError::Unavailable("provider absent from daemon catalog".into())
        })?;
        let keepalive = crate::provider_catalog::catalog_entry_provides(
            entry,
            cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY,
        );
        let call_yield = crate::provider_catalog::catalog_entry_provides(
            entry,
            cortexkit_role_tool_provider::CALL_YIELD_V1,
        );
        let selector = scope
            .map(frozen_scope_selector)
            .transpose()
            .map_err(ToolPlaneError::Unavailable)?;
        let route = self
            .conn
            .route_open_dispatch(
                provider,
                bind,
                selector.as_ref(),
                RouteShape {
                    tool_provider_v1: true,
                    session_identity: dispatch.uses_session_identity(provider),
                    ..Default::default()
                },
                false,
                Some(Arc::clone(&catalog.aft_harness)),
            )
            .await
            .map_err(conn_to_plane)?;
        // Even a timed-out catalog must release the route it opened.
        struct Close<'a>(&'a SubcToolPlane, RouteHandle);
        impl Drop for Close<'_> {
            fn drop(&mut self) {
                self.0.close_route_best_effort(self.1);
            }
        }
        let _close = Close(self, route);
        let args = serde_json::to_value(request)
            .map_err(|error| ToolPlaneError::Unavailable(error.to_string()))?;
        let answer = self
            .conn
            .tool_call(
                &route,
                broca_fetch_plan::CATALOG_OP,
                &args,
                None,
                OPTIONAL_TOOL_BUDGET,
                args.get("preset").and_then(serde_json::Value::as_str),
            )
            .await
            .map_err(conn_to_plane)?;
        let (catalog, declarations) =
            decode_transport_catalog(provider, answer, keepalive, call_yield)
                .map_err(|_| ToolPlaneError::Unavailable("invalid transport catalog".into()))?;
        Ok(Snapshot {
            catalog,
            declarations,
        })
    }
}
