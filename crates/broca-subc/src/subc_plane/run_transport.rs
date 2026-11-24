use super::*;
use broca_core::tool_plane::{CompatibleTool, FrozenProviderDeclarations, PlanManifest};
use broca_fetch_plan::admit::{decode_transport_catalog, ToolCatalog, OPTIONAL_TOOL_BUDGET};
use std::sync::LazyLock;

// A responsive provider's change reaches every run starting a minute later, with at most
// one catalog read attempt per minute per catalog request ([`Key`]), even during an outage.
// A route failure clears the throttle so a restarted provider can be read immediately.
const TRANSPORT_CACHE_TTL: Duration = Duration::from_secs(60);

/// The most catalog requests whose answers are kept at once. Past it, the request no run has
/// asked about for longest is forgotten, so the cache holds what current sessions use rather
/// than every request ever made. A forgotten request is read again by its next run, as one past
/// the TTL is.
const TRANSPORT_CACHE_CAPACITY: usize = 256;

/// Everything a catalog read depends on: the provider, the request sent to it (the plan's
/// params and preset, as encoded on the wire), and the session and scope its route is bound
/// to. A provider may answer another session, scope or set of params with other definitions,
/// so an answer serves only runs that would send the same request on the same route.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    provider: String,
    request: String,
    session: SessionKey,
    scope: ScopeKey,
}
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
struct Cached {
    slot: Slot,
    /// When a run last looked this request up; the least recent is forgotten first.
    used: Instant,
}
static CACHE: LazyLock<Mutex<HashMap<Key, Cached>>> = LazyLock::new(Mutex::default);
static WARNINGS: LazyLock<Mutex<HashSet<(String, &'static str)>>> = LazyLock::new(Mutex::default);

struct Snapshot {
    catalog: ToolCatalog,
    declarations: FrozenProviderDeclarations,
}

/// Drop every cached catalog of `provider`. The cache is keyed by the provider id the plan froze,
/// while a dead route names the id the module is listed under now, so a renamed module's
/// entries are dropped under either id.
pub(super) fn invalidate(provider: &str) {
    CACHE
        .lock()
        .unwrap()
        .retain(|key, _| !broca_protocol::same_module(&key.provider, provider));
}

/// The cached slot for `key`, if any, marked as just used.
fn cached(key: &Key) -> Option<Slot> {
    let mut cache = CACHE.lock().unwrap();
    let cached = cache.get_mut(key)?;
    cached.used = Instant::now();
    Some(Arc::clone(&cached.slot))
}

/// The slot for `key`, made if missing, marked as just used. Making one past
/// [`TRANSPORT_CACHE_CAPACITY`] forgets the least recently used slots first. A slot some run
/// still holds (it is reading or refreshing it) is never forgotten, so a lookup in progress
/// keeps seeing the slot it found.
fn slot(key: Key) -> Slot {
    let mut cache = CACHE.lock().unwrap();
    let now = Instant::now();
    if let Some(cached) = cache.get_mut(&key) {
        cached.used = now;
        return Arc::clone(&cached.slot);
    }
    while cache.len() >= TRANSPORT_CACHE_CAPACITY {
        let coldest = cache
            .iter()
            .filter(|(_, cached)| Arc::strong_count(&cached.slot) == 1)
            .min_by_key(|(_, cached)| cached.used)
            .map(|(key, _)| key.clone());
        let Some(coldest) = coldest else {
            break;
        };
        cache.remove(&coldest);
    }
    let slot = Slot::default();
    cache.insert(
        key,
        Cached {
            slot: Arc::clone(&slot),
            used: now,
        },
    );
    slot
}

/// The `tool.catalog` request a run of the plan `plan` sends `provider`: the params of the
/// provider's plan item, and its preset.
fn catalog_request(
    plan: &serde_json::Value,
    dispatch: &ToolManifest,
    provider: &str,
) -> cortexkit_role_tool_provider::catalog::CatalogRequest {
    let item = plan["tool_items"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|item| item["provider"].as_str() == Some(provider));
    let params = item
        .and_then(|item| item["params"].as_object())
        .cloned()
        .unwrap_or_default();
    let mut request = cortexkit_role_tool_provider::catalog::CatalogRequest::new(params);
    if let Some(preset) = dispatch.provider_preset(provider) {
        request = request.with_preset(preset.to_owned());
    }
    request
}

fn cache_key(
    provider: &str,
    request: &cortexkit_role_tool_provider::catalog::CatalogRequest,
    bind: &BindIdentity,
    scope: Option<&SessionScope>,
) -> Option<Key> {
    Some(Key {
        provider: provider.to_owned(),
        request: serde_json::to_string(request).ok()?,
        session: SubcToolPlane::session_key(bind),
        scope: scope_key(scope),
    })
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

/// `manifest`'s versioned tools whose provider's newest catalog read serves the same major
/// under another definition ([`broca_core::tool_plane::ToolPlane::compatible_tools`]). Reads only
/// what [`SubcToolPlane::refresh_run_transport`] cached for the request this run of `bind` under
/// `scope` sends each provider, never another session's or another request's answer.
pub(super) fn compatible_tools(
    bind: &BindIdentity,
    scope: Option<&SessionScope>,
    manifest: &PlanManifest,
) -> Vec<CompatibleTool> {
    let Ok(dispatch) =
        ToolManifest::from_shared_tools(Arc::clone(&manifest.tools)).with_plan_manifest(manifest)
    else {
        return Vec::new();
    };
    let Ok(plan) = serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()) else {
        return Vec::new();
    };
    manifest
        .tools
        .iter()
        .filter_map(|tool| {
            let provider = tool.module.as_deref()?;
            let frozen = dispatch
                .provider_declarations(provider)?
                .schema_pins
                .as_ref()?
                .get(&tool.name)?;
            let major = frozen.major?;
            let request = catalog_request(&plan, &dispatch, provider);
            let slot = cached(&cache_key(provider, &request, bind, scope)?)?;
            let current = slot.state.lock().unwrap().latest.clone()?;
            let pin = current.catalog.schema_pins.get(&tool.name)?;
            if pin.major != Some(major) {
                return None;
            }
            let now = current
                .catalog
                .tools
                .iter()
                .find(|now| now.name == tool.name)?;
            let unchanged = now.description == tool.description
                && now.input_schema == tool.input_schema
                && pin.schema_digest == frozen.schema_digest;
            (!unchanged).then(|| CompatibleTool {
                provider: provider.to_owned(),
                tool: now.clone(),
                pin: pin.clone(),
            })
        })
        .collect()
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
            let request = catalog_request(&plan, &dispatch, provider);
            let Some(key) = cache_key(provider, &request, bind, scope) else {
                continue;
            };
            let slot = slot(key);
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
        // A daemon without `scopes/v1` would drop the selector and bind the route unscoped,
        // so a scoped session's catalog is read only where the scope is attested.
        let selector = match scope {
            Some(scope) => Some(
                self.scoped_selector(scope)
                    .await
                    .map_err(RouteEnsureError::into_plane)?,
            ),
            None => None,
        };
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

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn key(n: usize) -> Key {
        Key {
            provider: "transport-capacity".into(),
            request: n.to_string(),
            session: SubcToolPlane::session_key(&BindIdentity::new("/project", "test", "capacity")),
            scope: None,
        }
    }

    /// The cache keeps at most [`TRANSPORT_CACHE_CAPACITY`] requests. Past it, the least recently
    /// used request no run holds is forgotten, so memory follows the requests current runs make,
    /// not every request ever made; one looked up again, or held by a run, stays.
    #[tokio::test(start_paused = true)]
    async fn the_transport_cache_forgets_its_least_recently_used_request_past_its_capacity() {
        let held = slot(key(0));
        for n in 1..TRANSPORT_CACHE_CAPACITY {
            tokio::time::advance(Duration::from_millis(1)).await;
            drop(slot(key(n)));
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(cached(&key(1)).is_some());
        tokio::time::advance(Duration::from_millis(1)).await;
        drop(slot(key(TRANSPORT_CACHE_CAPACITY)));
        let cache = CACHE.lock().unwrap();
        assert!(cache.len() <= TRANSPORT_CACHE_CAPACITY, "{}", cache.len());
        assert!(
            Arc::ptr_eq(&cache[&key(0)].slot, &held),
            "a slot a run holds is never forgotten"
        );
        assert!(
            cache.contains_key(&key(1)),
            "a request looked up again stays"
        );
        assert!(
            !cache.contains_key(&key(2)),
            "the least recently used request goes"
        );
        assert!(cache.contains_key(&key(TRANSPORT_CACHE_CAPACITY)));
    }
}
