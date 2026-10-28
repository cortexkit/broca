//! The serving layer's half of `session.warm`: reading what a frozen config declares about its
//! prompt cache, and sending a warm through a provider built for that config.

use broca_module::{CacheDeclaration, WarmError};
use broca_provider::wire::FrozenRenderConfig;

/// Reads a session's cache declaration from its frozen render config, with the provider
/// families' own rules.
pub struct ProviderCacheDeclarer;

impl broca_module::CacheDeclarer for ProviderCacheDeclarer {
    fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration> {
        let frozen = frozen_render_config(config).ok()?;
        let declared = broca_provider::cache_warm::cache_declaration(
            &config.model.provider_module_id,
            &frozen,
            &config.tool_choice,
            &config.response_format,
        );
        Some(CacheDeclaration {
            ttl_ms: declared.ttl_ms,
            ttl_options_ms: declared.ttl_options_ms,
            read_refreshes_ttl: declared.read_refreshes_ttl,
            warm: declared.warm,
            basis: declared.basis.map(str::to_owned),
        })
    }
}

fn frozen_render_config(config: &broca_wal::RunConfig) -> Result<FrozenRenderConfig, String> {
    serde_json::from_value(config.frozen_render_config.clone())
        .map_err(|error| format!("the session's frozen render config is unreadable: {error}"))
}

/// The frozen render config of a session that can be warmed, or why it cannot.
pub fn warmable_config(config: &broca_wal::RunConfig) -> Result<FrozenRenderConfig, WarmError> {
    let frozen = frozen_render_config(config).map_err(WarmError::Unsupported)?;
    broca_provider::cache_warm::cache_declaration(
        &config.model.provider_module_id,
        &frozen,
        &config.tool_choice,
        &config.response_format,
    )
    .warm
    .map_err(WarmError::Unsupported)?;
    Ok(frozen)
}

/// OpenAI guarantees thirty minutes of prompt retention for these Responses backends, but
/// does not document whether a cached read restarts that clock. Schedule the next refresh
/// twenty-five minutes after sending rather than relying on a read to extend retention.
pub fn warm_after_send_ms(config: &broca_wal::RunConfig) -> Option<u64> {
    use broca_catalog::live::WireFamilyId;
    let family = frozen_render_config(config).ok()?.family_id;
    matches!(
        family,
        WireFamilyId::OpenAiResponsesChatgpt | WireFamilyId::OpenAiResponses
    )
    .then_some(25 * 60 * 1000)
}

pub fn automatic_warm_prices(
    config: &broca_wal::RunConfig,
    prefix_tokens: u64,
    ttl_ms: u64,
) -> Result<broca_core::keep_warm::WarmCosts, String> {
    use crate::auth_routing::AuthMethod;
    let frozen =
        frozen_render_config(config).map_err(|error| format!("pricing_unavailable: {error}"))?;
    let (method, _) = frozen_credential(config).ok_or("pricing_unavailable: auth_method")?;
    let subscription = matches!(method, AuthMethod::Oauth | AuthMethod::Chatgpt);
    broca_catalog::keep_warm::warm_prices(
        &config.model.provider_module_id,
        &config.model.model_id,
        prefix_tokens,
        frozen.family_id,
        Some(ttl_ms),
        subscription,
    )
    .map_err(|error| error.to_string())
}

pub fn responses_warm_cost(
    config: &broca_wal::RunConfig,
    usage: broca_protocol::Usage,
) -> Result<f64, String> {
    use crate::auth_routing::AuthMethod;
    let (method, _) = frozen_credential(config).ok_or("pricing_unavailable: auth_method")?;
    broca_catalog::keep_warm::responses_warm_cost(
        &config.model.provider_module_id,
        &config.model.model_id,
        (
            usage.input_tokens.ok_or("pricing_unavailable: usage")?,
            usage
                .cached_input_tokens
                .ok_or("pricing_unavailable: usage")?,
            usage.output_tokens.ok_or("pricing_unavailable: usage")?,
        ),
        matches!(method, AuthMethod::Oauth | AuthMethod::Chatgpt),
    )
    .map_err(|error| error.to_string())
}

/// The method and provider of the run's frozen credential selection, read as the selection
/// froze them (see [`crate::auth_routing::frozen_selection_credential`]).
fn frozen_credential(
    config: &broca_wal::RunConfig,
) -> Option<(crate::auth_routing::AuthMethod, String)> {
    config.auth_selection.as_ref().and_then(|selection| {
        crate::auth_routing::frozen_selection_credential(
            selection,
            config.model.provider_module_id.as_str(),
        )
    })
}

/// Query quota using the frozen selection's provider (parsed from a broca key, or the run's
/// provider for a selection frozen with its method) and the vault account UUID in frozen
/// `auth_selection.account_ref`. The prefrontal routing module,
/// which chooses accounts and reports quota, needs exactly these identities, not a catalog
/// alias or the credential-id suffix.
pub async fn automatic_warm_quota_for_config(
    client: &crate::account_routing::RoutingClient,
    config: &broca_wal::RunConfig,
    headers: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    let frozen = frozen_render_config(config).map_err(|_| "reading_missing")?;
    let selection = config.auth_selection.as_ref().ok_or("account_unknown")?;
    let (method, provider) = frozen_credential(config).ok_or("account_unknown")?;
    match crate::keep_warm_quota::automatic_warm_quota(
        client,
        &provider,
        selection.account_ref.as_deref(),
        method,
        frozen.family_id,
        headers,
    )
    .await
    {
        crate::keep_warm_quota::QuotaDecision::Allow => Ok(()),
        crate::keep_warm_quota::QuotaDecision::Stop { code } => Err(code),
    }
}

#[cfg(test)]
mod automatic_tests {
    use super::*;
    use crate::account_routing::{
        AccountSelectionConfig, RoutingClient, RoutingReply, RoutingTransport,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    fn config() -> broca_wal::RunConfig {
        let record: broca_wal::WalRecord = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/run_started.keep_warm.json"
        ))
        .unwrap();
        let broca_wal::WalRecord::RunStarted { config, .. } = record else {
            unreachable!()
        };
        let mut config = *config;
        config.frozen_render_config = serde_json::to_value(
            broca_provider::families::anthropic_frozen_config("claude-opus-4-8"),
        )
        .unwrap();
        config.auth_selection = Some(broca_wal::AuthSelection {
            auth_method: None,
            credential_id: "oauth:anthropic:credential-suffix-is-not-account".into(),
            account_ref: Some("00000000-0000-4000-8000-00000000000a".into()),
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        config
    }
    struct Stub {
        reply: RoutingReply,
        requests: Mutex<Vec<serde_json::Value>>,
    }
    #[async_trait::async_trait]
    impl RoutingTransport for Stub {
        async fn request(&self, body: Vec<u8>) -> RoutingReply {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            self.reply.clone()
        }
    }
    #[tokio::test]
    async fn automatic_quota_adapter_uses_frozen_vault_identity_and_latest_headers() {
        for (fixture, expected) in [
            (
                include_str!("../tests/fixtures/route-account-status-v1/warm_ok.json"),
                Ok(()),
            ),
            (
                include_str!("../tests/fixtures/route-account-status-v1/within_reserve.json"),
                Err("within_reserve".into()),
            ),
            (
                include_str!("../tests/fixtures/route-account-status-v1/reading_stale.json"),
                Err("reading_stale".into()),
            ),
        ] {
            let vector: serde_json::Value = serde_json::from_str(fixture).unwrap();
            let stub = Arc::new(Stub {
                reply: Ok(Ok(serde_json::to_vec(&vector["reply"]).unwrap())),
                requests: Default::default(),
            });
            let client = RoutingClient::new(stub.clone(), AccountSelectionConfig::default());
            assert_eq!(
                automatic_warm_quota_for_config(&client, &config(), &BTreeMap::new()).await,
                expected
            );
            assert_eq!(
                stub.requests.lock().unwrap().as_slice(),
                &[vector["request"].clone()]
            );
        }
        let stub = Arc::new(Stub {
            reply: Ok(Err(br#"{"code":"unknown_method"}"#.to_vec())),
            requests: Default::default(),
        });
        let client = RoutingClient::new(stub.clone(), AccountSelectionConfig::default());
        for (value, expected) in [("0.89", Ok(())), ("0.90", Err("headers_90_percent".into()))] {
            let headers = BTreeMap::from([(
                "anthropic-ratelimit-unified-5h-utilization".into(),
                value.into(),
            )]);
            assert_eq!(
                automatic_warm_quota_for_config(&client, &config(), &headers).await,
                expected
            );
        }
        assert_eq!(client.counters().get("warm_quota_fallback"), 2);
        let mut metered = config();
        metered.auth_selection.as_mut().unwrap().credential_id =
            "apikey:anthropic:credential".into();
        metered.auth_selection.as_mut().unwrap().account_ref = None;
        assert_eq!(
            automatic_warm_quota_for_config(&client, &metered, &BTreeMap::new()).await,
            Ok(())
        );
        assert_eq!(stub.requests.lock().unwrap().len(), 2);
        let mut missing = config();
        missing.auth_selection.as_mut().unwrap().account_ref = None;
        assert_eq!(
            automatic_warm_quota_for_config(&client, &missing, &BTreeMap::new()).await,
            Err("account_unknown".into())
        );
        assert_eq!(stub.requests.lock().unwrap().len(), 2);
    }
    #[test]
    fn automatic_timing_adapter_supplies_codex_cadence_without_enabling_support() {
        let mut config = config();
        assert_eq!(warm_after_send_ms(&config), None);
        for family in [
            broca_provider::spec::WireFamilyId::OpenAiResponsesChatgpt,
            broca_provider::spec::WireFamilyId::OpenAiResponses,
        ] {
            let mut frozen: FrozenRenderConfig =
                serde_json::from_value(config.frozen_render_config.clone()).unwrap();
            frozen.family_id = family;
            config.frozen_render_config = serde_json::to_value(frozen).unwrap();
            assert_eq!(warm_after_send_ms(&config), Some(25 * 60 * 1000));
        }
    }
}
