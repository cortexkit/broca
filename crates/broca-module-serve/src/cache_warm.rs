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

/// Charge failed warm attempts only for token counts the provider actually reported.
/// Missing usage is never priced using the successful cache-refresh estimate, and an unsent
/// attempt costs no warm budget.
/// Subscription cache writes count as fresh input, matching the subscription cache-bust quote;
/// neither the five-minute nor one-hour metered write multiplier applies to those budget units.
pub fn failed_warm_cost(
    config: &broca_wal::RunConfig,
    reservation: &broca_wal::record::AttemptReservation,
    envelope: &broca_wal::record::AttemptEnvelope,
    ttl_ms: Option<u64>,
) -> Result<f64, String> {
    if !broca_core::cache_warm::warm_budget_needs_pricing(envelope) {
        return Ok(0.0);
    }
    let catalog =
        broca_catalog::runtime::catalog().map_err(|e| format!("pricing_unavailable: {e}"))?;
    failed_warm_cost_with_catalog(&catalog, config, reservation, envelope, ttl_ms)
}

fn failed_warm_cost_with_catalog(
    catalog: &broca_catalog::runtime::CatalogSnapshot,
    config: &broca_wal::RunConfig,
    reservation: &broca_wal::record::AttemptReservation,
    envelope: &broca_wal::record::AttemptEnvelope,
    ttl_ms: Option<u64>,
) -> Result<f64, String> {
    if !broca_core::cache_warm::warm_budget_needs_pricing(envelope) {
        return Ok(0.0);
    }
    let schedule = catalog
        .warm_cost_schedule(&reservation.provider, &reservation.model_sent)
        .ok_or_else(|| {
            format!(
                "pricing_unavailable: no schedule for {}/{}",
                reservation.provider, reservation.model_sent
            )
        })?;
    let (method, _) = frozen_credential(config).ok_or("pricing_unavailable: auth_method")?;
    failed_cost_from_schedule(
        &schedule,
        envelope,
        ttl_ms,
        matches!(
            method,
            crate::auth_routing::AuthMethod::Oauth | crate::auth_routing::AuthMethod::Chatgpt
        ),
    )
}

fn failed_cost_from_schedule(
    schedule: &broca_provider::spec::CostSchedule,
    envelope: &broca_wal::record::AttemptEnvelope,
    ttl_ms: Option<u64>,
    subscription: bool,
) -> Result<f64, String> {
    if !broca_core::cache_warm::warm_budget_needs_pricing(envelope) {
        return Ok(0.0);
    }
    let Some(usage) = envelope
        .usage
        .filter(|_| envelope.sent != broca_wal::record::AttemptSent::No)
    else {
        return Ok(0.0);
    };
    let one_hour = envelope.cache_write_1h_tokens.unwrap_or(0);
    let five_minute = envelope.cache_write_5m_tokens.unwrap_or(0);
    let writes = usage.cache_write_tokens;
    if writes.is_some_and(|writes| {
        one_hour > writes
            || envelope
                .cache_write_5m_tokens
                .is_some_and(|five| five > writes.saturating_sub(one_hour))
            || envelope
                .cache_write_5m_tokens
                .zip(envelope.cache_write_1h_tokens)
                .is_some_and(|(five, hour)| five.checked_add(hour) != Some(writes))
    }) {
        return Err("pricing_unavailable: inconsistent cache-write split".into());
    }
    let known_writes = five_minute.saturating_add(one_hour);
    let context_writes = writes.unwrap_or(known_writes);
    let unsplit_writes = writes.map_or(0, |n| n - known_writes);
    let total = usage
        .input_tokens
        .unwrap_or(0)
        .saturating_add(usage.cached_input_tokens.unwrap_or(0))
        .saturating_add(context_writes);
    let tier = schedule
        .tiers
        .iter()
        // Missing fresh input leaves occupancy unknown, so use the highest context bracket.
        .filter(|t| usage.input_tokens.is_none() || total >= t.min_context)
        .max_by_key(|t| t.min_context);
    let (input, read, write, output) = tier
        .map(|t| (t.input, t.cache_read, t.cache_write, t.output))
        .unwrap_or((
            schedule.input,
            schedule.cache_read,
            schedule.cache_write,
            schedule.output,
        ));
    let rate = |value: Option<f64>| {
        value
            .filter(|r| r.is_finite() && *r >= 0.0)
            .ok_or_else(|| "pricing_unavailable: reported usage rate".to_owned())
    };
    let contribution = |tokens: Option<u64>, price: Option<f64>| -> Result<f64, String> {
        match tokens {
            Some(n) if n > 0 => Ok(n as f64 * rate(price)?),
            _ => Ok(0.0),
        }
    };
    let write_cost = if subscription {
        contribution(Some(context_writes), input)?
    } else {
        let known = contribution(Some(five_minute), write)?
            + if one_hour > 0 {
                one_hour as f64 * rate(input)? * 2.0
            } else {
                0.0
            };
        let unsplit_rate = if unsplit_writes == 0 {
            0.0
        } else {
            // Reported splits are authoritative; an aggregate remainder needs the captured
            // marker TTL. Unknown TTL must not silently get the cheaper write tariff.
            match ttl_ms {
                Some(3_600_000) => rate(input)? * 2.0,
                Some(300_000) => rate(write)?,
                _ => rate(write)?.max(rate(input)? * 2.0),
            }
        };
        known + unsplit_writes as f64 * unsplit_rate
    };
    let cost = contribution(usage.input_tokens, input)?
        + contribution(usage.cached_input_tokens, read)?
        + contribution(usage.output_tokens, output)?
        + write_cost;
    let scale = if subscription {
        let input = rate(input)?;
        if input == 0.0 {
            return Err("pricing_unavailable: reported usage ratio".into());
        }
        input
    } else {
        1.0
    };
    let cost = cost / scale;
    if !cost.is_finite() {
        return Err("pricing_unavailable: reported usage ratio".into());
    }
    Ok(cost)
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

    fn reservation(model: &str) -> broca_wal::record::AttemptReservation {
        serde_json::from_value(serde_json::json!({
            "run_id":"r", "step_id":1, "attempt":0, "send_index":1, "warm":true,
            "reserved_at_ms":100, "provider":"anthropic", "endpoint_host":"example.com", "model_sent":model,
            "credential":{}, "run":{}, "session":{"project_root":"/p", "harness":"runner", "session":"s", "session_ref":"ref"},
            "request_body_bytes":100, "compressed":false
        })).unwrap()
    }

    fn metered_config() -> broca_wal::RunConfig {
        let mut config = config();
        config.model = broca_protocol::request::ModelHandle::new("anthropic", "source");
        config.auth_selection.as_mut().unwrap().credential_id = "apikey:anthropic:test".into();
        config
    }

    fn pricing_catalog() -> broca_catalog::runtime::CatalogSnapshot {
        let rate = |units: u64| serde_json::json!({"currency":"USD", "exponent":9, "state":"priced", "units":units});
        let row = |input, read, write, output| {
            BTreeMap::from([
                ("existence".into(), serde_json::json!("present")),
                (
                    "wire.npm".into(),
                    serde_json::json!({"value":"@ai-sdk/anthropic", "from":"provider"}),
                ),
                (
                    "wire.api".into(),
                    serde_json::json!({"value":"https://api.anthropic.com", "from":"provider"}),
                ),
                ("limit.context".into(), serde_json::json!(200_000)),
                ("limit.output".into(), serde_json::json!(64_000)),
                ("rate.input".into(), rate(input)),
                ("rate.cache_read".into(), rate(read)),
                ("rate.cache_write".into(), rate(write)),
                ("rate.output".into(), rate(output)),
            ])
        };
        let cache = broca_catalog::CatalogCache::empty();
        cache
            .install_for_test(
                broca_catalog::FusiformCatalogResponse {
                    source: "models.dev".into(),
                    resolved_at_ms: 1,
                    catalog_version: 1,
                    models: BTreeMap::from([
                        (
                            "anthropic/source".into(),
                            row(10_000_000_000, 500_000_000, 12_500_000_000, 80_000_000_000),
                        ),
                        (
                            "anthropic/fallback".into(),
                            row(
                                20_000_000_000,
                                2_000_000_000,
                                30_000_000_000,
                                160_000_000_000,
                            ),
                        ),
                    ]),
                    withheld: vec![],
                    uncertain: vec![],
                    wire_corrections: vec![],
                },
                broca_catalog::CatalogSource::Fusiform,
            )
            .unwrap();
        let snapshot = cache.snapshot().unwrap();
        assert_eq!(
            snapshot
                .warm_cost_schedule("anthropic", "source")
                .unwrap()
                .input,
            Some(10.0)
        );
        snapshot
    }

    #[test]
    fn failed_warm_adapter_uses_actual_model_and_one_hour_tariff() {
        use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome};
        let catalog = pricing_catalog();
        let config = metered_config();
        let mut e = AttemptEnvelope {
            sent: AttemptSent::Yes,
            outcome: BillingAttemptOutcome::HttpError,
            usage: Some(broca_protocol::Usage {
                input_tokens: Some(0),
                cache_write_tokens: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            failed_warm_cost_with_catalog(
                &catalog,
                &config,
                &reservation("source"),
                &e,
                Some(3_600_000)
            )
            .unwrap(),
            40.0
        );
        assert_eq!(
            failed_warm_cost_with_catalog(
                &catalog,
                &config,
                &reservation("source"),
                &e,
                Some(300_000)
            )
            .unwrap(),
            25.0
        );
        assert_eq!(
            failed_warm_cost_with_catalog(&catalog, &config, &reservation("source"), &e, None)
                .unwrap(),
            40.0
        );
        let mut subscription = config.clone();
        subscription.auth_selection.as_mut().unwrap().credential_id = "oauth:anthropic:test".into();
        assert_eq!(
            failed_warm_cost_with_catalog(
                &catalog,
                &subscription,
                &reservation("source"),
                &e,
                Some(3_600_000)
            )
            .unwrap(),
            2.0,
            "subscription writes are fresh-input equivalents, not metered write multipliers"
        );
        e.usage = Some(broca_protocol::Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(4),
            cache_write_tokens: Some(2),
            output_tokens: Some(1),
            ..Default::default()
        });
        assert_eq!(
            failed_warm_cost_with_catalog(
                &catalog,
                &config,
                &reservation("fallback"),
                &e,
                Some(3_600_000)
            )
            .unwrap(),
            308.0
        );
        let error = failed_warm_cost_with_catalog(
            &catalog,
            &config,
            &reservation("missing"),
            &e,
            Some(3_600_000),
        )
        .unwrap_err();
        assert!(
            error.contains("no schedule for anthropic/missing"),
            "{error}"
        );
    }

    #[test]
    fn failed_warm_unknown_tariff_and_missing_fresh_counts_choose_higher_prices() {
        use broca_wal::record::{AttemptEnvelope, AttemptSent};
        let schedule = broca_provider::spec::CostSchedule {
            input: Some(10.0),
            cache_write: Some(30.0),
            cache_read: Some(0.5),
            tiers: vec![broca_provider::spec::CostTier {
                min_context: 100,
                input: Some(40.0),
                cache_write: Some(60.0),
                cache_read: Some(4.0),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut e = AttemptEnvelope {
            sent: AttemptSent::Yes,
            usage: Some(broca_protocol::Usage {
                input_tokens: Some(0),
                cache_write_tokens: Some(2),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            failed_cost_from_schedule(&schedule, &e, None, false).unwrap(),
            60.0,
            "unknown TTL uses the higher tariff even when the five-minute price is higher"
        );
        e.usage = Some(broca_protocol::Usage {
            cached_input_tokens: Some(1),
            ..Default::default()
        });
        assert_eq!(
            failed_cost_from_schedule(&schedule, &e, Some(3_600_000), false).unwrap(),
            4.0,
            "missing fresh occupancy must not select the base tier"
        );
        e.usage.as_mut().unwrap().input_tokens = Some(0);
        assert_eq!(
            failed_cost_from_schedule(&schedule, &e, Some(3_600_000), false).unwrap(),
            0.5
        );
    }

    #[cfg(feature = "test-clock")]
    struct PricedWarm {
        catalog: broca_catalog::runtime::CatalogSnapshot,
        model: &'static str,
        ttl_ms: Option<u64>,
        usage: broca_protocol::Usage,
        resend: bool,
    }
    #[cfg(feature = "test-clock")]
    #[async_trait::async_trait]
    impl broca_module::RunExecutor for PricedWarm {
        async fn execute(
            &self,
            _: broca_module::RunJob,
            _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
        ) -> Result<broca_core::RunSummary, broca_core::RunError> {
            unreachable!()
        }
        fn warm_costs(
            &self,
            config: &broca_wal::RunConfig,
            prefix: u64,
            ttl: u64,
        ) -> Result<broca_core::keep_warm::WarmCosts, String> {
            let schedule = self
                .catalog
                .warm_cost_schedule(&config.model.provider_module_id, &config.model.model_id)
                .unwrap();
            broca_catalog::keep_warm::costs_from_schedule(
                &schedule,
                prefix,
                broca_provider::spec::WireFamilyId::AnthropicMessages,
                Some(ttl),
                false,
            )
            .map_err(|e| e.to_string())
        }
        fn failed_warm_cost(
            &self,
            config: &broca_wal::RunConfig,
            reservation: &broca_wal::record::AttemptReservation,
            envelope: &broca_wal::record::AttemptEnvelope,
            ttl_ms: Option<u64>,
        ) -> Result<f64, String> {
            failed_warm_cost_with_catalog(&self.catalog, config, reservation, envelope, ttl_ms)
        }
        async fn warm(
            &self,
            job: broca_module::WarmJob,
        ) -> Result<broca_core::WarmSent, broca_module::WarmError> {
            use broca_wal::record::{
                AttemptEnvelope, AttemptSent, BillingAttemptOutcome as Outcome,
            };
            let hook = job.attempt_hook.unwrap();
            let mut result = None;
            for index in 0..=u32::from(self.resend) {
                let id = hook
                    .before_send(
                        broca_core::run::PhysicalSend {
                            attempt: index,
                            send_index: index + 1,
                            endpoint_host: "example.com".into(),
                            model_sent: self.model.into(),
                            request_body_bytes: 100,
                            compressed: self.resend && index == 0,
                            account_ref: None,
                            speed_requested: None,
                            warm: Some(broca_core::cache_warm::WarmSendPolicy {
                                provider: Some("anthropic".into()),
                                ttl_ms: self.ttl_ms,
                            }),
                        },
                        Default::default(),
                    )
                    .await
                    .map_err(|e| broca_module::WarmError::Provider(Box::new(e)))?;
                let success = index == 1;
                let sent_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let sent = success.then(|| broca_core::WarmSent {
                    warm_body_sha256: "warm".into(),
                    sent_at_ms,
                    usage: broca_protocol::Usage {
                        input_tokens: Some(0),
                        cached_input_tokens: Some(1000),
                        output_tokens: Some(0),
                        ..Default::default()
                    },
                    served_account_ref: None,
                    ratelimit_headers: vec![],
                    http_status: 200,
                });
                hook.warm_finished(
                    id,
                    AttemptEnvelope {
                        sent: AttemptSent::Yes,
                        sent_at_ms: Some(sent_at_ms),
                        finished_at_ms: Some(sent_at_ms + 1),
                        http_status: Some(if success { 200 } else { 415 }),
                        outcome: if success {
                            Outcome::Complete
                        } else if self.resend {
                            Outcome::CompressionRefused
                        } else {
                            Outcome::HttpError
                        },
                        usage: Some(sent.as_ref().map_or(self.usage, |s| s.usage)),
                        ..Default::default()
                    },
                    sent.clone(),
                    "warm".into(),
                )
                .await
                .map_err(|e| broca_module::WarmError::Provider(Box::new(e)))?;
                result = sent;
            }
            result.ok_or_else(|| {
                broca_module::WarmError::Provider(Box::new(
                    serde_json::from_value(
                        serde_json::json!({"class":"transient","message":"warm refused"}),
                    )
                    .unwrap(),
                ))
            })
        }
    }

    #[cfg(feature = "test-clock")]
    #[derive(Clone)]
    struct PriceLog(Arc<Mutex<Vec<u8>>>);
    #[cfg(feature = "test-clock")]
    impl std::io::Write for PriceLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(feature = "test-clock")]
    async fn check_priced_live_and_restart(
        model: &'static str,
        ttl_ms: Option<u64>,
        usage: broca_protocol::Usage,
        resend: bool,
        expected: f64,
    ) {
        use broca_core::{
            keep_warm_wait::{WarmTick, WarmWait},
            WalStore,
        };
        use tracing::instrument::WithSubscriber;
        let root = broca_test_support::scratch_root().join(format!(
            "warm-tariff-{}",
            broca_core::attempt_id::mint_attempt_id().unwrap().as_str()
        ));
        let store = Arc::new(broca_core::FileWalStore::with_config(
            root.join("wal"),
            broca_wal::WalConfig::OVERHAUL,
        ));
        let key = broca_protocol::BindIdentity::new("/p", "runner", "tariff");
        let tenure = broca_core::lease::WalLeaseDomain::new(root.join("leases"))
            .acquire(&key)
            .unwrap();
        let writer = Arc::new(
            broca_core::WalHandle::open_and_spawn_owned(store.clone(), key.clone(), tenure, None)
                .await
                .unwrap(),
        );
        let config = metered_config();
        let driver = broca_module::automatic_warmer_for_test(
            writer.clone(),
            Arc::new(PricedWarm {
                catalog: pricing_catalog(),
                model,
                ttl_ms,
                usage,
                resend,
            }),
            key.clone(),
            config.clone(),
            3_600_000,
        );
        let run = broca_protocol::RunId("r".into());
        let logs = Arc::new(Mutex::new(Vec::new()));
        let sink = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || PriceLog(sink.clone()))
            .finish();
        let tick = driver.warm(&config, &run).with_subscriber(subscriber).await;
        let live = match tick {
            WarmTick::SentWithCost { cost } if resend => cost,
            WarmTick::StopWithCost { cost, .. } if !resend => cost,
            _ => panic!("unexpected live warm outcome"),
        };
        assert_eq!(live, expected, "actual model={model}, ttl={ttl_ms:?}");
        let unsupported = std::sync::atomic::AtomicBool::new(false);
        let missed = std::sync::atomic::AtomicBool::new(false);
        let restarted_driver = broca_module::automatic_warmer_for_test(
            writer.clone(),
            Arc::new(PricedWarm {
                catalog: pricing_catalog(),
                model,
                ttl_ms,
                usage,
                resend,
            }),
            key.clone(),
            config.clone(),
            3_600_000,
        );
        let wait = WarmWait {
            driver: &restarted_driver,
            config: &config,
            run_id: &run,
            cancel: None,
            unsupported_logged: &unsupported,
            warm_missed: &missed,
        };
        let frames = store.read_records(&key).unwrap();
        let restarted = broca_core::keep_warm_wait::batch_spend(
            Some(&wait),
            broca_protocol::StepId(1),
            &frames,
        );
        assert_eq!(restarted, expected);
        assert_eq!(restarted, live);
        let successes = frames.iter().filter(|f| matches!(&f.record, broca_wal::WalRecord::CacheWarmed { envelope: Some(e), .. } if e.outcome == broca_wal::record::BillingAttemptOutcome::Complete)).count();
        assert_eq!(
            successes,
            usize::from(resend),
            "the compression pair includes exactly one successful estimate"
        );
        if expected.is_infinite() {
            assert!(!driver.costs(&config).unwrap().allows_next(restarted));
            let text = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
            assert!(
                text.contains("pricing_unavailable")
                    && text.contains("no schedule for anthropic/missing"),
                "{text}"
            );
        }
        drop(restarted_driver);
        drop(driver);
        Arc::try_unwrap(writer)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(feature = "test-clock")]
    fn aggregate_write_usage() -> broca_protocol::Usage {
        broca_protocol::Usage {
            input_tokens: Some(0),
            cache_write_tokens: Some(2),
            ..Default::default()
        }
    }
    #[cfg(feature = "test-clock")]
    fn fallback_refusal_usage() -> broca_protocol::Usage {
        broca_protocol::Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(4),
            cache_write_tokens: Some(2),
            output_tokens: Some(1),
            ..Default::default()
        }
    }

    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn metered_hour_refusal_and_unknown_tariff_spend_match_after_restart() {
        check_priced_live_and_restart(
            "source",
            Some(3_600_000),
            aggregate_write_usage(),
            false,
            40.0,
        )
        .await;
        check_priced_live_and_restart(
            "source",
            Some(300_000),
            aggregate_write_usage(),
            false,
            25.0,
        )
        .await;
        check_priced_live_and_restart("source", None, aggregate_write_usage(), false, 40.0).await;
    }
    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn fallback_refusal_uses_actual_schedule_live_and_after_restart() {
        // The actual fallback row costs 3*20 + 4*2 + 2*(2*20) + 160 = 308.
        check_priced_live_and_restart(
            "fallback",
            Some(3_600_000),
            fallback_refusal_usage(),
            false,
            308.0,
        )
        .await;
    }
    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn compression_pair_has_actual_failed_charge_and_one_success_estimate() {
        // The existing successful source-model quote is 1000*0.5 = 500; one pair costs 308+500.
        check_priced_live_and_restart(
            "fallback",
            Some(3_600_000),
            fallback_refusal_usage(),
            true,
            808.0,
        )
        .await;
    }
    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn unknown_actual_schedule_exhausts_budget_and_logs_reason() {
        check_priced_live_and_restart(
            "missing",
            Some(3_600_000),
            aggregate_write_usage(),
            false,
            f64::INFINITY,
        )
        .await;
    }

    #[test]
    fn failed_warm_prices_only_reported_usage_and_cache_write_splits() {
        use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome};
        let rates = broca_provider::spec::CostSchedule {
            input: Some(10.0),
            output: Some(80.0),
            cache_read: Some(0.5),
            cache_write: Some(12.5),
            ..Default::default()
        };
        let mut e = AttemptEnvelope {
            sent: AttemptSent::Yes,
            outcome: BillingAttemptOutcome::HttpError,
            ..Default::default()
        };
        assert_eq!(
            failed_warm_cost(&config(), &reservation("source"), &e, None).unwrap(),
            0.0,
            "absent usage requires no catalog lookup"
        );
        e.usage = Some(broca_protocol::Usage::default());
        assert_eq!(
            failed_warm_cost(&config(), &reservation("source"), &e, None).unwrap(),
            0.0,
            "an empty usage object reports no chargeable counts"
        );
        assert_eq!(
            failed_cost_from_schedule(&rates, &e, None, false).unwrap(),
            0.0
        );
        e.usage = Some(broca_protocol::Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(4),
            cache_write_tokens: Some(5),
            output_tokens: Some(1),
            ..Default::default()
        });
        e.cache_write_5m_tokens = Some(3);
        e.cache_write_1h_tokens = Some(2);
        assert_eq!(
            failed_cost_from_schedule(&rates, &e, None, false).unwrap(),
            189.5
        );
        assert_eq!(
            failed_cost_from_schedule(&rates, &e, None, true).unwrap(),
            16.2
        );
        e.sent = AttemptSent::No;
        assert_eq!(
            failed_cost_from_schedule(&rates, &e, None, false).unwrap(),
            0.0
        );
        e.sent = AttemptSent::Yes;
        e.usage = Some(broca_protocol::Usage {
            cached_input_tokens: Some(4),
            ..Default::default()
        });
        e.cache_write_5m_tokens = None;
        e.cache_write_1h_tokens = None;
        let read_only = broca_provider::spec::CostSchedule {
            cache_read: Some(0.5),
            ..Default::default()
        };
        assert_eq!(
            failed_cost_from_schedule(&read_only, &e, None, false).unwrap(),
            2.0,
            "missing, unreported counts require no rate and add no charge"
        );
        let mut tiered = rates;
        tiered.tiers.push(broca_provider::spec::CostTier {
            min_context: 4,
            input: Some(20.0),
            output: None,
            cache_read: Some(2.0),
            cache_write: None,
        });
        assert_eq!(
            failed_cost_from_schedule(&tiered, &e, None, false).unwrap(),
            8.0
        );
        e.usage = Some(broca_protocol::Usage::default());
        e.cache_write_1h_tokens = Some(2);
        assert_eq!(
            failed_cost_from_schedule(&tiered, &e, None, false).unwrap(),
            80.0,
            "a reported split remains priceable without an aggregate write count, at the higher tier when fresh input is unknown"
        );
    }
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
