//! Current in-memory prices for cache maintenance, separate from render and admission freezes.

use broca_core::keep_warm::WarmCosts;
use broca_provider::spec::{CostSchedule, WireFamilyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricingUnavailable {
    pub rate: &'static str,
}

impl std::fmt::Display for PricingUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pricing_unavailable: {}", self.rate)
    }
}

impl std::error::Error for PricingUnavailable {}

/// Read the currently installed snapshot at every decision; no rates are persisted into the
/// WAL. Service and speed tiers are deliberately ignored: only base/context brackets price
/// cache maintenance, so mode rates never fill a missing base rate.
pub fn warm_prices(
    provider: &str,
    model: &str,
    prefix_tokens: u64,
    family: WireFamilyId,
    ttl_ms: Option<u64>,
    subscription: bool,
) -> Result<WarmCosts, PricingUnavailable> {
    let catalog = crate::runtime::catalog().map_err(|_| PricingUnavailable { rate: "catalog" })?;
    let schedule = catalog
        .warm_cost_schedule(provider, model)
        .ok_or(PricingUnavailable { rate: "model" })?;
    costs_from_schedule(&schedule, prefix_tokens, family, ttl_ms, subscription)
}

/// Price a Responses refresh from the provider's disjoint fresh/cached counts and actual
/// output, using the same units and context bracket as the rent-or-buy quote.
pub fn responses_warm_cost(
    provider: &str,
    model: &str,
    counts: (u64, u64, u64),
    subscription: bool,
) -> Result<f64, PricingUnavailable> {
    let catalog = crate::runtime::catalog().map_err(|_| PricingUnavailable { rate: "catalog" })?;
    let schedule = catalog
        .warm_cost_schedule(provider, model)
        .ok_or(PricingUnavailable { rate: "model" })?;
    responses_cost_from_schedule(&schedule, counts, subscription)
}

fn responses_cost_from_schedule(
    schedule: &CostSchedule,
    counts: (u64, u64, u64),
    subscription: bool,
) -> Result<f64, PricingUnavailable> {
    let (fresh, cached, output_tokens) = counts;
    let total = fresh.saturating_add(cached);
    let tier = schedule
        .tiers
        .iter()
        .filter(|tier| total >= tier.min_context)
        .max_by_key(|tier| tier.min_context);
    let (input, read, output) = match tier {
        Some(tier) => (tier.input, tier.cache_read, tier.output),
        None => (schedule.input, schedule.cache_read, schedule.output),
    };
    let required = |rate: Option<f64>, name| {
        rate.filter(|rate| rate.is_finite() && *rate >= 0.0)
            .ok_or(PricingUnavailable { rate: name })
    };
    let input = required(input, "input")?;
    let read = required(read, "cache_read")?;
    let output = required(output, "output")?;
    let scale = if subscription {
        if input == 0.0 {
            return Err(PricingUnavailable {
                rate: "input ratio",
            });
        }
        input
    } else {
        1.0
    };
    let cost =
        (fresh as f64 * input + cached as f64 * read + output_tokens as f64 * output) / scale;
    if cost.is_finite() {
        Ok(cost)
    } else {
        Err(PricingUnavailable { rate: "ratios" })
    }
}

pub fn costs_from_schedule(
    schedule: &CostSchedule,
    prefix_tokens: u64,
    family: WireFamilyId,
    ttl_ms: Option<u64>,
    subscription: bool,
) -> Result<WarmCosts, PricingUnavailable> {
    let tier = schedule
        .tiers
        .iter()
        .filter(|tier| prefix_tokens >= tier.min_context)
        .max_by_key(|tier| tier.min_context);
    let (input, output, read, write) = match tier {
        Some(tier) => (tier.input, tier.output, tier.cache_read, tier.cache_write),
        None => (
            schedule.input,
            schedule.output,
            schedule.cache_read,
            schedule.cache_write,
        ),
    };
    let required = |rate: Option<f64>, name| -> Result<f64, PricingUnavailable> {
        rate.filter(|rate| rate.is_finite() && *rate >= 0.0)
            .ok_or(PricingUnavailable { rate: name })
    };
    let read = required(read, "cache_read")?;
    let (bust, output_tokens) = match family {
        WireFamilyId::AnthropicMessages if !subscription => {
            let ttl = ttl_ms.ok_or(PricingUnavailable { rate: "ttl" })?;
            // The catalog's write price is the five-minute tariff. A one-hour write is billed
            // at twice input instead, so multiplying a missing write rate would be incorrect.
            let bust = if ttl == 3_600_000 {
                required(input, "input")? * 2.0
            } else {
                required(write, "cache_write")?
            };
            (bust, 0)
        }
        WireFamilyId::AnthropicMessages => (required(input, "input")?, 0),
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
            (required(input, "input")?, 16)
        }
        _ => return Err(PricingUnavailable { rate: "family" }),
    };
    let output = if output_tokens == 0 {
        0.0
    } else {
        required(output, "output")?
    };
    // Subscription money is zero, not a zero-price waiver of the gate. Normalize published
    // prices into input-token equivalents; subscriptions buy a fresh read, not a billed write.
    let scale = if subscription {
        let input = required(input, "input")?;
        if input == 0.0 {
            return Err(PricingUnavailable {
                rate: "input ratio",
            });
        }
        input
    } else {
        1.0
    };
    WarmCosts::from_ratios(
        prefix_tokens,
        read / scale,
        bust / scale,
        output / scale,
        output_tokens,
    )
    .ok_or(PricingUnavailable { rate: "ratios" })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_provider::spec::CostTier;

    fn prices() -> CostSchedule {
        CostSchedule {
            input: Some(10.0),
            output: Some(80.0),
            cache_read: Some(0.5),
            cache_write: Some(12.5),
            ..CostSchedule::default()
        }
    }

    #[test]
    fn responses_actual_cost_charges_fresh_cached_and_output_in_selected_bracket() {
        let usage = (164_384, 15_616, 3);
        assert_eq!(
            responses_cost_from_schedule(&prices(), usage, false).unwrap(),
            1_651_888.0
        );
        assert_eq!(
            responses_cost_from_schedule(&prices(), usage, true).unwrap(),
            165_188.8
        );
        let mut schedule = prices();
        schedule.tiers.push(broca_provider::spec::CostTier {
            min_context: 180_000,
            input: Some(20.0),
            cache_read: Some(1.0),
            output: Some(160.0),
            ..Default::default()
        });
        assert_eq!(
            responses_cost_from_schedule(&schedule, usage, false).unwrap(),
            3_303_776.0
        );
    }

    #[test]
    fn warm_pricing_uses_write_tariffs_and_subscription_token_ratios() {
        let schedule = prices();
        let five = costs_from_schedule(
            &schedule,
            1000,
            WireFamilyId::AnthropicMessages,
            Some(300_000),
            false,
        )
        .unwrap();
        assert_eq!(
            five,
            WarmCosts {
                warm: 500.0,
                bust: 12500.0
            }
        );
        let hour = costs_from_schedule(
            &schedule,
            1000,
            WireFamilyId::AnthropicMessages,
            Some(3_600_000),
            false,
        )
        .unwrap();
        assert_eq!(hour.bust, 20000.0);
        for family in [
            WireFamilyId::OpenAiResponses,
            WireFamilyId::OpenAiResponsesChatgpt,
        ] {
            let metered = costs_from_schedule(&schedule, 1000, family, None, false).unwrap();
            assert_eq!(
                metered,
                WarmCosts {
                    warm: 1780.0,
                    bust: 10000.0
                }
            );
            let included = costs_from_schedule(&schedule, 1000, family, None, true).unwrap();
            assert_eq!(
                included,
                WarmCosts {
                    warm: 178.0,
                    bust: 1000.0
                }
            );
        }
        let included = costs_from_schedule(
            &schedule,
            1000,
            WireFamilyId::AnthropicMessages,
            Some(300_000),
            true,
        )
        .unwrap();
        assert_eq!(
            included,
            WarmCosts {
                warm: 50.0,
                bust: 1000.0
            }
        );
    }

    #[test]
    fn warm_pricing_refuses_missing_rates_and_does_not_fill_context_rates_from_base() {
        let mut schedule = prices();
        schedule.output = None;
        assert!(costs_from_schedule(
            &schedule,
            1000,
            WireFamilyId::AnthropicMessages,
            Some(300_000),
            false
        )
        .is_ok());
        assert_eq!(
            costs_from_schedule(
                &schedule,
                1000,
                WireFamilyId::OpenAiResponsesChatgpt,
                None,
                true
            )
            .unwrap_err()
            .rate,
            "output"
        );
        schedule = prices();
        schedule.tiers.push(CostTier {
            min_context: 1001,
            input: Some(20.0),
            cache_read: Some(1.0),
            ..CostTier::default()
        });
        assert!(costs_from_schedule(
            &schedule,
            1000,
            WireFamilyId::AnthropicMessages,
            Some(300_000),
            false
        )
        .is_ok());
        assert_eq!(
            costs_from_schedule(
                &schedule,
                1001,
                WireFamilyId::AnthropicMessages,
                Some(300_000),
                false
            )
            .unwrap_err()
            .rate,
            "cache_write"
        );
        assert_eq!(
            costs_from_schedule(
                &schedule,
                1001,
                WireFamilyId::AnthropicMessages,
                Some(3_600_000),
                false
            )
            .unwrap()
            .bust,
            40040.0
        );
        schedule.cache_read = None;
        assert_eq!(
            costs_from_schedule(
                &schedule,
                1000,
                WireFamilyId::AnthropicMessages,
                Some(300_000),
                true
            )
            .unwrap_err()
            .rate,
            "cache_read"
        );
    }
}
