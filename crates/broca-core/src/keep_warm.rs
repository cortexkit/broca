//! Cache-maintenance economics without clocks, providers, or IO.

/// Costs in one consistent unit. The reader uses money for metered accounts and input-token
/// equivalents for subscriptions; the decision itself depends only on their ratios.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WarmCosts {
    pub warm: f64,
    pub bust: f64,
}

impl WarmCosts {
    /// Read and output prices must use the same unit as the bust price. An absent rate must be
    /// rejected by the reader before calling this function, never substituted with zero.
    pub fn from_ratios(
        prefix_tokens: u64,
        read: f64,
        bust: f64,
        output: f64,
        output_tokens: u64,
    ) -> Option<Self> {
        if [read, bust, output]
            .iter()
            .any(|rate| !rate.is_finite() || *rate < 0.0)
        {
            return None;
        }
        let costs = Self {
            warm: prefix_tokens as f64 * read + output_tokens as f64 * output,
            bust: prefix_tokens as f64 * bust,
        };
        (costs.warm.is_finite() && costs.bust.is_finite()).then_some(costs)
    }

    /// Do not buy a read that would make this wait cost as much as one cache bust. A small
    /// rounding allowance prevents repeated fractional additions from buying an extra read
    /// at mathematical equality. It is relative to the prices, not a fixed monetary floor.
    pub fn allows_next(self, spent: f64) -> bool {
        if [self.warm, self.bust, spent]
            .iter()
            .any(|amount| !amount.is_finite() || *amount < 0.0)
        {
            return false;
        }
        let next = spent + self.warm;
        let tolerance = f64::EPSILON * 16.0 * self.bust.abs().max(next.abs());
        next.is_finite() && self.bust - next > tolerance
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rent_or_buy_matches_provider_ratio_table() {
        let cases = [
            ("Anthropic 5m", 0.1, 1.25, 0.0, 0, 12),
            ("Anthropic 1h", 0.1, 2.0, 0.0, 0, 19),
            ("Opus 5.5 5m", 0.05, 1.25, 0.0, 0, 24),
            ("Opus 5.5 1h", 0.05, 2.0, 0.0, 0, 39),
            ("Codex", 0.05, 1.0, 8.0, 16, 19),
        ];
        for (name, read, bust, output, output_tokens, expected) in cases {
            let costs = WarmCosts::from_ratios(125_000, read, bust, output, output_tokens).unwrap();
            let mut spent = 0.0;
            let mut count = 0;
            while costs.allows_next(spent) {
                spent += costs.warm;
                count += 1;
                assert!(count < 100, "{name} failed to stop");
            }
            assert_eq!(count, expected, "{name}");
        }
    }

    #[test]
    fn rent_or_buy_stops_exactly_at_boundary_including_fractional_rounding() {
        let costs = WarmCosts::from_ratios(1, 0.1, 1.0, 0.0, 0).unwrap();
        let mut spent = 0.0;
        for _ in 0..9 {
            assert!(costs.allows_next(spent));
            spent += costs.warm;
        }
        assert!(!costs.allows_next(spent));
        assert!(!costs.allows_next(0.9));
        assert!(costs.allows_next(0.89));
        assert!(!costs.allows_next(1.0));
    }

    #[test]
    fn rent_or_buy_rejects_invalid_rates_and_spend_without_a_free_price_floor() {
        for bad in [f64::NAN, f64::INFINITY, -1.0] {
            assert!(WarmCosts::from_ratios(100, bad, 1.0, 0.0, 0).is_none());
            assert!(!WarmCosts {
                warm: 1.0,
                bust: 100.0
            }
            .allows_next(bad));
        }
        assert!(!WarmCosts::from_ratios(100, 0.0, 0.0, 0.0, 0)
            .unwrap()
            .allows_next(0.0));
        assert!(WarmCosts::from_ratios(100, 0.0, 1.0, 0.0, 0)
            .unwrap()
            .allows_next(0.0));
        assert!(!WarmCosts::from_ratios(100, 1e-20, 1e-20, 0.0, 0)
            .unwrap()
            .allows_next(0.0));
    }
}
