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

    /// Buy reads until what this wait spent reaches the cost of one cache bust (llm-runner
    /// §9.3.5: `auto` warms "until the warms since the last real request reach the cost of one
    /// cache rewrite"), so the last read may take the total to it or just past it. A small
    /// rounding allowance stops repeated fractional additions that land a hair below the bust
    /// from buying one more. It is relative to the prices, not a fixed monetary floor.
    pub fn allows_next(self, spent: f64) -> bool {
        if [self.warm, self.bust, spent]
            .iter()
            .any(|amount| !amount.is_finite() || *amount < 0.0)
        {
            return false;
        }
        let tolerance = f64::EPSILON * 16.0 * self.bust.abs().max(spent.abs());
        self.bust - spent > tolerance
    }
}

/// Why a keep-warm sends no more warms, as the step report maps them (llm-runner `warming`):
/// `auto` reached the cost of one cache rewrite (`cost-cap`).
pub const STOP_BUDGET: &str = "budget";
/// `always` or `hours` sent [`WARM_COUNT_CAP`] warms since the last real request (`count-cap`).
pub const STOP_COUNT_CAP: &str = "count_cap";
/// `hours`, and the warm fell due outside its window (`outside-hours`).
pub const STOP_OUTSIDE_HOURS: &str = "outside_hours";

pub use broca_protocol::request::WARM_COUNT_CAP;

/// Whether `keep_warm` lets one more warm go out at `now_ms`, after `warms` warms costing
/// `spent` since the last real request. Only `auto` is priced: it buys reads while they stay
/// below one cache rewrite (`costs`). `always` and `hours` stop at [`WARM_COUNT_CAP`], and `hours`
/// sends only inside its window. Answers the stop reason, with a detail for the log, otherwise.
pub fn allows_warm(
    keep_warm: &broca_protocol::request::KeepWarm,
    costs: Result<WarmCosts, String>,
    spent: f64,
    warms: u32,
    now_ms: u64,
) -> Result<(), (&'static str, String)> {
    use broca_protocol::request::KeepWarm;
    match keep_warm {
        KeepWarm::Auto => match costs {
            Ok(costs) if costs.allows_next(spent) => Ok(()),
            Ok(_) => Err((
                STOP_BUDGET,
                "the warms since the last real request cost one cache rewrite".into(),
            )),
            Err(detail) => Err((STOP_BUDGET, detail)),
        },
        KeepWarm::Always | KeepWarm::Hours(_) if warms >= WARM_COUNT_CAP => Err((
            STOP_COUNT_CAP,
            format!("{warms} warms since the last real request"),
        )),
        KeepWarm::Hours(hours) if !hours.contains(now_ms) => Err((
            STOP_OUTSIDE_HOURS,
            format!("outside {}-{} in {}", hours.from, hours.to, hours.tz),
        )),
        KeepWarm::Always | KeepWarm::Hours(_) => Ok(()),
        KeepWarm::Off => Err(("unsupported", "keep_warm is off".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rent_or_buy_matches_provider_ratio_table() {
        let cases = [
            ("Anthropic 5m", 0.1, 1.25, 0.0, 0, 13),
            ("Anthropic 1h", 0.1, 2.0, 0.0, 0, 20),
            ("Opus 5.5 5m", 0.05, 1.25, 0.0, 0, 25),
            ("Opus 5.5 1h", 0.05, 2.0, 0.0, 0, 40),
            ("Codex", 0.05, 1.0, 8.0, 16, 20),
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
        for _ in 0..10 {
            assert!(costs.allows_next(spent));
            spent += costs.warm;
        }
        // Ten reads of 0.1 add up to a hair below 1.0, which counts as reaching it.
        assert!(spent < 1.0);
        assert!(!costs.allows_next(spent));
        assert!(costs.allows_next(0.9));
        assert!(costs.allows_next(0.99));
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
        // A read that costs a whole rewrite is bought once: that one read reaches the cap.
        let even = WarmCosts::from_ratios(100, 1e-20, 1e-20, 0.0, 0).unwrap();
        assert!(even.allows_next(0.0));
        assert!(!even.allows_next(even.warm));
    }

    mod modes {
        use super::super::{
            allows_warm, WarmCosts, STOP_BUDGET, STOP_COUNT_CAP, STOP_OUTSIDE_HOURS, WARM_COUNT_CAP,
        };
        use broca_protocol::request::{KeepWarm, WarmHours};

        /// 2026-11-17 22:30 UTC, in Unix milliseconds.
        const LATE_EVENING_UTC_MS: u64 = 1_794_954_600_000;

        fn costs() -> Result<WarmCosts, String> {
            Ok(WarmCosts {
                warm: 1.0,
                bust: 10.0,
            })
        }

        fn night() -> KeepWarm {
            KeepWarm::Hours(WarmHours {
                from: "22:00".into(),
                to: "06:00".into(),
                tz: "Etc/UTC".into(),
            })
        }

        #[test]
        fn auto_stops_at_the_cost_of_one_rewrite_whatever_the_count() {
            assert!(allows_warm(&KeepWarm::Auto, costs(), 9.0, 100, LATE_EVENING_UTC_MS).is_ok());
            assert_eq!(
                allows_warm(&KeepWarm::Auto, costs(), 10.0, 0, LATE_EVENING_UTC_MS)
                    .unwrap_err()
                    .0,
                STOP_BUDGET
            );
            assert_eq!(
                allows_warm(
                    &KeepWarm::Auto,
                    Err("no rate".into()),
                    0.0,
                    0,
                    LATE_EVENING_UTC_MS
                )
                .unwrap_err()
                .0,
                STOP_BUDGET
            );
        }

        #[test]
        fn always_ignores_price_and_stops_at_the_count_cap() {
            assert!(allows_warm(
                &KeepWarm::Always,
                Err("no rate".into()),
                1e9,
                WARM_COUNT_CAP - 1,
                LATE_EVENING_UTC_MS
            )
            .is_ok());
            assert_eq!(
                allows_warm(
                    &KeepWarm::Always,
                    costs(),
                    0.0,
                    WARM_COUNT_CAP,
                    LATE_EVENING_UTC_MS
                )
                .unwrap_err()
                .0,
                STOP_COUNT_CAP
            );
        }

        #[test]
        fn hours_warms_only_inside_its_window_and_under_the_count_cap() {
            assert!(allows_warm(&night(), costs(), 1e9, 0, LATE_EVENING_UTC_MS).is_ok());
            // Noon the same day is outside 22:00-06:00.
            let noon = LATE_EVENING_UTC_MS - 10 * 3_600_000 - 30 * 60_000;
            assert_eq!(
                allows_warm(&night(), costs(), 0.0, 0, noon).unwrap_err().0,
                STOP_OUTSIDE_HOURS
            );
            assert_eq!(
                allows_warm(&night(), costs(), 0.0, WARM_COUNT_CAP, LATE_EVENING_UTC_MS)
                    .unwrap_err()
                    .0,
                STOP_COUNT_CAP
            );
        }
    }
}
