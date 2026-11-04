//! Process-lifetime scoped role-open refusal counts and bounded-frequency diagnostics.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use broca_protocol::BindIdentity;

use crate::connection::ConnError;

const WARN_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Refusals {
    counts: BTreeMap<String, u64>,
    warned: HashMap<(String, String), Instant>,
}

impl Refusals {
    fn record(&mut self, provider: &str, code: &str, now: Instant) -> bool {
        let count = self.counts.entry(code.into()).or_default();
        *count = count.saturating_add(1);
        self.warned
            .retain(|_, last| now.duration_since(*last) < WARN_INTERVAL);
        let key = (provider.into(), code.into());
        if self.warned.contains_key(&key) {
            return false;
        }
        self.warned.insert(key, now);
        true
    }
}

fn process_refusals() -> &'static Mutex<Refusals> {
    static REFUSALS: OnceLock<Mutex<Refusals>> = OnceLock::new();
    REFUSALS.get_or_init(Mutex::default)
}

/// Daemon refusals of scoped compaction/step-transform route opens, keyed by refusal code.
/// Counts reset at process restart and include opens whose diagnostic was rate-limited.
pub fn scoped_role_route_refusals() -> BTreeMap<String, u64> {
    process_refusals().lock().unwrap().counts.clone()
}

pub(crate) fn observe(provider: &str, role: &str, bind: &BindIdentity, error: &ConnError) {
    let ConnError::Rejected(refusal) = error else {
        return;
    };
    let code = refusal.code();
    if process_refusals()
        .lock()
        .unwrap()
        .record(provider, code, Instant::now())
    {
        tracing::warn!(target: "routing", session = %bind.session, harness = %bind.harness,
            session_address = %broca_core::session_addr(bind), provider, role, code,
            "scoped role route open refused by daemon");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_refusal_counts_but_warnings_are_limited_per_provider_and_code() {
        let mut tally = Refusals::default();
        let now = Instant::now();
        assert!(tally.record("mc", "scope_not_carrier", now));
        assert!(!tally.record("mc", "scope_not_carrier", now + Duration::from_secs(59)));
        assert!(tally.record(
            "transform",
            "scope_not_carrier",
            now + Duration::from_secs(59)
        ));
        assert!(tally.record("mc", "scope_ended", now + Duration::from_secs(59)));
        assert!(tally.record("mc", "scope_not_carrier", now + WARN_INTERVAL));
        assert_eq!(
            tally.counts,
            BTreeMap::from([("scope_not_carrier".into(), 4), ("scope_ended".into(), 1),])
        );
    }
}
