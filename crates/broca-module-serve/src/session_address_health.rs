//! Process-local evidence of failed address resolution, independent of SQLite health.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock, Mutex,
};
use std::time::{Duration, Instant};

use broca_module::diagnostics::{Event, Ring, Snapshot};
use broca_store::session_address::{InventoryUnavailable, SessionAddressError};
use serde_json::{json, Value};

const REASONS: [&str; 5] = [
    "incomplete_inventory",
    "gap_marker",
    "store_error",
    "ambiguous",
    "invalid_identity",
];
const WARN_INTERVAL: Duration = Duration::from_secs(60);

struct State {
    counters: [AtomicU64; 5],
    last_warn: Mutex<[Option<Instant>; 3]>,
    unavailable: Ring,
}

impl State {
    fn new(since_ms: i64) -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            last_warn: Mutex::new([None; 3]),
            unavailable: Ring::new(since_ms),
        }
    }

    fn record(&self, error: &SessionAddressError, at_ms: i64, now: Instant) -> bool {
        let index = match error {
            SessionAddressError::Unavailable(reason) => match reason {
                InventoryUnavailable::IncompleteInventory => 0,
                InventoryUnavailable::GapMarker => 1,
                InventoryUnavailable::StoreError(_) => 2,
            },
            SessionAddressError::Ambiguous => 3,
            SessionAddressError::Invalid(_) => 4,
        };
        self.counters[index].fetch_add(1, Ordering::Relaxed);
        if index >= 3 {
            return false;
        }
        self.unavailable.record(Event {
            at_ms,
            module: String::new(),
            code: REASONS[index].into(),
        });
        let mut last = self
            .last_warn
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if last[index]
            .is_some_and(|previous| now.saturating_duration_since(previous) < WARN_INTERVAL)
        {
            return false;
        }
        last[index] = Some(now);
        true
    }

    fn health(&self) -> Value {
        Value::Object(
            REASONS
                .iter()
                .zip(&self.counters)
                .map(|(name, counter)| ((*name).to_owned(), json!(counter.load(Ordering::Relaxed))))
                .collect(),
        )
    }
}

static STATE: LazyLock<State> = LazyLock::new(|| State::new(broca_module::diagnostics::now_ms()));

pub(crate) fn initialize() {
    LazyLock::force(&STATE);
}

pub(crate) fn record_refusal(error: &SessionAddressError) {
    record_into(
        &STATE,
        error,
        broca_module::diagnostics::now_ms(),
        Instant::now(),
    );
}

fn record_into(state: &State, error: &SessionAddressError, at_ms: i64, now: Instant) {
    if !state.record(error, at_ms, now) {
        return;
    }
    let SessionAddressError::Unavailable(reason) = error else {
        return;
    };
    let message: String = error
        .to_string()
        .replace(['\r', '\n'], " ")
        .chars()
        .take(400)
        .collect();
    tracing::warn!(target: "session_address", reason = reason.reason(), error = %message,
        "session bind refused because its address inventory is unavailable");
}

pub(crate) fn health() -> Value {
    STATE.health()
}

pub(crate) fn snapshot(since: i64, now: i64) -> Result<Snapshot, String> {
    STATE.unavailable.snapshot(since, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_warnings_are_one_line_and_rate_limited_per_reason() {
        #[derive(Clone)]
        struct Buffer(std::sync::Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buffer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let bytes = std::sync::Arc::new(Mutex::new(Vec::new()));
        let buffer = Buffer(bytes.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .without_time()
            .with_writer(move || buffer.clone())
            .finish();
        let state = State::new(100);
        let error = SessionAddressError::store_error("database\nis locked");
        let now = Instant::now();
        tracing::subscriber::with_default(subscriber, || {
            record_into(&state, &error, 110, now);
            record_into(&state, &error, 120, now + Duration::from_secs(1));
            record_into(&state, &error, 130, now + WARN_INTERVAL);
        });
        let captured = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert_eq!(captured.lines().count(), 2, "{captured}");
        assert!(captured
            .lines()
            .all(|line| line.contains("store_error") && line.contains("database is locked")));
    }

    #[test]
    fn counters_and_error_evidence_start_on_the_first_refusal_while_warnings_are_limited() {
        let state = State::new(100);
        assert!(state
            .health()
            .as_object()
            .unwrap()
            .values()
            .all(|value| value.as_u64() == Some(0)));
        let error = SessionAddressError::Unavailable(InventoryUnavailable::IncompleteInventory);
        let start = Instant::now();
        assert!(state.record(&error, 110, start));
        assert_eq!(
            state.unavailable.snapshot(100, 110).unwrap().events[0].code,
            "incomplete_inventory"
        );
        assert!(!state.record(&error, 120, start + Duration::from_secs(1)));
        assert!(state.record(&error, 130, start + WARN_INTERVAL));
        let gap = SessionAddressError::Unavailable(InventoryUnavailable::GapMarker);
        assert!(state.record(&gap, 140, start + WARN_INTERVAL));
        assert!(state.record(
            &SessionAddressError::store_error("database is locked"),
            150,
            start + WARN_INTERVAL
        ));
        assert!(!state.record(&SessionAddressError::Ambiguous, 160, start));
        assert!(!state.record(
            &SessionAddressError::Invalid("separator".into()),
            170,
            start
        ));
        assert_eq!(
            state.health(),
            json!({"incomplete_inventory":3,"gap_marker":1,"store_error":1,"ambiguous":1,"invalid_identity":1})
        );
        assert_eq!(
            state.unavailable.snapshot(100, 170).unwrap().events.len(),
            5
        );
    }
}
