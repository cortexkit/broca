use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

#[derive(Default)]
struct ConnectionProbe {
    begin_us: Option<u128>,
    acquired_us: Option<u128>,
    implicit: bool,
    max_hold_us: u128,
    max_wait_us: u128,
    max_span_us: u128,
    transactions: usize,
}

struct Probe {
    start: Instant,
    connections: [ConnectionProbe; 2],
    timeline: String,
    errors: String,
}

fn probe() -> &'static Mutex<Probe> {
    static PROBE: OnceLock<Mutex<Probe>> = OnceLock::new();
    PROBE.get_or_init(|| {
        Mutex::new(Probe {
            start: Instant::now(),
            connections: Default::default(),
            timeline: String::new(),
            errors: String::new(),
        })
    })
}

pub(super) fn reset() {
    *probe().lock().unwrap() = Probe {
        start: Instant::now(),
        connections: Default::default(),
        timeline: String::new(),
        errors: String::new(),
    };
}

pub(super) fn message(message: &str) {
    use std::fmt::Write;
    let mut probe = probe().lock().unwrap();
    let now = probe.start.elapsed().as_micros();
    writeln!(probe.errors, "at_us={now} {message}").unwrap();
}

// Installing SQLite's legacy profile callback replaces its trace callback. Use
// profile alone, reconstructing statement start from its millisecond duration.
pub(super) fn event(connection: usize, sql: &str, elapsed: Duration) {
    use std::fmt::Write;
    let mut probe = probe().lock().unwrap();
    let now = probe.start.elapsed().as_micros();
    let sql = sql.trim_start();
    let state = &mut probe.connections[connection];
    let write = ["INSERT", "UPDATE", "DELETE"]
        .iter()
        .any(|prefix| sql.starts_with(prefix));
    if sql.starts_with("BEGIN") {
        state.begin_us = Some(now.saturating_sub(elapsed.as_micros()));
        state.acquired_us = sql.starts_with("BEGIN IMMEDIATE").then_some(now);
        state.implicit = false;
    } else if write {
        if state.begin_us.is_none() {
            state.begin_us = Some(now.saturating_sub(elapsed.as_micros()));
            state.implicit = true;
        }
        // Deferred and implicit transactions acquire during their first DML.
        // Its completion is an upper bound on acquisition, not a lock wait.
        if state.acquired_us.is_none() {
            state.acquired_us = Some(now);
        }
    }
    if sql.starts_with("COMMIT") || sql.starts_with("ROLLBACK") || (write && state.implicit) {
        if let Some(begin) = state.begin_us.take() {
            let acquired = state.acquired_us.take().unwrap_or(begin);
            let hold = now - acquired;
            let wait = acquired - begin;
            state.max_hold_us = state.max_hold_us.max(hold);
            state.max_wait_us = state.max_wait_us.max(wait);
            state.max_span_us = state.max_span_us.max(now - begin);
            state.transactions += 1;
            // Millisecond triples keep all twenty timelines within the CI log budget.
            write!(
                probe.timeline,
                "{}{},{},{};",
                if connection == 0 { 'C' } else { 'S' },
                begin / 1000,
                acquired / 1000,
                now / 1000,
            )
            .unwrap();
        }
    }
}

pub(super) fn report(iteration: usize, panicked: bool) -> String {
    use std::fmt::Write;
    let probe = probe().lock().unwrap();
    let mut report = format!("CENSUS_LOCK_PROBE iteration={iteration} panicked={panicked}\n");
    for (name, state) in ["census", "serving"].into_iter().zip(&probe.connections) {
        assert!(
            state.transactions > 0,
            "{name}: profile must observe transactions"
        );
        writeln!(report, "conn={name} tx={} max_hold_lower_bound_us={} max_transaction_span_us={} max_first_statement_or_wait_us={} unfinished_begin_us={:?} unfinished_acquired_us={:?}", state.transactions, state.max_hold_us, state.max_span_us, state.max_wait_us, state.begin_us, state.acquired_us).unwrap();
    }
    report.push_str(&probe.errors);
    report.push_str("timeline C=census S=serving; triples=begin,acquired,release in monotonic ms; begin reconstructed from profile duration (+/-1ms), deferred/autocommit acquisition is first-DML completion\n");
    if probe.timeline.len() <= 7500 {
        report.push_str(&probe.timeline);
    } else {
        report.push_str(&probe.timeline[..3500]);
        report.push_str(" ... middle elided; extrema above cover all transactions ... ");
        report.push_str(&probe.timeline[probe.timeline.len() - 3500..]);
    }
    report.push('\n');
    report
}
