//! A conformance report carrying no checks must not read as conformant.
//!
//! `passed()` is `all()` over the checks, and `all()` over an empty list is true — so an
//! empty report is the one shape that claims every family invariant holds while having
//! tested none of them. Today every producer pushes its checks unconditionally, so the
//! shape is unreachable through them; this pins the property at the type instead, because
//! `checks` is a public field and a check that becomes conditional later would open the
//! hole silently. The symptom would be a green conformance run.
//!
//! Kept as a test rather than proven once and deleted: a guard demonstrated by a throwaway
//! is a guard nothing defends.

use broca_provider::conformance::ConformanceReport;

#[test]
fn an_empty_report_does_not_pass() {
    let report = ConformanceReport { checks: Vec::new() };
    assert!(
        !report.passed(),
        "an empty conformance report must not read as conformant"
    );
}

#[test]
fn a_report_whose_checks_all_passed_does_pass() {
    // The other half: the emptiness guard must not swallow the property it guards.
    let report = ConformanceReport {
        checks: vec![("some_check", Ok(()))],
    };
    assert!(report.passed(), "a report of passing checks must pass");
}
