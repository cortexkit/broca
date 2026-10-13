//! Verifies the anti-masking guard for the aimock e2e suite actually guards.
//!
//! The family e2e tests skip cleanly when aimock can't boot — convenient for local dev
//! (run without `npm ci`), but a masking risk in CI: a silent skip gives the whole mock
//! matrix ZERO coverage while showing green. So `decide` turns "aimock didn't boot" into
//! a LOUD failure when required (CI), and a clean skip otherwise (local).
//!
//! This guard can't be exercised by the normal CI run (there aimock is always present
//! after `npm ci`, so the fail-loud branch never triggers), so we prove it here directly:
//! required + absent → panics; not-required + absent → skips; present → passes through.
//! The env read (`aimock_required`) is kept separate from `decide` precisely so the
//! decision is testable without manipulating the process environment.

use crate::family_e2e;

use family_e2e::decide;

#[test]
fn required_and_absent_fails_loud() {
    // The masking-prevention case: required + aimock absent must PANIC, not skip.
    let result = std::panic::catch_unwind(|| decide::<()>(None, true, "guard test"));
    assert!(
        result.is_err(),
        "decide(None, required=true) must panic — a required-but-absent aimock is a loud \
         failure, never a silent skip"
    );
}

#[test]
fn not_required_and_absent_skips() {
    // Local-dev case: not required + absent → None (the caller skips cleanly).
    assert!(
        decide::<()>(None, false, "guard test").is_none(),
        "decide(None, required=false) returns None so the caller skips"
    );
}

#[test]
fn present_passes_through_regardless_of_required() {
    // A booted aimock is returned untouched whether or not it was required.
    assert_eq!(decide(Some(7u32), true, "guard test"), Some(7));
    assert_eq!(decide(Some(7u32), false, "guard test"), Some(7));
}
