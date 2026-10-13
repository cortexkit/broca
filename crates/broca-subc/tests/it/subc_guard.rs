//! Verifies the anti-masking guard for the real-subc spine suite actually guards.
//!
//! The real-subc tests (dogfood, resume_spine, tool_fault_spine, the live full-stack
//! demo) skip cleanly when the subc binaries can't be found — convenient for local dev
//! (run without first building the sibling subc-core bins), but a masking risk in CI: a
//! silent skip gives the whole spine suite ZERO coverage while showing green. So
//! `decide_subc_bin` turns "a required subc binary is missing" into a LOUD failure when
//! required (CI), and a clean skip otherwise (local).
//!
//! This guard can't be exercised by the normal CI run (there the binaries are always
//! present after the build step, so the fail-loud branch never triggers), so we prove it
//! here directly: required + absent → panics; not-required + absent → skips; present →
//! passes through. The env read (`subc_required`) is kept separate from `decide_subc_bin`
//! precisely so the decision is testable without manipulating the process environment.

use crate::common;

use std::path::PathBuf;

use common::decide_subc_bin;

#[test]
fn required_and_absent_fails_loud() {
    // The masking-prevention case: required + binary absent must PANIC, not skip.
    let result = std::panic::catch_unwind(|| decide_subc_bin(None, true, "fake-aft-stub"));
    assert!(
        result.is_err(),
        "decide_subc_bin(None, required=true) must panic — a required-but-absent subc \
         binary is a loud failure, never a silent skip"
    );
}

#[test]
fn not_required_and_absent_skips() {
    // Local-dev case: not required + absent → None (the caller skips cleanly).
    assert!(
        decide_subc_bin(None, false, "fake-aft-stub").is_none(),
        "decide_subc_bin(None, required=false) returns None so the caller skips"
    );
}

#[test]
fn present_passes_through_regardless_of_required() {
    // A found binary is returned untouched whether or not it was required.
    let path = PathBuf::from("/some/subc-core");
    assert_eq!(
        decide_subc_bin(Some(path.clone()), true, "subc-core"),
        Some(path.clone())
    );
    assert_eq!(
        decide_subc_bin(Some(path.clone()), false, "subc-core"),
        Some(path)
    );
}
