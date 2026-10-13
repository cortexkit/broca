#[allow(dead_code)]
use crate::render_golden_support;

use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn changed_bytes_without_version_bump_fail_with_a_clear_error() {
    let failure = catch_unwind(AssertUnwindSafe(|| {
        render_golden_support::assert_matches_or_requires_bump(
            b"new rendered bytes",
            b"old rendered bytes\n",
            "7\n",
            7,
            "mutation probe",
        );
    }))
    .expect_err("a changed render without a version bump must fail");

    let message = failure
        .downcast_ref::<String>()
        .expect("version enforcement should panic with a String message");
    assert!(message.contains("a render change requires a renderer version bump"));
}

#[test]
fn changed_bytes_with_a_version_bump_pass_the_version_gate() {
    render_golden_support::validate_version_for_change(
        b"new rendered bytes",
        b"old rendered bytes\n",
        "7\n",
        8,
        "mutation probe",
    )
    .expect("a changed render with a newer renderer version is an allowed transition");
}
