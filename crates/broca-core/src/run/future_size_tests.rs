//! The run loop's public entries must keep the run body off their callers' stacks.
//!
//! The body's future (`run_on_writer`, about 42 KiB) used to sit inline in every entry that
//! awaited it, and in every future that awaited those. Unoptimized builds copy a future through
//! each enclosing poll frame, so the inline body pushed
//! `compaction::tests::an_announced_view_is_fetched_at_the_step_boundary_and_applied` past its
//! 2 MiB test thread in the workspace's Linux test build: 24 more bytes in each stored record and
//! queued send were enough. The entries now box the body; this guard fails if one stops.

#![allow(unreachable_code, clippy::diverging_sub_expression)]

use crate::{AllowAll, FileLeaseStore, FileWalStore, StubProvider, StubToolPlane};

/// The size of the future `call` returns, read from its type: `call` is never invoked, so its
/// arguments may be `todo!()`.
fn future_size<Fut>(_call: impl FnOnce() -> Fut) -> usize {
    std::mem::size_of::<Fut>()
}

/// Above every entry's own state (about 5-14 KiB), and well below the 42 KiB body it must not
/// hold.
const ENTRY_FUTURE_MAX: usize = 24 * 1024;

#[test]
fn run_loop_entries_keep_the_run_body_on_the_heap() {
    type P = StubProvider;
    type T = StubToolPlane;
    type A = AllowAll;
    type W = FileWalStore;
    type L = FileLeaseStore;
    let entries = [
        (
            "run_loop",
            future_size(|| {
                super::run_loop::<P, T, L, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_under_lease",
            future_size(|| {
                super::run_loop_under_lease::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_from_offsets",
            future_size(|| {
                super::run_loop_from_offsets::<P, T, L, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_on_writer",
            future_size(|| {
                super::run_loop_on_writer::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_on_writer_admitted",
            future_size(|| {
                super::run_loop_on_writer_admitted::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_on_writer_admitted_with_prior_snapshot",
            future_size(|| {
                super::run_loop_on_writer_admitted_with_prior_snapshot::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_on_writer_from_offsets",
            future_size(|| {
                super::run_loop_on_writer_from_offsets::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
        (
            "run_loop_on_writer_with_prepared_resume",
            future_size(|| {
                super::run_loop_on_writer_with_prepared_resume::<P, T, A, W>(
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                    todo!(),
                )
            }),
        ),
    ];
    for (name, size) in entries {
        println!("{name}: {size} bytes");
        assert!(
            size <= ENTRY_FUTURE_MAX,
            "{name} carries {size} bytes in its callers; box the run body it awaits"
        );
    }
}
