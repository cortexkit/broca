## Which issue gave this a go?

<!-- Required. Link the issue where scope was agreed. A PR without one will be
     asked for the issue rather than reviewed — see CONTRIBUTING.md. -->

Closes #

## What changed

<!-- The mechanism, not the diff: what was wrong, what is different now. -->

## Bug-fix regression test

<!-- For a bug fix, add a test that fails without the fix and show that once. -->

## Checks

- [ ] `cargo check --workspace --all-targets` is clean (not just `--workspace`)
- [ ] `./scripts/quickcheck.sh <touched crates>` passes
- [ ] No type errors suppressed, no tests deleted to pass
- [ ] Comments give the reason for a reader with no context
