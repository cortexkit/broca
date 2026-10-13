# Contributing to broca

Thank you for contributing to broca. Use the workflow below to discuss scope,
reproduce problems, and verify changes before opening a pull request.

## Issue first, then a go, then a PR

1. Open an issue. Tag it `bug` or `design discussion`.
2. We reply — usually quickly. For a bug with an obvious shape that is a
   reproduction and a go; for anything with a design question in it, we discuss
   it there.
3. Once the issue has an explicit go, open the PR and reference the issue.

A PR that arrives without an issue that got a go will be asked for the issue
rather than reviewed. This is not a judgement on the work. It is because the
ten-line conversation in an issue is where scope gets settled, and a reviewer
facing a finished implementation is under quiet pressure to find a way to take
it — which is its own failure mode. Settling scope before the code exists is
cheaper for everyone, and cheapest for the person who would otherwise have
written the code.

## What a good issue carries

- **A reproduction, or the command that would produce one.** A claim we can run
  is settled in minutes; a claim we have to re-derive is settled in hours.
- **The mechanism, if you have it.** "X returns Y because Z at `file:line`" is
  the shape that gets a go fastest, because it names what to check.
- **What you are NOT claiming.** If you measured one case and are inferring the
  rest, say so. An honest bound is worth more than a confident guess.

## What a good PR carries

- The issue number that gave it a go.
- **Mutation evidence** for anything with a test: name the change that makes the
  new test fail, and show it failing. A test that passes with the fix and would
  also pass without it is not a test.
- For a fix: both directions where they exist. "Retryable code is retried" and
  "terminal code is not retried" are two tests, not one.
- `cargo check --workspace --all-targets` clean. `--workspace` alone builds
  neither tests nor examples; a construct-breaking change can leave a dozen
  test sites red behind a green library build.

## Ground rules that will come up in review

- **Never suppress a type error.** No `as any`-shaped casts, no `#[allow]` to
  make a warning go away, no deleted test to make a suite green.
- **A refusal must say what it is about, not only what would silence it.** If
  you write an error message, name the subject, not just the remedy.
- **Comments explain the reason for a reader with no context.** Never reference
  a task id, a chat, or a review round a future reader cannot see.
- **Durability core changes go through the crash-cut harness unmodified.** If a
  change needs the harness altered to pass, that is a finding about the change.

## Where things are

- [Architecture](docs/architecture.md) — the run loop, durability boundaries and crate map.
- [Public contracts](docs/module-surface.md) and design notes live under `docs/`.
- `docs/module-surface.md` — the wire contract this module serves.
- `docs/error-class-contract.md` — how errors are classified and why consumers
  decode the class as an open string.
- `Cargo.toml` and `Cargo.lock` — exact registry releases and pinned git sources for
  linked crates. `scripts/check-path-deps.py` rejects dependencies outside this repo.
- [docs/mutations.md](docs/mutations.md) — tooling for the checked-in mutation catalogue:
  pinned runner installation, one-row replay, new proofs and the nightly breadth audit.
