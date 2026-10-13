//! The conformance harness — the gate every wire family (and every parallel author
//! adding one) must pass. It mechanically ENFORCES the framework's load-bearing
//! invariants, so they hold by construction rather than by a reviewer noticing.
//!
//! A family that passes `check_render_determinism` cannot have the determinism bugs
//! that silently bust the prompt cache and break resume — the harness reproduces
//! exactly the conditions those bugs hide in.

use broca_protocol::{CallOptions, NormalizedEvent, StepId};

use crate::wire::{FrozenRenderConfig, WireFamily};

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The result of a conformance run: a list of `(check_name, Result)`. A family is
/// conformant iff every check is `Ok`.
#[derive(Debug)]
pub struct ConformanceReport {
    pub checks: Vec<(&'static str, Result<(), String>)>,
}

impl ConformanceReport {
    /// Whether every check passed.
    ///
    /// `all()` over an empty list is TRUE, so a report carrying no checks passes — the
    /// vacuous shape, in the one place where it would read as "this family is conformant".
    /// Safe today by construction: every producing function pushes its checks
    /// unconditionally, so a report always carries at least one. That is a property of the
    /// current producers rather than of this type, and `checks` is a pub field on a pub
    /// struct, so a caller can build an empty report directly.
    ///
    /// Made explicit rather than left to the producers: a check that becomes conditional
    /// later ("only run the ambient probe on families that read env") would open the hole
    /// silently, and the symptom is a green conformance run.
    pub fn passed(&self) -> bool {
        !self.checks.is_empty() && self.checks.iter().all(|(_, r)| r.is_ok())
    }

    /// The names + messages of any failed checks (for a test assertion message).
    pub fn failures(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter_map(|(name, r)| r.as_ref().err().map(|e| format!("{name}: {e}")))
            .collect()
    }
}

/// Run the render-determinism checks for `family` against `frozen` + a
/// representative `opts`. This is the core gate; it does NOT touch the network.
///
/// Checks:
///  1. RENDER-TWICE with fresh intermediates — render the same inputs twice (each
///     call builds its own intermediate containers) and require identical bytes.
///     This catches nondeterminism that canonical JSON CANNOT fix: canonical JSON
///     sorts object KEYS but preserves array ORDER, so a family that builds a JSON
///     array by iterating a `HashMap` (randomized per-process iteration order)
///     leaks nondeterminism into array order. A fresh-intermediates re-render
///     surfaces it because the standard `HashMap` reseeds per instance.
///  2. PERTURBED-AMBIENT — render under a mutated process environment (timezone,
///     locale, an injected env var) and require the SAME bytes. A pure renderer
///     ignores ambient state; one that reads the clock/TZ/env leaks it here.
///
/// The catalog-mutation-freeze check (mutate the spec AFTER freezing, re-render,
/// require identical bytes) needs a real spec→frozen resolver, so it lives in
/// `check_freeze_against_mutation` and is called by the per-family conformance test.
pub fn check_render_determinism(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
) -> ConformanceReport {
    let _env_lock = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut checks = Vec::new();

    // Process-global environment variables are shared by every test thread in this
    // binary. Hold one lock from the baseline render through the perturbed render
    // so no other conformance check can interleave its own ambient mutation.
    // 1. Render-twice with fresh intermediates.
    let first = family.render(frozen, opts);
    let second = family.render(frozen, opts);
    checks.push((
        "render_twice_fresh_intermediates",
        if first == second {
            Ok(())
        } else {
            Err(format!(
                "two renders of identical inputs differ ({} vs {} bytes) — likely \
                 nondeterministic array order (e.g. building an array from a HashMap)",
                first.len(),
                second.len()
            ))
        },
    ));

    // 2. Perturbed ambient: mutate TZ/locale/an env var, render, restore, compare.
    let perturbed = with_perturbed_env(|| family.render(frozen, opts));
    checks.push((
        "perturbed_ambient",
        if first == perturbed {
            Ok(())
        } else {
            Err(
                "render bytes changed under a mutated environment (TZ/LANG/env) — \
                 the renderer is reading ambient state instead of being pure"
                    .to_string(),
            )
        },
    ));

    ConformanceReport { checks }
}

/// CATALOG-MUTATION-FREEZE: prove that freezing the render config at run start makes
/// the rendered bytes independent of later catalog/spec changes. The caller supplies
/// a `resolve` that turns the current spec into a `FrozenRenderConfig`; this:
///  1. resolves + freezes from the original spec, renders (the "run start" bytes),
///  2. lets the caller MUTATE the spec (via `mutate`),
///  3. renders again FROM THE FROZEN config (what resume does).
///
/// It requires the two byte strings to be identical. If a render input had leaked
/// past the freeze (e.g. read live from the spec), the mutation would change the
/// bytes and this fails.
pub fn check_freeze_against_mutation<S>(
    family: &dyn WireFamily,
    spec: &mut S,
    opts: &CallOptions,
    resolve: impl Fn(&S) -> FrozenRenderConfig,
    mutate: impl FnOnce(&mut S),
) -> Result<(), String> {
    let frozen = resolve(spec);
    let at_run_start = family.render(&frozen, opts);
    mutate(spec);
    // Resume renders from the FROZEN config captured at run start, never the
    // (now-mutated) live spec.
    let on_resume = family.render(&frozen, opts);
    if at_run_start == on_resume {
        Ok(())
    } else {
        Err(
            "a spec mutation after the freeze changed the rendered bytes — a render \
             input leaked past the frozen config"
                .to_string(),
        )
    }
}

/// NORMALIZER-VS-FIXTURE: feed a recorded response stream (split into arbitrary
/// chunks) through the family's normalizer and return the canonical events, so a
/// test can assert them against the expected set. Splitting at byte `chunk_size`
/// also exercises the normalizer's incremental framing (a real network delivers
/// arbitrary chunk boundaries).
pub fn normalize_recorded(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    step_id: StepId,
    recorded: &[u8],
    chunk_size: usize,
) -> Result<Vec<NormalizedEvent>, String> {
    let mut norm = family.normalizer(frozen, step_id);
    let mut out = Vec::new();
    let chunk_size = chunk_size.max(1);
    for chunk in recorded.chunks(chunk_size) {
        let events = norm.push(chunk).map_err(|e| e.to_string())?;
        out.extend(events);
    }
    Ok(out)
}

/// Run `f` with a deliberately mutated process environment (timezone, locale, and
/// an injected variable), restoring the prior values afterward. Used to prove a
/// renderer ignores ambient state.
///
/// Note: env mutation is process-global, so save/restore is only correct while the
/// conformance harness holds its process-global lock. `check_render_determinism`
/// acquires that lock before the baseline render and keeps it through the
/// perturbed render so no other test thread can observe or overwrite these values.
fn with_perturbed_env<T>(f: impl FnOnce() -> T) -> T {
    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, prev) in &self.saved {
                match prev {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    let keys = ["TZ", "LANG", "LC_ALL", "BROCA_CONFORMANCE_PERTURB"];
    let saved = keys
        .iter()
        .map(|k| (*k, std::env::var(k).ok()))
        .collect::<Vec<_>>();
    let _guard = EnvGuard { saved };

    std::env::set_var("TZ", "Pacific/Kiritimati");
    std::env::set_var("LANG", "tr_TR.UTF-8");
    std::env::set_var("LC_ALL", "tr_TR.UTF-8");
    std::env::set_var("BROCA_CONFORMANCE_PERTURB", "1");

    f()
}
