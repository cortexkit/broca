//! Every model production actually used must still resolve for serving.
//!
//! A catalog refresh is a restore from upstream, and upstream is a stale-capable source
//! that retires models on its own schedule. The committed resolution report guards the
//! wire-family decision, but it is PER-PROVIDER: a model vanishing from a provider moves
//! no row in it. That gap is not hypothetical — one refresh removed 319 retired models
//! and nothing failed, which was noticed only because unrelated fixtures happened to pin
//! ids that no longer existed.
//!
//! So this pins the other axis, from evidence rather than opinion. `data/in-use-models.json`
//! is generated from production export facts, and every pair in it must resolve. A refresh
//! that un-serves a model real traffic depends on fails here rather than at someone's next
//! send.
//!
//! RESOLUTION MUST BE COMPOSED THE WAY PRODUCTION COMPOSES IT, and that is why this test
//! lives here rather than in `broca-catalog`. Three separate inputs decide whether an id
//! serves, and missing any one of them reports a healthy model as retired:
//!
//!   - the catalog lookup itself, where an id must be listed exactly (there are no tier
//!     suffixes) and the overlay's `remove` verb suppresses ids still present in the
//!     snapshot;
//!   - the auth method, since a credential selects which family is even eligible;
//!   - `family_override`, derived from that method by `family_for_method`, which is what
//!     actually re-routes chatgpt and antigravity ids to a family the catalog row does
//!     not name.
//!
//! I got this wrong twice while building it. A raw JSON-presence check reported six false
//! positives; adding the auth method alone fixed none of them, because the override is the
//! input that matters and it is composed in the serve layer. Both times the models had run
//! successfully in production minutes earlier, which is the only reason I did not believe
//! the instrument.

use broca_catalog::live::{resolve_provider, AuthMethod, AuthMode, ResolveOptions};
use broca_module_serve::family_for_method;
use serde::Deserialize;

mod common;

#[derive(Deserialize)]
struct InUse {
    generated_at: String,
    pairs: Vec<Pair>,
}

#[derive(Deserialize)]
struct Pair {
    provider: String,
    model: String,
    /// The auth method production used, which is load-bearing rather than descriptive:
    /// `chatgpt` and `antigravity` OVERRIDE the wire family, and their ids resolve only
    /// under their own method. Resolving them with the default options reports them as
    /// unserved — mine did, on models that had run minutes earlier.
    auth_method: Option<String>,
    facts: u64,
    last_used: String,
}

/// How stale the served-set evidence may get before it stops describing the fleet. The
/// manifest's own window is 90 days of traffic; this bounds how old that snapshot may be.
///
/// Measured rather than chosen, because slack here is not caution -- it is the number of
/// models this guard agrees in advance not to cover. The fleet takes on roughly three new
/// (provider, model) pairs a month: over one recent 30-day window, three pairs appeared in
/// traffic that the manifest did not list. At that rate a 120-day bound would leave about a
/// dozen served models unguarded by the time it finally complained, which is a worse state
/// than the one the guard exists to prevent. Thirty days keeps the uncovered set to roughly
/// the turnover of a single month.
///
/// Raising this is the wrong response to a failure. The check is asking for a regenerated
/// manifest, and regenerating is cheap; a larger number buys quiet by widening exactly the
/// blind spot being reported.
const MAX_MANIFEST_AGE_DAYS: i64 = 30;

fn parse_date(text: &str) -> Option<(i64, i64, i64)> {
    let mut parts = text.split('-');
    let y = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let d = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((y, m, d))
}

/// Days between a calendar date and today, via days-from-civil (Howard Hinnant's
/// algorithm). Avoids a chrono dependency in a test that needs one subtraction.
fn days_since((y, m, d): (i64, i64, i64)) -> i64 {
    fn to_days(y: i64, m: i64, d: i64) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let mp = (m + 9) % 12;
        let doy = (153 * mp + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the unix epoch")
        .as_secs() as i64
        / 86_400;
    now - to_days(y, m, d)
}

/// The Fusiform LKG the daemon writes and reads, found through the daemon's own state-root
/// resolver. A copy of that rule here would drift from it, and this guard would then report
/// about a catalog the daemon never serves. Read-only: this opens the operator's catalog on
/// purpose, and never writes under it.
fn live_catalog_path() -> Option<std::path::PathBuf> {
    let root = broca_module_serve::state_root::resolve_state_root()
        .ok()?
        .path;
    let path = root.join("catalog/fusiform-lkg.json");
    path.exists().then_some(path)
}

/// Loads through `CatalogCache::open` -- THE SAME CALL THE DAEMON MAKES AT BOOT -- rather than a
/// test-only installer. A helper that parsed the file its own way could diverge from the loader
/// production uses, and then this guard would report about a catalog nothing serves.
fn install_live_catalog(path: &std::path::Path) {
    let cache = broca_catalog::CatalogCache::open(path);
    if let Some(error) = cache.lkg_load_error() {
        panic!("live catalog at {} failed to load: {error}", path.display());
    }
    broca_catalog::install_global_catalog_cache(std::sync::Arc::new(cache));
}

fn auth_method_of(pair: &Pair) -> Option<AuthMethod> {
    match pair.auth_method.as_deref() {
        Some("apikey") => Some(AuthMethod::ApiKey),
        Some("oauth") => Some(AuthMethod::Oauth),
        Some("antigravity") => Some(AuthMethod::Antigravity),
        Some("chatgpt") => Some(AuthMethod::Chatgpt),
        Some(other) => panic!("manifest carries an unknown auth method: {other}"),
        None => None,
    }
}

#[test]
fn every_model_production_uses_still_resolves() {
    common::install_snapshot();
    let doc: InUse = serde_json::from_str(include_str!("../data/in-use-models.json"))
        .expect("in-use manifest parses");
    assert!(
        !doc.pairs.is_empty(),
        "an empty manifest would make this test vacuous"
    );

    // An empty manifest is refused above. A STALE one is the same defect wearing a
    // plausible face: 39 pairs from three months ago pass forever while covering models
    // nobody runs, and every model that IS running goes unguarded. The guard would report
    // health about a world it no longer describes.
    //
    // The check is against the manifest's own generation date rather than the export lane,
    // because CI has no production database -- a freshness check that cannot run in CI is
    // itself the thing that silently stops happening.
    let generated = parse_date(&doc.generated_at)
        .unwrap_or_else(|| panic!("generated_at is not YYYY-MM-DD: {}", doc.generated_at));
    let age_days = days_since(generated);
    assert!(
        age_days <= MAX_MANIFEST_AGE_DAYS,
        "data/in-use-models.json was generated {age_days} days ago ({}), past the \
         {MAX_MANIFEST_AGE_DAYS}-day limit. Regenerate it from export_facts so this guard \
         covers the models production runs NOW, then re-run. Do not raise the limit to make \
         this pass -- the limit is what keeps the guard describing the current fleet.",
        doc.generated_at
    );

    // RESOLVE AGAINST THE CATALOG PRODUCTION ACTUALLY SERVES FROM, or say plainly that we did
    // not.
    //
    // This guard used to install the vendored `models.dev.json` snapshot. That was correct until
    // the Fusiform cutover and silently wrong afterwards: the snapshot became a TEST FIXTURE, and
    // the live catalog moved to a Fusiform LKG on disk. On 2026-09-10 it reported three models as
    // "no longer resolve for serving" -- xai/grok-4.6 among them, 712 facts, LAST USED THAT DAY --
    // while all three carried `existence: present` in the live catalog and were serving fine.
    //
    // A FALSE ALARM IS THE EXPENSIVE FAILURE HERE, not a missed one. The assertion's own text
    // invites "drop it from data/in-use-models.json", so the cheapest way to make it green is to
    // delete a live model from the manifest -- laundering a serving model as retired, and removing
    // it from the guard's coverage permanently. The next person to hit this would have no reason
    // to doubt the instruction.
    //
    // Refreshing the fixture instead would reset a treadmill: the snapshot drifts from the live
    // catalog continuously, so the same false alarm returns on whatever cadence the fixture is
    // regenerated.
    //
    // So the guard now asks its question of the live catalog, and where there is no live catalog
    // -- CI, a fresh checkout -- it SKIPS WITH A NOTICE NAMING THE VARIABLE rather than answering
    // from a fixture. `BROCA_REQUIRE_CATALOG=1` turns the skip into a refusal, which is what the
    // deploy path sets, matching BROCA_REQUIRE_SUBC/VAULT/AIMOCK elsewhere in this suite.
    let Some(catalog_path) = live_catalog_path() else {
        let required = std::env::var("BROCA_REQUIRE_CATALOG").is_ok_and(|v| v == "1");
        assert!(
            !required,
            "BROCA_REQUIRE_CATALOG=1 but no Fusiform LKG was found under broca's state root \
             ($BROCA_STATE_ROOT, else $XDG_DATA_HOME or ~/.local/share, then \
             cortexkit/broca/catalog/fusiform-lkg.json). This guard cannot answer \
             its question without the catalog production serves from."
        );
        eprintln!(
            "SKIP every_model_production_uses_still_resolves: no Fusiform LKG on this machine, \
             so the live catalog is unavailable. The manifest freshness assertion above DID run. \
             Set BROCA_REQUIRE_CATALOG=1 where a catalog exists (the deploy path) to make this a \
             refusal instead of a skip."
        );
        return;
    };
    install_live_catalog(&catalog_path);

    let mut unserved = Vec::new();
    let mut still_served_but_retired = Vec::new();
    for pair in &doc.pairs {
        let handle = format!("{}/{}", pair.provider, pair.model);
        let retired_by_decision = RETIRED_BY_DECISION.contains(&handle.as_str());
        // A dummy bearer keeps this offline: resolution is a catalog decision, and the
        // credential is only shaped, never used. Failure here means the id no longer
        // resolves, not that the token is wrong.
        let method = auth_method_of(pair);
        let opts = ResolveOptions {
            auth_method: method,
            // The same derivation the serve path performs, from the same authority, so a
            // change to `family_for_method` moves this guard with it.
            family_override: method.and_then(family_for_method),
            ..ResolveOptions::default()
        };
        let resolved = resolve_provider(
            &handle,
            None,
            2048,
            &AuthMode::Bearer("dummy-token".into()),
            &opts,
        );
        // Only catalog retirement counts here. Resolution also shapes the credential, and
        // a dummy token legitimately fails that step -- the ChatGPT path, for instance,
        // reads a `chatgpt_account_id` claim that a placeholder does not carry. Treating
        // that as "model retired" would make this test fail for a reason it does not
        // cover, on models serving live traffic right now. The retirement signal is the
        // catalog's own refusal, which names the handle and says the model is not served.
        if retired_by_decision {
            // These must keep refusing: resolving again would mean the retirement was undone.
            if resolved.is_ok() {
                still_served_but_retired.push(handle);
            }
            continue;
        }
        if let Err(error) = resolved {
            // Three refusal arms name a model that will not serve, and all three count: the
            // unknown-id arm, the overlay-retirement arm, and the UPSTREAM-WITHDRAWAL arm.
            // This matched only the first until 2026-09-18, when models.dev withdrew
            // kimi-for-coding (739 facts, last used a week earlier): the refusal read "was
            // withdrawn upstream", the string here did not match, and the guard passed a
            // production model that had stopped serving -- blind to the most common way a
            // model stops serving, which is not a broca swap at all.
            let retired = error.to_string().contains("catalog has no served model")
                || error.to_string().contains("was withdrawn upstream")
                || error
                    .to_string()
                    .contains("is retired by broca's own overlay");
            if retired {
                unserved.push(format!(
                    "{handle} ({} facts, last used {}): {error}",
                    pair.facts, pair.last_used
                ));
            }
        }
    }

    assert!(
        unserved.is_empty(),
        "these models carry production traffic but no longer resolve for serving:\n  {}\n\n\
         If a provider genuinely retired one, drop it from data/in-use-models.json in the \
         same commit and say why. Do not regenerate the manifest to make this pass — the \
         manifest is the evidence, and regenerating it deletes the evidence.",
        unserved.join("\n  ")
    );
    assert!(
        still_served_but_retired.is_empty(),
        "these ids were retired on purpose but resolve again: {still_served_but_retired:?}"
    );
}

/// Ids production sent that broca stopped serving ON PURPOSE, not because a catalog lost them.
///
/// `gpt-5.6-luna-pro`, `gpt-5.6-terra-pro`, `gpt-6-luna-pro` and `gpt-6-luna-fast` were never
/// catalog models: broca mapped the `-pro` suffix to the base model with reasoning effort `max`,
/// and `-fast` to priority processing, on the ChatGPT path. Effort is now requested as
/// `model.variant` and priority as `service_tier`, and an id resolves only if the catalog lists
/// it, so these are refused as unknown. They stay in the manifest because the manifest is the record
/// of what was sent; this list is the record of the decision to stop serving them, and the test
/// checks that they really do refuse.
///
/// `antigravity-gemini-3.7-flash` was withdrawn upstream with `antigravity-gemini-3.8-flash` as
/// its named successor; broca follows Fusiform's retirement facts and refuses it by name.
const RETIRED_BY_DECISION: &[&str] = &[
    "openai/gpt-5.6-luna-pro",
    "openai/gpt-5.6-terra-pro",
    "openai/gpt-6-luna-pro",
    "openai/gpt-6-luna-fast",
    "google/antigravity-gemini-3.7-flash",
];
