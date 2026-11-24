//! Readers for the reasoning facts of a fusiform row: whether the model reasons, the variants
//! it offers, and the effort its vendor uses when a request names none.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use broca_provider::spec::{CapabilityFlag, ReasoningOptions};
use serde_json::Value;

use super::{optional_capability_fact, value_kind};
use crate::CatalogError;

/// `capability.reasoning`: an omitted fact reads as [`CapabilityFlag::Unknown`], exactly like
/// an explicit null, never as `Absent`.
///
/// The difference matters because `CapabilityFlag::admit` (called by `resolve_frozen`) admits
/// `Absent` as `false` and freezes `ReasoningPolicy::None`. A reasoning model whose flag
/// fusiform stopped serving would then run without reasoning and nothing would report it:
/// "could not tell" would have become a known `false`. As `Unknown`, a send that would reason
/// (the family's reasoning policy is not `None`) is refused at admission, as it is for null,
/// and a send that would not reason admits.
///
/// The mapping lives here rather than in `admit` because `Absent` as `false` is still the
/// documented reading for the vendored snapshot catalog. Excluding the row instead would also
/// lose rows that are servable as they are: the curated decision models (clef, clairic, jev)
/// state no reasoning flag and are served on families that never reason.
pub(super) fn reasoning_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<CapabilityFlag, CatalogError> {
    Ok(
        match optional_capability_fact(identity, facts, "capability.reasoning")? {
            CapabilityFlag::Absent => CapabilityFlag::Unknown,
            stated => stated,
        },
    )
}

/// `capability.reasoning_options`, served verbatim from models.dev. Unlike the capability flags
/// this fact is optional: a fusiform from before it existed does not serve it, and that reads
/// as unstated (no variants) rather than refusing the whole catalog.
pub(super) fn reasoning_options_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Result<ReasoningOptions, CatalogError> {
    match facts.get("capability.reasoning_options") {
        None => Ok(ReasoningOptions::Unstated),
        Some(value) => ReasoningOptions::from_value(value)
            .map(shared_options)
            .ok_or_else(|| CatalogError::FusiformFact {
                model: identity.to_string(),
                fact: "capability.reasoning_options".to_string(),
                reason: format!("expected array or null, found {}", value_kind(value)),
            }),
    }
}

/// The parsed option lists rows currently hold, by their JSON text. A catalog serves the same
/// few lists on thousands of rows (183 distinct lists on 9,089 rows at one count), and a parsed
/// list is a tree of JSON values, so rows with the same list share one copy, also across a
/// refresh. A list lives only as long as some row holds it.
static SHARED_OPTIONS: LazyLock<Mutex<HashMap<String, Weak<[Value]>>>> =
    LazyLock::new(Default::default);

/// `options`, sharing the parsed list of any row that already holds the same one.
fn shared_options(options: ReasoningOptions) -> ReasoningOptions {
    let ReasoningOptions::Entries(entries) = options else {
        return options;
    };
    let Ok(text) = serde_json::to_string(&*entries) else {
        return ReasoningOptions::Entries(entries);
    };
    let mut shared = SHARED_OPTIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(held) = shared.get(&text).and_then(Weak::upgrade) {
        return ReasoningOptions::Entries(held);
    }
    // Forget the lists no row holds any more before the map outgrows the live ones.
    if shared.len() >= 1024 {
        shared.retain(|_, list| list.strong_count() > 0);
    }
    shared.insert(text, Arc::downgrade(&entries));
    ReasoningOptions::Entries(entries)
}

/// `capability.reasoning_default`: `{type, value}`, one entry's type and one of its values. Only
/// an `effort` default is read, and only when the row's own `effort` option offers its value; any
/// other default, an absent one or a malformed one reads as `None` and the row still serves, so
/// a run that names no variant then sends no effort.
pub(super) fn reasoning_default_effort_fact(
    identity: &str,
    facts: &BTreeMap<String, Value>,
) -> Option<String> {
    const FACT: &str = "capability.reasoning_default";
    let value = facts.get(FACT)?;
    if value.is_null() {
        return None;
    }
    let effort = (value.get("type").and_then(Value::as_str) == Some("effort"))
        .then(|| value.get("value").and_then(Value::as_str))
        .flatten();
    let offered = facts
        .get("capability.reasoning_options")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|entry| entry.get("type").and_then(Value::as_str) == Some("effort"))
        .filter_map(|entry| entry.get("values").and_then(Value::as_array))
        .flatten()
        .any(|offered| offered.as_str().is_some() && offered.as_str() == effort);
    match effort {
        Some(effort) if offered => Some(effort.to_owned()),
        // Another type of default broca doesn't send: nothing to say.
        _ if value.get("type").and_then(Value::as_str) != Some("effort") => None,
        _ => {
            tracing::warn!(
                target: "catalog",
                model = identity,
                fact = FACT,
                value = %value,
                "an effort default its own options don't offer is read as unstated; the row \
                 still serves"
            );
            None
        }
    }
}
