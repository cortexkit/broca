//! The billing differential: a log's spend facts as today's builder makes them from the log,
//! against the facts the new storage's billing path (`broca_session_loop::billing`) makes from
//! the converted session, compared field by field, fact ids included.
//!
//! Today's builder reads a record's seq only as a warm fact's segment ordinal and in its fact id.
//! A session in the new storage numbers its records itself, so a converted warm takes another
//! seq than the one today's log gave it. Today's side is therefore built over the log with each
//! cache warm renumbered by the seq its converted `Warmed` record took; every other record keeps
//! its seq, which today's builder doesn't read. How many warm facts that moved is counted.

use std::collections::BTreeMap;
use std::path::PathBuf;

use broca_core::billing::{compute_segments_for_run, ChargeBasis, SpendSegment};
use broca_protocol::{BindIdentity, RunId};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::head::SessionTriple;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use serde::Serialize;
use serde_json::Value;

/// The source instance both sides' facts are keyed under.
pub const SOURCE: &str = "billing-differential";

/// A charge-basis resolver that answers each provider and auth method differently, so a fact
/// resolved under the wrong provider or method shows as a difference.
pub fn resolver(provider: &str, method: &str) -> (ChargeBasis, u32) {
    let basis = match (provider.len() + 3 * method.len()) % 3 {
        0 => ChargeBasis::MeteredApi,
        1 => ChargeBasis::SubscriptionIncluded,
        _ => ChargeBasis::Unknown,
    };
    (basis, (provider.len() * 7 + method.len()) as u32)
}

/// The session identity a log's runs were started under: the first run start's, which every run
/// of a session repeats.
pub fn log_session(old: &[FramedRecord]) -> Option<BindIdentity> {
    old.iter().find_map(|framed| match &framed.record {
        WalRecord::RunStarted { session, .. } => Some(session.clone()),
        _ => None,
    })
}

/// The triple a session in the new storage is created under for `bind`, as the module creates it.
pub fn triple(bind: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: bind.project_root.display().to_string(),
        harness: bind.harness.clone(),
        session: bind.session.clone(),
    }
}

/// The bind identity the store actor bills a session with, read back from its triple.
pub fn bind_of(triple: &SessionTriple) -> BindIdentity {
    BindIdentity::new(
        PathBuf::from(&triple.project_root),
        triple.harness.clone(),
        triple.session.clone(),
    )
}

/// Today's log with each cache warm renumbered by `warm_seqs`, the seqs the converted `Warmed`
/// records took, in order.
pub fn warms_at(old: &[FramedRecord], warm_seqs: &[u64]) -> Result<Vec<FramedRecord>, String> {
    let mut seqs = warm_seqs.iter();
    let renumbered = old
        .iter()
        .map(|framed| match framed.record {
            WalRecord::CacheWarmed { .. } => seqs
                .next()
                .map(|seq| FramedRecord {
                    seq: *seq,
                    ..framed.clone()
                })
                .ok_or_else(|| "a cache warm has no converted record".to_string()),
            _ => Ok(framed.clone()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if seqs.next().is_some() {
        return Err("a converted warm has no cache warm in the log".into());
    }
    Ok(renumbered)
}

/// What one comparison found.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Comparison {
    /// Runs compared: every run a run start names, once.
    pub runs: u64,
    pub facts_today: u64,
    pub facts_new: u64,
    /// Facts equal field for field, fact id included.
    pub facts_equal: u64,
    /// Of today's facts, the warm ones, whose ordinal and id are taken over the store's seq.
    pub warm_facts: u64,
    /// Of those, the ones whose ordinal moved from the log's seq.
    pub warm_moved: u64,
    /// Differing facts by field name, a count of facts per field. A run whose fact count
    /// differs counts under `fact count`; a run the new path could not bill under `unbilled`.
    pub differences: BTreeMap<String, u64>,
    /// The runs with any difference, in this comparison only; a report lists them per log.
    #[serde(skip)]
    pub differing_runs: Vec<String>,
    /// Why runs couldn't be billed by the new path, by kind of failure (never by content).
    pub unbilled: BTreeMap<String, u64>,
}

impl Comparison {
    pub fn add(&mut self, other: &Comparison) {
        self.runs += other.runs;
        self.facts_today += other.facts_today;
        self.facts_new += other.facts_new;
        self.facts_equal += other.facts_equal;
        self.warm_facts += other.warm_facts;
        self.warm_moved += other.warm_moved;
        for (field, count) in &other.differences {
            *self.differences.entry(field.clone()).or_default() += count;
        }
        for (reason, count) in &other.unbilled {
            *self.unbilled.entry(reason.clone()).or_default() += count;
        }
    }
}

/// Why the new path could not bill a run, as a kind of failure: the rebuilt record's type when
/// it doesn't read as today's, never the message, which can quote a value.
fn failure_kind(error: &str) -> String {
    if let Some(rest) = error.strip_prefix("the rebuilt ") {
        let kind = rest.split_whitespace().next().unwrap_or("?");
        return format!("a rebuilt {kind} doesn't read as today's record");
    }
    for (needle, kind) in [
        ("holds no start", "no start of the run"),
        (
            "without its assistant message",
            "a step end without its assistant message",
        ),
        ("setup part", "a setup part could not be read"),
        ("could not be found", "the id index could not be read"),
        ("could not be read", "the run's records could not be read"),
    ] {
        if error.contains(needle) {
            return kind.into();
        }
    }
    "other".into()
}

/// The fields of `a` and `b` that differ, by name.
fn differing_fields(a: &SpendSegment, b: &SpendSegment) -> Vec<String> {
    let as_object = |segment: &SpendSegment| match serde_json::to_value(segment) {
        Ok(Value::Object(object)) => object,
        _ => serde_json::Map::new(),
    };
    let (a, b) = (as_object(a), as_object(b));
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|key| a.get(*key) != b.get(*key))
        .cloned()
        .collect()
}

/// Every run today's log starts, in the order each first starts.
pub fn run_ids(old: &[FramedRecord]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    old.iter()
        .filter_map(|framed| match &framed.record {
            WalRecord::RunStarted { run_id, .. } if seen.insert(run_id.0.clone()) => {
                Some(run_id.0.clone())
            }
            _ => None,
        })
        .collect()
}

/// Counts one more fact differing in `field`.
fn bump(differences: &mut BTreeMap<String, u64>, field: &str) {
    *differences.entry(field.to_owned()).or_default() += 1;
}

/// Compares, for every run of `old`, today's facts over `today` (the log with its warms
/// renumbered) with the facts the store's billing path makes from `session`.
pub async fn compare<B: SessionBackend>(
    old: &[FramedRecord],
    today: &[FramedRecord],
    session: &Session<B>,
    bind: &BindIdentity,
    parts: &dyn Parts,
) -> Comparison {
    let mut out = Comparison::default();
    for run_id in run_ids(old) {
        out.runs += 1;
        let run = RunId(run_id.clone());
        let expected = compute_segments_for_run(today, &run, SOURCE, &resolver);
        let original = compute_segments_for_run(old, &run, SOURCE, &resolver);
        out.facts_today += expected.len() as u64;
        for (renumbered, logged) in expected.iter().zip(&original) {
            if renumbered.warm {
                out.warm_facts += 1;
                out.warm_moved += u64::from(renumbered.segment_ordinal != logged.segment_ordinal);
            }
        }
        let got =
            match broca_session_loop::billing::run_records(session, &run_id, bind, parts).await {
                Ok(records) => compute_segments_for_run(&records, &run, SOURCE, &resolver),
                Err(error) => {
                    bump(&mut out.differences, "unbilled");
                    bump(&mut out.unbilled, &failure_kind(&error));
                    out.differing_runs.push(run_id);
                    continue;
                }
            };
        out.facts_new += got.len() as u64;
        let mut run_differs = got.len() != expected.len();
        if run_differs {
            bump(&mut out.differences, "fact count");
        }
        for (a, b) in expected.iter().zip(&got) {
            let fields = differing_fields(a, b);
            if fields.is_empty() {
                out.facts_equal += 1;
            }
            for field in fields {
                run_differs = true;
                bump(&mut out.differences, &field);
            }
        }
        if run_differs {
            out.differing_runs.push(run_id);
        }
    }
    out
}

#[cfg(test)]
mod tests;
