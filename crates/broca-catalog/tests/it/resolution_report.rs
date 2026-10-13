//! Pins the per-provider resolution report against the committed artifact
//! (`data/resolution-report.json`). This is the structural guardrail against a
//! silent family re-route: if a catalog refresh changes ANY provider's resolution
//! row, this test fails until the committed report is regenerated and the diff
//! reviewed. Regenerate with `BROCA_UPDATE_REPORT=1` (the refresh script does this).
#![cfg(feature = "bundled-catalog")]

use broca_catalog::{Catalog, ResolutionRow};

const REPORT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data/resolution-report.json");

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone)]
struct ReportRow {
    provider_id: String,
    npm: Option<String>,
    resolved_family: Option<String>,
    resolution_source: String,
    pending_reason: String,
}

impl From<&ResolutionRow> for ReportRow {
    fn from(r: &ResolutionRow) -> Self {
        ReportRow {
            provider_id: r.provider_id.clone(),
            npm: r.npm.clone(),
            resolved_family: r.resolved_family.clone(),
            resolution_source: r.resolution_source.clone(),
            pending_reason: r.pending_reason.clone(),
        }
    }
}

fn current_rows() -> Vec<ReportRow> {
    Catalog::from_json_str(
        include_str!("../../data/models.dev.json"),
        include_str!("../../data/overlay.json"),
    )
    .expect("legacy report fixture is well formed")
    .resolution_report()
    .iter()
    .map(ReportRow::from)
    .collect()
}

fn to_json(rows: &[ReportRow]) -> String {
    let mut s = serde_json::to_string_pretty(rows).unwrap();
    s.push('\n');
    s
}

/// A one-line summary of a row, for a focused diff in the failure message.
fn line(r: &ReportRow) -> String {
    let fam = r.resolved_family.as_deref().unwrap_or("<pending>");
    format!(
        "{} → {} (source={})",
        r.provider_id, fam, r.resolution_source
    )
}

#[test]
fn resolution_report_matches_committed_artifact() {
    let current = current_rows();

    if std::env::var("BROCA_UPDATE_REPORT").is_ok() {
        std::fs::write(REPORT_PATH, to_json(&current)).expect("write report artifact");
        eprintln!("updated {REPORT_PATH}");
        return;
    }

    let committed_json = std::fs::read_to_string(REPORT_PATH).unwrap_or_default();
    let committed: Vec<ReportRow> = serde_json::from_str(&committed_json).unwrap_or_default();

    if current == committed {
        return;
    }

    // Report ONLY the providers that actually differ — a 144-row dump would bury the
    // signal. This is the reviewable line the guardrail exists to surface.
    use std::collections::BTreeMap;
    let by_id = |rows: &[ReportRow]| -> BTreeMap<String, ReportRow> {
        rows.iter()
            .map(|r| (r.provider_id.clone(), r.clone()))
            .collect()
    };
    let cur = by_id(&current);
    let com = by_id(&committed);
    let mut diffs = Vec::new();
    for (id, c) in &cur {
        match com.get(id) {
            Some(old) if old == c => {}
            Some(old) => diffs.push(format!("  CHANGED {}  (was: {})", line(c), line(old))),
            None => diffs.push(format!("  ADDED   {}", line(c))),
        }
    }
    for (id, old) in &com {
        if !cur.contains_key(id) {
            diffs.push(format!("  REMOVED {}", line(old)));
        }
    }
    diffs.sort();

    panic!(
        "the per-provider resolution report changed vs the committed artifact — a \
         catalog refresh re-routed at least one provider's wire family. Review these \
         changes, then regenerate with `scripts/refresh-catalog.sh` (or \
         BROCA_UPDATE_REPORT=1). This guard makes a silent family re-route impossible.\n{}",
        diffs.join("\n")
    );
}
