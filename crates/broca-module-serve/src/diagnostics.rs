//! Read-only self-diagnosis from bounded index reads and content-free process evidence.
use broca_module::diagnostics::{self as live, Event, Fence, Snapshot};
use broca_protocol::{BindPrincipal, ScopeKind, SendScope};
use broca_store::diagnostics::{Evidence as Runs, RunEvidence, SCAN_LIMIT};
use broca_wire::diagnostics::{Coverage, Finding, Reply, Request, Severity, DEFAULT_WINDOW_MS};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use subc_protocol::ErrorBody;

pub use broca_wire::diagnostics::OP;
pub const REPLY_LIMIT: usize = 65_536;
static ROOT: OnceLock<PathBuf> = OnceLock::new();

pub fn install(root: PathBuf) {
    live::initialize();
    let _ = ROOT.set(root);
}

pub fn admit(principal: Option<&BindPrincipal>, scope: &SendScope) -> Result<(), ErrorBody> {
    if let SendScope::Stamped(stamp) = scope {
        if stamp.scope.flow_id.is_some() {
            if stamp.kind != ScopeKind::Worker
                && stamp.owner_authorized
                && matches!(principal, Some(BindPrincipal::Reserved { .. }))
            {
                return Ok(());
            }
        } else if stamp.kind == ScopeKind::Head
            && stamp.scope.run_id.is_none()
            && principal == Some(&stamp.scope.owner)
        {
            return Ok(());
        }
    }
    Err(ErrorBody::new("permission_denied", format!(
        "diagnostics.findings refuses {}: only a current head-owner route or owner-authorized flow route may read broca diagnostics",
        principal.map(BindPrincipal::as_owner_string).unwrap_or_else(|| "unstamped principal".into())
    )))
}

pub struct Evidence {
    pub runs: Runs,
    pub late_fence_ms: Option<i64>,
    pub retention: Result<Vec<Fence>, String>,
    pub catalog_age_ms: Result<i64, String>,
    pub unmapped_credentials: u64,
    pub rings: BTreeMap<String, Result<Snapshot, String>>,
}

pub fn gather(
    root: &Path,
    catalog_age_ms: Result<i64, String>,
    since: i64,
    now: i64,
) -> Result<Evidence, ErrorBody> {
    let store =
        broca_store::SqliteStore::open_read_only(root.join("run-index.db")).map_err(|_| {
            ErrorBody::new(
                "store_error",
                "diagnostics.findings cannot read run-index.db",
            )
        })?;
    let runs = store.diagnostic_evidence(since, now).map_err(|_| {
        ErrorBody::new(
            "store_error",
            "diagnostics.findings cannot read indexed run evidence",
        )
    })?;
    let late_fence_ms = match std::fs::metadata(root.join("late-results-rebuild-required")) {
        Ok(meta) => Some(
            meta.modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .and_then(|d| i64::try_from(d.as_millis()).ok())
                .unwrap_or(now),
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => {
            return Err(ErrorBody::new(
                "store_error",
                "diagnostics.findings cannot inspect the late-results rebuild fence",
            ))
        }
    };
    let metrics = crate::account_routing::published_metrics();
    Ok(Evidence {
        runs,
        late_fence_ms,
        catalog_age_ms,
        retention: live::retention_fences(),
        unmapped_credentials: metrics["unmapped_credentials"].as_u64().unwrap_or(0),
        rings: [
            (
                "send_refusals".into(),
                live::SEND_REFUSALS.snapshot(since, now),
            ),
            (
                "catalog_stale".into(),
                live::CATALOG_REFUSALS.snapshot(since, now),
            ),
            (
                "tool_outages".into(),
                live::TOOL_OUTAGES.snapshot(since, now),
            ),
        ]
        .into(),
    })
}

/// Admission precedes the evidence closure, including parameter validation and root lookup.
/// A refused route cannot learn whether any of the evidence sources exist.
pub fn answer(
    params: Value,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    now: i64,
    read: impl FnOnce(i64) -> Result<Evidence, ErrorBody>,
) -> Result<Reply, ErrorBody> {
    admit(principal, scope)?;
    let window =
        Request::window(params).map_err(|message| ErrorBody::new("invalid_params", message))?;
    let evidence = read(now.saturating_sub(window))?;
    Ok(report(evidence, now, window))
}

pub fn serving_reply(
    params: Value,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    catalog_age: Result<i64, String>,
    now: i64,
) -> Result<Reply, ErrorBody> {
    answer(params, principal, scope, now, |since| {
        let root = ROOT.get().ok_or_else(|| {
            ErrorBody::new(
                "store_error",
                "diagnostics.findings run-index source is not configured",
            )
        })?;
        gather(root, catalog_age, since, now)
    })
}

fn finding(
    check: &str,
    key: String,
    severity: Severity,
    first: i64,
    last: i64,
    summary: &str,
    hint: &str,
) -> Finding {
    Finding {
        check: check.into(),
        key,
        severity,
        count: 1,
        first_seen_ms: first,
        last_seen_ms: last,
        summary: summary.into(),
        hint: hint.into(),
        samples: Vec::new(),
    }
}

fn unavailable(check: &str, now: i64) -> Finding {
    let mut f = finding(check, "evidence_unavailable".into(), Severity::Error, now, now,
        "Broca could not read the evidence needed to run this check.",
        "Inspect health.check and the broca daemon log for an unavailable diagnostic evidence source.");
    f.samples.push(json!({"code":"evidence_unavailable"}));
    f
}

fn group(
    rows: &[RunEvidence],
    check: &str,
    now: i64,
    key: impl Fn(&RunEvidence) -> String,
    hint: &str,
) -> Vec<Finding> {
    let mut groups = BTreeMap::<String, Finding>::new();
    for row in rows.iter().take(SCAN_LIMIT) {
        let key = key(row);
        let at = row.at_ms.unwrap_or(now);
        let f = groups.entry(key.clone()).or_insert_with(|| {
            let mut f = finding(check, key, Severity::Warn, at, at, "", hint);
            f.count = 0;
            f
        });
        f.count += 1;
        f.first_seen_ms = f.first_seen_ms.min(at);
        f.last_seen_ms = f.last_seen_ms.max(at);
        if f.samples.len() < 5 {
            f.samples.push(json!({"run_id":row.run_id,"at_ms":row.at_ms,
                "provider_module_id":row.provider,"credential_id":row.credential,
                "error_class":row.class,"error_code":row.code,"interrupt_reason":row.interrupt_reason}));
        }
    }
    groups.into_values().collect()
}

// Calibration from the operator's 2026-10-28 seven-day session-log aggregate (1,359 error
// runs), grouped by provider/class/code in six-hour windows: transient rate_limited median
// 60/p90 322; permanent request_invalid p90 9/max 93; context_overflow max 10.
// Floors suppress ordinary scattered errors; credential/billing failures always need action.
const ERROR_FLOORS: [(&str, u64); 4] = [
    ("credential", 1),
    ("permanent", 10),
    ("context_overflow", 25),
    ("transient", 50),
];
fn error_floor(class: &str, code: &str, window: i64) -> (u64, Severity) {
    let credential = class == "auth_required"
        || matches!(
            code,
            "billing_exhausted"
                | "credit_balance_exhausted"
                | "ENTITLEMENT_ERROR"
                | "upstream_api_key_rejected"
                | "invalid_api_key"
                | "incorrect_api_key"
                | "credit_insufficient"
                | "organization_spend_limit_exceeded"
                | "project_spend_limit_exceeded"
                | "usage_not_included"
                | "websocket_auth_rejected"
        );
    let kind = if credential {
        "credential"
    } else if class == "permanent" {
        "permanent"
    } else if class == "context_overflow" {
        "context_overflow"
    } else {
        "transient"
    };
    let floor = ERROR_FLOORS
        .iter()
        .find(|(name, _)| *name == kind)
        .unwrap()
        .1;
    let floor = if credential {
        1
    } else if window == DEFAULT_WINDOW_MS {
        floor
    } else {
        (floor * window as u64)
            .div_ceil(DEFAULT_WINDOW_MS as u64)
            .max(3)
    };
    (
        floor,
        if credential || class == "permanent" {
            Severity::Error
        } else {
            Severity::Warn
        },
    )
}

pub fn report(mut evidence: Evidence, now: i64, window: i64) -> Reply {
    let mut findings = Vec::new();
    let mut coverage = BTreeMap::new();
    let live_truncated = evidence.runs.live.len() > SCAN_LIMIT;
    let paused: Vec<_> = evidence
        .runs
        .live
        .iter()
        .take(SCAN_LIMIT)
        .filter(|r| {
            r.state == "paused"
                && (r.pause_reason.as_deref() == Some("auth_required")
                    || r.code.as_deref() == Some("account_limit_exhausted")
                    || r.pause_reason.is_none())
        })
        .cloned()
        .collect();
    for mut f in group(&paused, "paused_runs", now, |r| {
        if r.pause_reason.is_none() { "unattributed".into() }
        else { json!([r.provider,r.credential]).to_string() }
    }, "Use broca-session status for a sampled run_id; inspect paused:auth_required and its frozen credential id.") {
        if now.saturating_sub(f.first_seen_ms) > 86_400_000 { f.severity = Severity::Error; }
        f.summary = if f.key == "unattributed" {
            format!("{} indexed paused runs lack pause attribution, so broca cannot determine whether authentication or account availability blocks them.", f.count)
        } else {
            format!("{} runs remain paused for authentication or account availability.", f.count)
        };
        findings.push(f);
    }
    if live_truncated {
        findings.push(unavailable("paused_runs", now));
    }
    let errors_truncated = evidence.runs.errors.len() > SCAN_LIMIT;
    for mut f in group(&evidence.runs.errors,"run_errors",now,|r| {
        if r.provider.is_none() || r.class.is_none() { "unattributed".into() }
        else { json!([r.provider,r.class,r.code]).to_string() }
    }, "Use broca-session status for a sampled run_id; inspect its error class and provider code, not provider message text.") {
        let sample = &f.samples[0];
        let (floor,severity) = error_floor(sample["error_class"].as_str().unwrap_or("unattributed"),sample["error_code"].as_str().unwrap_or(""),window);
        if f.count < floor && f.key != "unattributed" { continue; }
        f.severity = severity;
        f.summary = format!("{} runs ended in error in the indexed window{}.",f.count,
            if errors_truncated { "; only the newest 20000 error rows were examined" } else { "" });
        if let Some(sample) = f.samples.first_mut() {
            sample["rows_without_attribution"] = json!(if f.key == "unattributed" { f.count } else { 0 });
        }
        findings.push(f);
    }
    let attributed_since = evidence
        .runs
        .errors
        .iter()
        .take(SCAN_LIMIT)
        .filter(|r| r.provider.is_some() && r.class.is_some())
        .filter_map(|r| r.at_ms)
        .min()
        .unwrap_or(now);
    coverage.insert(
        "run_errors".into(),
        Coverage {
            since_ms: attributed_since,
            truncated: errors_truncated,
        },
    );
    let interruptions: Vec<_> = evidence
        .runs
        .interruptions
        .iter()
        .take(SCAN_LIMIT)
        .filter(|r| {
            r.state == "transform_unavailable"
                || !matches!(
                    r.interrupt_reason.as_deref(),
                    Some("runner_shutdown" | "runner_crash")
                )
        })
        .cloned()
        .collect();
    for mut f in group(&interruptions,"interrupted_runs",now,|r| {
        if r.state == "transform_unavailable" { r.state.clone() }
        else { r.interrupt_reason.clone().unwrap_or_else(|| "unattributed".into()) }
    }, "Use broca-session status for the sampled run_id and inspect the broca lifecycle log; unknown causes cannot be classified as restart seals.") {
        f.summary = format!("{} runs ended interrupted outside an attributed restart seal or could not reach the transform{}.",f.count,
            if evidence.runs.interruptions.len() > SCAN_LIMIT { "; only the newest 20000 interruption rows were examined" } else { "" });
        findings.push(f);
    }
    coverage.insert(
        "interrupted_runs".into(),
        Coverage {
            since_ms: evidence
                .runs
                .interruptions
                .iter()
                .take(SCAN_LIMIT)
                .filter_map(|r| r.at_ms)
                .min()
                .unwrap_or(now),
            truncated: evidence.runs.interruptions.len() > SCAN_LIMIT,
        },
    );
    for (check, rows) in [
        ("run_errors", &evidence.runs.untimed_errors),
        ("interrupted_runs", &evidence.runs.untimed_interruptions),
    ] {
        if !rows.is_empty() {
            let mut f = unavailable(check, now);
            f.key = "unknown_observation_time".into();
            f.count = rows.len().min(SCAN_LIMIT) as u64;
            f.summary = "Legacy index rows have no observation time, so broca cannot place them in the requested window.".into();
            if rows.len() > SCAN_LIMIT {
                f.summary
                    .push_str(" Only the first 20000 untimed rows were examined.");
            }
            if let Some(sample) = f.samples.first_mut() {
                sample["scan_limit"] = json!(SCAN_LIMIT);
                sample["truncated"] = json!(rows.len() > SCAN_LIMIT);
            }
            if let Some(coverage) = coverage.get_mut(check) {
                coverage.truncated |= rows.len() > SCAN_LIMIT;
            }
            findings.push(f);
        }
    }
    match evidence.retention {
        Ok(fences) => {
            let mut grouped = BTreeMap::<String, Finding>::new();
            for fence in fences {
                let f = grouped.entry(fence.kind.clone()).or_insert_with(|| {
                    let mut f = finding("deletion_fenced",fence.kind.clone(),Severity::Warn,fence.first_ms,now,
                        "Retention deletion remains refused or deferred for these sessions.",
                        "Inspect the retention log for the sampled session address and fence code; health.check exposes retention backlog counters.");
                    f.count = 0; f
                });
                f.count += 1;
                f.first_seen_ms = f.first_seen_ms.min(fence.first_ms);
                if f.samples.len() < 5 {
                    f.samples.push(json!({"session_addr":fence.session,"code":fence.kind,"at_ms":fence.last_ms}));
                }
            }
            findings.extend(grouped.into_values());
        }
        Err(_) => findings.push(unavailable("deletion_fenced", now)),
    }
    if let Some(first) = evidence.late_fence_ms {
        findings.push(finding("late_results_rebuild_required","late-results-rebuild-required".into(),Severity::Error,first,now,
            "The late-results rebuild fence is present, so provider-held intake cannot be trusted.",
            "Inspect the late-results-rebuild-required marker and late-results rebuild log before removing the fence."));
    }
    if evidence.unmapped_credentials > 0 {
        let mut f = finding("credential_metadata_missing","vault_provider_metadata".into(),Severity::Warn,now,now,
            "Listed model credentials lack the provider metadata required for routed account selection.",
            "Inspect health.check account_selection.unmapped_credentials and the account-selection unmapped credential warning.");
        f.count = evidence.unmapped_credentials;
        findings.push(f);
    }
    let catalog_events = evidence
        .rings
        .get("catalog_stale")
        .and_then(|r| r.as_ref().ok());
    match evidence.catalog_age_ms {
        Ok(age) if age > crate::CATALOG_POLL_INTERVAL.as_millis() as i64 => {
            let first = now.saturating_sub(age);
            let mut f = finding("catalog_stale","age".into(),if age>21_600_000 {Severity::Error}else{Severity::Warn},first,now,
                "Broca's catalog is older than its refresh interval.",
                "Inspect health.check catalog_age_ms and catalog_refusals; compare fusiform catalog poll logs.");
            f.samples.push(
                json!({"catalog_age_ms":age,"coverage_since_ms":catalog_events.map(|s|s.since_ms)}),
            );
            findings.push(f);
        }
        Err(_) => findings.push(unavailable("catalog_stale", now)),
        _ => {}
    }
    for (check, snapshot) in std::mem::take(&mut evidence.rings) {
        match snapshot {
            Ok(snapshot) => {
                coverage.insert(
                    check.clone(),
                    Coverage {
                        since_ms: snapshot.since_ms,
                        truncated: snapshot.truncated,
                    },
                );
                let mut groups = BTreeMap::<String, Finding>::new();
                for Event {
                    at_ms,
                    module,
                    code,
                } in snapshot.events
                {
                    let key = if check == "tool_outages" {
                        module.clone()
                    } else if check == "catalog_stale" {
                        "refusals".into()
                    } else {
                        code.clone()
                    };
                    let f = groups.entry(key.clone()).or_insert_with(|| {
                        let mut f = finding(&check,key,Severity::Warn,at_ms,at_ms,
                            if check == "tool_outages" {"A tool module returned attributed outage results in the window."} else if check == "catalog_stale" {"Catalog refreshes were refused in the observed window."} else {"Routing refused sends in the observed window."},
                            if check == "tool_outages" {"Inspect the tool-plane outage log for the sampled module and code."} else if check == "catalog_stale" {"Inspect health.check catalog_refusals and the fusiform catalog poll log."} else {"Inspect the routing refused account selection log and health.check account_selection counters."});
                        f.count=0;f
                    });
                    f.count += 1;
                    f.first_seen_ms = f.first_seen_ms.min(at_ms);
                    f.last_seen_ms = f.last_seen_ms.max(at_ms);
                    if f.samples.len() < 5 {
                        f.samples.push(json!({"module_id":module,"code":code,"at_ms":at_ms,"coverage_since_ms":snapshot.since_ms}));
                    }
                }
                findings.extend(groups.into_values());
            }
            Err(_) => {
                if !findings
                    .iter()
                    .any(|f| f.check == check && f.key == "evidence_unavailable")
                {
                    findings.push(unavailable(&check, now));
                }
            }
        }
    }
    trim(Reply {
        generated_at_ms: now,
        window_ms: window,
        findings,
        coverage,
    })
}

pub fn trim(mut reply: Reply) -> Reply {
    reply.findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| a.check.cmp(&b.check))
            .then_with(|| a.key.cmp(&b.key))
    });
    let findings = std::mem::take(&mut reply.findings);
    let mut used = serde_json::to_vec(&json!({"result":&reply}))
        .expect("reply encodes")
        .len();
    for mut finding in findings {
        finding.samples.truncate(5);
        finding.summary = finding.summary.chars().take(400).collect();
        let size = serde_json::to_vec(&finding).expect("finding encodes").len() + 1;
        if used.saturating_add(size) >= REPLY_LIMIT {
            break;
        }
        used += size;
        reply.findings.push(finding);
    }
    reply
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
