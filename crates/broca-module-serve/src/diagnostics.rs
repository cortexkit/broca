//! Read-only self-diagnosis from content-free process evidence.
use broca_module::diagnostics::{self as live, Event, Fence, Snapshot};
use broca_module::run_outcomes::{OutcomeKind, RunOutcome};
use broca_protocol::{BindPrincipal, ScopeKind, SendScope};
use broca_wire::diagnostics::{Coverage, Finding, Reply, Request, Severity, DEFAULT_WINDOW_MS};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use subc_protocol::ErrorBody;

pub use broca_wire::diagnostics::OP;
pub const REPLY_LIMIT: usize = 65_536;
/// The most outcomes one check groups: what a ring holds.
const SCAN_LIMIT: usize = live::CAP;
static ROOT: OnceLock<PathBuf> = OnceLock::new();

/// Start the process's evidence rings and keep the state root the checks read files from.
pub fn install(root: PathBuf) {
    crate::session_address_health::initialize();
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
    pub tool_result_json_dumps: Result<broca_subc::tool_result_diagnostics::Snapshot, String>,
    pub runs: Result<RunEvidence, String>,
    pub late_fence_ms: Option<i64>,
    pub late_delivery_stuck: Vec<Value>,
    pub retention: Result<Vec<Fence>, String>,
    pub catalog_age_ms: Result<i64, String>,
    pub unmapped_credentials: BTreeMap<String, &'static str>,
    pub rings: BTreeMap<String, Result<Snapshot, String>>,
}

/// The run outcomes the run checks read (see [`broca_module::run_outcomes`]).
pub struct RunEvidence {
    /// Those recorded in the request's window.
    pub window: Snapshot<RunOutcome>,
    /// Every one the ring still holds: a run paused before the window is still paused.
    pub held: Vec<RunOutcome>,
}

pub fn gather(
    root: &Path,
    catalog_age_ms: Result<i64, String>,
    since: i64,
    now: i64,
) -> Result<Evidence, ErrorBody> {
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
    let runs = live::RUN_OUTCOMES.snapshot(since, now).and_then(|window| {
        Ok(RunEvidence {
            window,
            held: live::RUN_OUTCOMES.snapshot(i64::MIN, now)?.events,
        })
    });
    Ok(Evidence {
        tool_result_json_dumps: broca_subc::tool_result_diagnostics::TOOL_RESULT_JSON_DUMPS
            .snapshot(since, now),
        runs,
        late_fence_ms,
        late_delivery_stuck: crate::held_runtime::health_gauges()["late_results_delivery_stuck"]
            .as_array()
            .cloned()
            .unwrap_or_default(),
        catalog_age_ms,
        retention: live::retention_fences(),
        unmapped_credentials: crate::account_routing::published_unmapped_credentials(),
        rings: [
            (
                "session_log_missing".into(),
                crate::session_address_health::missing_snapshot(since, now),
            ),
            (
                "session_inventory_repairs".into(),
                crate::session_address_health::repair_snapshot(since, now),
            ),
            (
                "session_address_unavailable".into(),
                crate::session_address_health::snapshot(since, now),
            ),
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
                "diagnostics.findings state root is not configured",
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

/// The class and provider code of an error outcome, and the reason of an interruption.
fn outcome_fields(outcome: &RunOutcome) -> (Option<&str>, Option<&str>, Option<&str>) {
    match &outcome.kind {
        OutcomeKind::Error { class, code } => (class.as_deref(), code.as_deref(), None),
        OutcomeKind::Interrupted { reason } => (None, None, reason.as_deref()),
        OutcomeKind::Paused { .. } | OutcomeKind::Resumed => (None, None, None),
    }
}

fn group(
    rows: &[&RunOutcome],
    check: &str,
    key: impl Fn(&RunOutcome) -> String,
    hint: &str,
) -> Vec<Finding> {
    let mut groups = BTreeMap::<String, Finding>::new();
    for row in rows.iter().take(SCAN_LIMIT) {
        let key = key(row);
        let at = row.at_ms;
        let f = groups.entry(key.clone()).or_insert_with(|| {
            let mut f = finding(check, key, Severity::Warn, at, at, "", hint);
            f.count = 0;
            f
        });
        f.count += 1;
        f.first_seen_ms = f.first_seen_ms.min(at);
        f.last_seen_ms = f.last_seen_ms.max(at);
        if f.samples.len() < 5 {
            let (class, code, interrupt) = outcome_fields(row);
            f.samples.push(json!({"run_id":row.run_id,"at_ms":row.at_ms,
                "provider_module_id":row.provider,"credential_id":row.credential,
                "error_class":class,"error_code":code,"interrupt_reason":interrupt}));
        }
    }
    groups.into_values().collect()
}

/// The paused-run, run-error and interrupted-run findings, and the coverage of the latter two.
fn run_findings(
    runs: &RunEvidence,
    now: i64,
    window: i64,
    findings: &mut Vec<Finding>,
    coverage: &mut BTreeMap<String, Coverage>,
) {
    // A run is still paused when the newest outcome it reported is its pause.
    let mut newest = BTreeMap::<&str, &RunOutcome>::new();
    for outcome in &runs.held {
        newest.insert(&outcome.run_id, outcome);
    }
    let paused: Vec<_> = newest
        .into_values()
        .filter(|outcome| matches!(outcome.kind, OutcomeKind::Paused { .. }))
        .collect();
    for mut f in group(&paused, "paused_runs", |r| {
        json!([r.provider,r.credential]).to_string()
    }, "Use broca-session status for a sampled run_id; inspect paused:auth_required and its frozen credential id. Only sessions opened since broca started report a pause from before then.") {
        if now.saturating_sub(f.first_seen_ms) > 86_400_000 { f.severity = Severity::Error; }
        f.summary = format!("{} runs remain paused for authentication or account availability.", f.count);
        findings.push(f);
    }
    let dropped = if runs.window.truncated {
        "; outcomes from the start of the window were dropped to bound memory"
    } else {
        ""
    };
    let errors: Vec<_> = runs
        .window
        .events
        .iter()
        .filter(|outcome| matches!(outcome.kind, OutcomeKind::Error { .. }))
        .collect();
    for mut f in group(&errors,"run_errors",|r| {
        let (class, code, _) = outcome_fields(r);
        json!([r.provider,class,code]).to_string()
    }, "Use broca-session status for a sampled run_id; inspect its error class and provider code, not provider message text.") {
        let sample = &f.samples[0];
        let (floor,severity) = error_floor(sample["error_class"].as_str().unwrap_or("unattributed"),sample["error_code"].as_str().unwrap_or(""),window);
        if f.count < floor { continue; }
        f.severity = severity;
        f.summary = format!("{} runs ended in error in the window{dropped}.",f.count);
        findings.push(f);
    }
    let interruptions: Vec<_> = runs
        .window
        .events
        .iter()
        .filter(|outcome| {
            matches!(&outcome.kind, OutcomeKind::Interrupted { reason }
                if !matches!(reason.as_deref(), Some("runner_shutdown" | "runner_crash")))
        })
        .collect();
    for mut f in group(&interruptions,"interrupted_runs",|r| {
        outcome_fields(r).2.unwrap_or("unattributed").to_owned()
    }, "Use broca-session status for the sampled run_id and inspect the broca lifecycle log; unknown causes cannot be classified as restart seals.") {
        f.summary = format!("{} runs ended interrupted outside a restart seal{dropped}.",f.count);
        findings.push(f);
    }
    for check in ["run_errors", "interrupted_runs"] {
        coverage.insert(
            check.into(),
            Coverage {
                since_ms: runs.window.since_ms,
                truncated: runs.window.truncated,
            },
        );
    }
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
                // Meta: the key belongs to a subscription that serves only Meta's own client.
                | "client_attestation_denied"
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
    match evidence.tool_result_json_dumps {
        Ok(snapshot) => {
            coverage.insert(
                "tool_result_json_dump".into(),
                Coverage {
                    since_ms: snapshot.since_ms,
                    truncated: snapshot.truncated,
                },
            );
            for tool in snapshot.tools {
                // Digests print a finding's summary and hint, never its key, so both name the
                // tool, its module and who has to act.
                let summary = format!(
                    "{} from {}: {} {} in {} {} reached the model as a raw JSON dump instead of readable text.",
                    tool.tool_name,
                    tool.module_id,
                    tool.count,
                    if tool.count == 1 { "result" } else { "results" },
                    tool.sessions,
                    if tool.sessions == 1 { "session" } else { "sessions" },
                );
                let hint = format!(
                    "Report it to the owner of module {} with a sample call_key. The provider should shape its output into concise text for the model.",
                    tool.module_id,
                );
                let mut f = finding(
                    "tool_result_json_dump",
                    json!([tool.module_id, tool.tool_name]).to_string(),
                    Severity::Warn,
                    tool.first_seen_ms,
                    tool.last_seen_ms,
                    &summary,
                    &hint,
                );
                f.count = tool.count;
                f.samples = tool
                    .samples
                    .into_iter()
                    .map(|sample| {
                        json!({"module_id":tool.module_id,"tool_name":tool.tool_name,
                        "session":sample.session,"call_key":sample.call_key,
                        "text_bytes":sample.text_bytes,"coverage_since_ms":snapshot.since_ms})
                    })
                    .collect();
                findings.push(f);
            }
        }
        Err(_) => findings.push(unavailable("tool_result_json_dump", now)),
    }
    match &evidence.runs {
        Ok(runs) => run_findings(runs, now, window, &mut findings, &mut coverage),
        Err(_) => {
            for check in ["paused_runs", "run_errors", "interrupted_runs"] {
                findings.push(unavailable(check, now));
            }
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
    if !evidence.late_delivery_stuck.is_empty() {
        let mut f = finding("late_results_delivery_stuck", "delivery".into(), Severity::Error, now, now,
            "A held call's late result failed delivery identically at least three times; its acknowledgement remains pinned.",
            "Inspect the sampled session's held terminal and resume refusal. Later entries are delivered without acknowledging past the pin; paging beyond a permanently stuck prefix needs a durable deferred-entry spool, not an unsafe acknowledgement.");
        f.count = evidence.late_delivery_stuck.len() as u64;
        f.samples = evidence.late_delivery_stuck.into_iter().take(5).collect();
        findings.push(f);
    }
    for (credential, missing) in evidence.unmapped_credentials {
        let mut f = finding("credential_metadata_missing",credential.clone(),Severity::Warn,now,now,
            "A listed model credential lacks recognized routing metadata required for account selection.",
            "Fix the credential in the vault: set its provider ids and authentication method, or use a non-model credential type if routed model selection must not use it.");
        f.samples
            .push(json!({"credential_id":credential,"missing_fields":missing}));
        findings.push(f);
    }
    let catalog_events = evidence
        .rings
        .get("catalog_stale")
        .and_then(|r| r.as_ref().ok());
    match evidence.catalog_age_ms {
        // A healthy poller lets the catalog age to slightly over one interval just before each
        // refresh, since the next poll starts one interval after the last one finished. Only an
        // age past two intervals means at least one refresh was missed.
        Ok(age) if age > 2 * crate::CATALOG_POLL_INTERVAL.as_millis() as i64 => {
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
                    let key = if check == "tool_outages" || check == "session_log_missing" {
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
                        if check == "session_address_unavailable" {
                            f.severity = Severity::Error;
                            f.summary = format!("Session binds were refused because address discovery was unavailable: {}.", f.key.replace('_', " "));
                            f.hint = "Inspect health.check session_address_refusals and the session_address WARN log; verify inventory completeness and repair before resuming binds.".into();
                        }
                        if check == "session_inventory_repairs" {
                            f.severity = if code == "failed" { Severity::Error } else { Severity::Info };
                            f.summary = format!("Session inventory repairs {} in the observed window.", code);
                            f.hint = "Inspect health.check session_address_refusals.repairs and the session-ref migration log; known sessions remain available during repair.".into();
                        }
                        if check == "session_log_missing" {
                            f.severity = Severity::Error;
                            f.summary = format!("Session {} has history but its log is missing.", module);
                            f.hint = "Stop broca and inspect store-maintenance repair-session-root; do not create a replacement log.".into();
                        }
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
