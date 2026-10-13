//! Read-only reasoning audit: cargo run -p broca-module-serve --example reasoning-audit --
//! --days 3 [--prefix alfonso:consult- --prefix alfonso:spec-].
//! Uses the shared state-root resolver, never opens endpoints or the SQLite index, and emits
//! only reasoning settings. Digest mismatches remain visible; their renders are not attested.
#[path = "support/wal_render.rs"]
mod wal_render;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use broca_wal::{RunConfig, WalRecord};
use clap::Parser;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Parser)]
struct Args {
    /// Include live WALs and archive containers modified within this many days.
    #[arg(long, default_value_t = 3)]
    days: u64,
    /// Session name prefixes (repeatable). Defaults to consult and spec sessions.
    #[arg(long)]
    prefix: Vec<String>,
}

#[derive(Default)]
struct Counts {
    live_seen: usize,
    archives_seen: usize,
    scanned: usize,
    skipped: BTreeMap<&'static str, usize>,
    runs: usize,
    problems: usize,
}
impl Counts {
    fn skip(&mut self, why: &'static str) {
        *self.skipped.entry(why).or_default() += 1;
    }
}

// Select leaves explicitly, never entire objects: provider options may contain unrelated data.
fn reasoning_fields(body: &Value) -> Value {
    select_leaves(
        body,
        &[
            "/reasoning/effort",
            "/reasoning/summary",
            "/reasoning_effort",
            "/thinking/type",
            "/thinking/budget_tokens",
            "/output_config/effort",
            "/generationConfig/thinkingConfig/thinkingBudget",
            "/generationConfig/thinkingConfig/thinkingLevel",
            "/generationConfig/thinkingConfig/includeThoughts",
            "/request/generationConfig/thinkingConfig/thinkingBudget",
            "/request/generationConfig/thinkingConfig/thinkingLevel",
            "/request/generationConfig/thinkingConfig/includeThoughts",
            "/additionalModelRequestFields/thinking/type",
            "/additionalModelRequestFields/thinking/budget_tokens",
            "/additionalModelRequestFields/reasoning_effort",
        ],
    )
}

fn select_leaves(body: &Value, pointers: &[&str]) -> Value {
    let mut out = Map::new();
    for pointer in pointers {
        if let Some(v) = body.pointer(pointer) {
            if v.is_string() || v.is_number() || v.is_boolean() {
                out.insert(pointer.trim_start_matches('/').to_owned(), v.clone());
            }
        }
    }
    Value::Object(out)
}

fn digest_matches(body: &[u8], len: u64, expected: &str) -> bool {
    let actual = Sha256::digest(body);
    let hex: String = actual.iter().map(|b| format!("{b:02x}")).collect();
    body.len() as u64 == len && hex == expected
}

fn cell(v: impl std::fmt::Display) -> String {
    v.to_string().replace('|', "\\|").replace(['\n', '\r'], " ")
}

struct Check {
    text: String,
    matches: bool,
    rendered: bool,
}

fn check_step(
    records: &[WalRecord],
    config: &RunConfig,
    run_id: &str,
    episode: std::ops::Range<usize>,
    recorded: (u64, u32, u64, &str),
) -> Check {
    let (step, attempt, len, digest) = recorded;
    let start = episode.start;
    let end = episode.end;
    let result = (|| {
        let offset = records[start..end]
            .iter()
            .position(|r| matches!(r, WalRecord::StepStarted { step_id } if step_id.0 == step))
            .ok_or("missing step_started")?;
        let (frozen, _, body) =
            wal_render::render_step(&records[..start + offset], config, run_id, step)?;
        let parsed: Value =
            serde_json::from_slice(&body).map_err(|_| "rendered body is not JSON")?;
        let fields = reasoning_fields(&parsed);
        let matches = digest_matches(&body, len, digest);
        let backend = if matches!(
            frozen.family_id,
            broca_provider::spec::WireFamilyId::Gemini
                | broca_provider::spec::WireFamilyId::GoogleCodeAssist
        ) {
            format!(" backend={}", frozen.wire_model_id)
        } else {
            String::new()
        };
        Ok::<_, String>(Check {
            rendered: fields.as_object().is_some_and(|m| !m.is_empty()),
            matches,
            text: format!(
                "step={step} attempt={attempt} {fields}{backend}; digest={}",
                if matches { "yes" } else { "NO" }
            ),
        })
    })();
    result.unwrap_or_else(|_| Check {
        // Do not print renderer errors: a decode error can quote arbitrary frozen input.
        text: format!(
            "step={step} attempt={attempt}; render/replay failed; digest=NO (unverified)"
        ),
        matches: false,
        rendered: false,
    })
}

#[derive(Default)]
struct Summary {
    runs: usize,
    requested: usize,
    rendered: usize,
    matched: usize,
    later: usize,
    later_matched: usize,
}

fn audit(
    records: &[WalRecord],
    prefixes: &[String],
    counts: &mut Counts,
    summary: &mut BTreeMap<String, Summary>,
    rows: &mut Vec<String>,
) {
    let starts: Vec<usize> = records
        .iter()
        .enumerate()
        .filter_map(|(i, r)| matches!(r, WalRecord::RunStarted { .. }).then_some(i))
        .collect();
    for (pos, &start) in starts.iter().enumerate() {
        let end = starts.get(pos + 1).copied().unwrap_or(records.len());
        let WalRecord::RunStarted {
            session,
            run_id,
            config,
            ..
        } = &records[start]
        else {
            unreachable!()
        };
        let Some(prefix) = prefixes
            .iter()
            .find(|p| session.session.starts_with(p.as_str()))
        else {
            counts.skip("nonmatching episode");
            continue;
        };
        counts.runs += 1;
        let key = format!(
            "{}/{}",
            config.model.provider_module_id, config.model.model_id
        );
        let group = summary.entry(key.clone()).or_default();
        group.runs += 1;
        group.requested += usize::from(config.variant.is_some());
        let frozen = &config.frozen_render_config;
        let resolved = select_leaves(
            frozen,
            &[
                "/variant/name",
                "/variant/request/kind",
                "/variant/request/effort",
                "/variant/request/thinking/type",
                "/variant/request/thinking/budget_tokens",
                "/variant/request/thinking_config/thinkingLevel",
                "/variant/request/thinking_config/thinkingBudget",
                "/variant/request/thinking_config/includeThoughts",
                "/reasoning/mode",
                "/reasoning/effort",
                "/reasoning/summary",
                "/reasoning/budget_tokens",
                "/reasoning_effort",
                "/service_tier",
                "/speed",
            ],
        );
        // Each selected step uses its first finished attempt, not a later retry of the same step.
        let mut attempts = Vec::new();
        let mut steps = BTreeSet::new();
        for r in &records[start..end] {
            if let WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                request_body_len,
                request_body_sha256,
                ..
            } = r
            {
                if steps.insert(step_id.0) {
                    attempts.push((
                        step_id.0,
                        *attempt,
                        *request_body_len,
                        request_body_sha256.as_str(),
                    ));
                }
            }
        }
        let mut checked = Vec::new();
        for &(step, attempt, len, digest) in attempts.iter().take(2) {
            checked.push(check_step(
                records,
                config,
                &run_id.0,
                start..end,
                (step, attempt, len, digest),
            ));
        }
        if let Some(first) = checked.first() {
            group.rendered += usize::from(first.rendered);
            group.matched += usize::from(first.matches);
            counts.problems += usize::from(!first.matches);
        } else {
            counts.skip("matched run without attempt evidence");
            counts.problems += 1;
        }
        if let Some(later) = checked.get(1) {
            group.later += 1;
            group.later_matched += usize::from(later.matches);
            counts.problems += usize::from(!later.matches);
        }
        rows.push(format!(
            "| {} {} | {} | {} | {} | {} | {} |",
            cell(prefix),
            cell(
                run_id
                    .0
                    .strip_prefix("run-sid-")
                    .unwrap_or(&run_id.0)
                    .chars()
                    .take(16)
                    .collect::<String>()
            ),
            cell(&key),
            cell(config.variant.as_deref().unwrap_or("—")),
            cell(resolved),
            cell(
                checked
                    .first()
                    .map_or("no attempt evidence; digest=NO (unverified)", |c| &c.text)
            ),
            cell(
                checked
                    .get(1)
                    .map_or("— (no later step evidence)", |c| &c.text)
            )
        ));
    }
}

fn recent(path: &Path, cutoff: SystemTime) -> Result<bool, String> {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|t| t >= cutoff)
        .map_err(|e| e.to_string())
}

fn entries(dir: &Path) -> Result<impl Iterator<Item = Result<PathBuf, String>>, String> {
    let read = match std::fs::read_dir(dir) {
        Ok(read) => Some(read),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.to_string()),
    };
    Ok(read
        .into_iter()
        .flatten()
        .map(|e| e.map(|e| e.path()).map_err(|e| e.to_string())))
}

fn run(args: Args) -> Result<(), String> {
    let root = broca_module_serve::state_root::resolve_state_root()
        .map_err(|e| e.to_string())?
        .path;
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(
            args.days.checked_mul(86400).ok_or("days overflow")?,
        ))
        .ok_or("days out of range")?;
    let prefixes = if args.prefix.is_empty() {
        vec!["alfonso:consult-".into(), "alfonso:spec-".into()]
    } else {
        args.prefix
    };
    let mut counts = Counts::default();
    let mut summary = BTreeMap::new();
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for entry in entries(&root.join("wal"))? {
        let path = entry?;
        if !path.extension().is_some_and(|e| e == "wal") {
            continue;
        }
        counts.live_seen += 1;
        if let Some(stem) = path.file_stem() {
            seen.insert(stem.to_string_lossy().into_owned());
        }
        if !recent(&path, cutoff)? {
            counts.skip("live WAL older than window");
            continue;
        }
        counts.scanned += 1;
        match broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES) {
            Ok(frames) => audit(
                &frames.into_iter().map(|f| f.record).collect::<Vec<_>>(),
                &prefixes,
                &mut counts,
                &mut summary,
                &mut rows,
            ),
            Err(_) => {
                counts.skip("unreadable live WAL");
                counts.problems += 1;
            }
        }
    }
    // A container has no per-member modification time, so its modification time selects the
    // window. Live WALs take precedence; among containers holding the same WAL, the most recently
    // created archive wins. All archive access uses the read-only reader.
    let mut archives = Vec::new();
    for entry in entries(&root.join("wal-archive"))? {
        let path = entry?;
        if !path.extension().is_some_and(|e| e == "ark") {
            continue;
        }
        counts.archives_seen += 1;
        if !recent(&path, cutoff)? {
            counts.skip("archive container older than window (members not inventoried)");
            continue;
        }
        archives.push((
            broca_wal::archive::fold_timestamp(&path).map_err(|e| e.to_string())?,
            path,
        ));
    }
    archives.sort_by_key(|(stamp, _)| std::cmp::Reverse(*stamp));
    for (_, path) in archives {
        let result = (|| {
            let mut reader =
                broca_wal::archive::ArchiveReader::open(&path, broca_wal::DEFAULT_MAX_READ_BYTES)
                    .map_err(|e| e.to_string())?;
            if reader.state().map_err(|e| e.to_string())?
                == broca_wal::archive::ContainerState::Unsealed
            {
                counts.skip("unsealed archive container");
                return Ok::<_, String>(());
            }
            for info in reader.rescan().map_err(|e| e.to_string())? {
                if !seen.insert(info.session_addr.clone()) {
                    counts.skip("archive member shadowed by live/newer WAL");
                    continue;
                }
                counts.scanned += 1;
                let member = reader
                    .read_member(&info.session_addr)
                    .map_err(|e| e.to_string())?
                    .ok_or("missing member")?;
                let range =
                    broca_wal::file_wal::read_verified_member_range(&member.payload, 1, 0, None)
                        .map_err(|e| e.to_string())?;
                match range.records() {
                    broca_wal::WalRangeRecords::Complete(records) => audit(
                        &records.iter().map(|f| f.record.clone()).collect::<Vec<_>>(),
                        &prefixes,
                        &mut counts,
                        &mut summary,
                        &mut rows,
                    ),
                    _ => return Err("incomplete archive member".into()),
                }
            }
            Ok(())
        })();
        if result.is_err() {
            counts.skip("unreadable archive container (remaining members unknown)");
            counts.problems += 1;
        }
    }
    println!(
        "# Broca reasoning audit\n\nState root: `{}`; window: {} days by WAL/container mtime.\n",
        root.display(),
        args.days
    );
    println!("Live WALs inventoried: {}; archive containers inventoried: {}; WALs scanned: {}; matching runs: {}; unverified checks/read failures: {}.\n", counts.live_seen, counts.archives_seen, counts.scanned, counts.runs, counts.problems);
    println!("## Skips (units named explicitly)\n");
    for (reason, n) in &counts.skipped {
        println!("- {reason}: {n}");
    }
    println!("\n## Runs\n\n| Session prefix / run | Provider/model | Requested variant | Frozen resolved settings | First step reasoning / digest | Later step reasoning / digest |\n|---|---|---|---|---|---|");
    rows.sort();
    let rows_by_prefix: BTreeMap<_, _> = prefixes
        .iter()
        .map(|p| {
            (
                p.clone(),
                rows.iter()
                    .filter(|row| row.starts_with(&format!("| {} ", cell(p))))
                    .count(),
            )
        })
        .collect();
    for row in rows {
        println!("{row}");
    }
    println!("\n## Summary\n\nRendered counts include defaults even when no variant was requested. Only digest=yes attests bytes. Later means a distinct step, not a retry.\n\n| Provider/model | Runs | Requested variant | Rendered reasoning (first) | Digest matched (first) | Later checked | Later matched |\n|---|---:|---:|---:|---:|---:|---:|");
    let mut missing_later = Vec::new();
    for (key, s) in summary {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            cell(&key),
            s.runs,
            s.requested,
            s.rendered,
            s.matched,
            s.later,
            s.later_matched
        );
        if s.later == 0 {
            missing_later.push(key);
        }
    }
    for key in missing_later {
        println!("\nNo multi-step evidence for `{}`.", cell(key));
    }
    for prefix in &prefixes {
        let n = rows_by_prefix.get(prefix).copied().unwrap_or(0);
        println!("\nMatching episodes for `{}`: {n}.", cell(prefix));
    }
    Ok(())
}

fn main() {
    if run(Args::parse()).is_err() {
        eprintln!(
            "audit incomplete: state resolution, directory inventory, or metadata read failed"
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn start(session: &str, id: &str) -> WalRecord {
        serde_json::from_value(json!({
            "type": "run_started", "run_id": id,
            "session": {"project_root": "/tmp/project", "harness": "test", "session": session},
            "config": {"model": {"provider_module_id": "p", "model_id": "m"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1},
            "input": []
        }))
        .unwrap()
    }

    fn attempt(step: u64, n: u32) -> WalRecord {
        serde_json::from_value(json!({
            "type": "model_attempt_finished", "step_id": step, "attempt": n,
            "request_body_len": 3, "request_body_sha256": "bad",
            "response_body_len": 0, "response_body_sha256": "", "outcome": "complete", "ts_ms": 0
        }))
        .unwrap()
    }

    #[test]
    fn later_check_selects_distinct_step_not_retry_and_keeps_unverified_runs() {
        let records = vec![
            start("alfonso:consult-test", "first"),
            attempt(1, 0),
            attempt(1, 1),
            attempt(2, 0),
            start("unrelated", "other"),
            start("alfonso:spec-test", "no-attempt"),
        ];
        let mut counts = Counts::default();
        let mut summary = BTreeMap::new();
        let mut rows = Vec::new();
        audit(
            &records,
            &["alfonso:consult-".into(), "alfonso:spec-".into()],
            &mut counts,
            &mut summary,
            &mut rows,
        );
        assert_eq!(counts.runs, 2);
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains("step=1 attempt=0"));
        assert!(rows[0].contains("step=2 attempt=0"));
        assert!(!rows[0].contains("attempt=1"));
        assert!(rows[1].contains("no attempt evidence"));
        assert_eq!(summary["p/m"].later, 1);
        assert_eq!(summary["p/m"].matched, 0);
    }

    #[test]
    fn frozen_settings_do_not_print_unknown_nested_data() {
        let v = json!({"variant": {"request": {"effort": "high", "thinking": {"text": "SECRET"}}}});
        assert_eq!(
            select_leaves(&v, &["/variant/request/effort"]),
            json!({"variant/request/effort": "high"})
        );
    }
    #[test]
    fn reports_only_reasoning_leaves() {
        let body = json!({"messages": [{"content": "SECRET PROMPT"}], "tools": [{"secret": "schema"}],
            "reasoning": {"effort": "high", "summary": "auto", "prompt": "SECRET"},
            "thinking": {"type": "enabled", "budget_tokens": 1000, "text": "SECRET"},
            "output_config": {"effort": "max", "schema": "SECRET"},
            "request": {"generationConfig": {"thinkingConfig": {"thinkingLevel": "high", "includeThoughts": true}}, "contents": "SECRET"}});
        assert_eq!(
            reasoning_fields(&body),
            json!({"reasoning/effort": "high", "reasoning/summary": "auto",
            "thinking/type": "enabled", "thinking/budget_tokens": 1000, "output_config/effort": "max",
            "request/generationConfig/thinkingConfig/thinkingLevel": "high",
            "request/generationConfig/thinkingConfig/includeThoughts": true})
        );
    }
    #[test]
    fn digest_checks_bytes_and_length() {
        let hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert!(digest_matches(b"abc", 3, hash));
        assert!(!digest_matches(b"abd", 3, hash));
        assert!(!digest_matches(b"abc", 4, hash));
    }
}
