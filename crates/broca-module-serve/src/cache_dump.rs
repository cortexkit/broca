//! Read-only, digest-attested provider request exports. Prompt bytes stay in private files.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use broca_core::wal_writer::{session_addr, FileWalStore};
use broca_core::{Clock, WalStore};
use broca_protocol::{provider::AttemptOutcome, BindIdentity, Usage};
use broca_provider::spec::WireFamilyId;
use broca_wal::WalRecord;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[derive(Default, Serialize)]
pub struct Summary {
    pub sessions_read: usize,
    pub sessions_not_found: Vec<String>,
    pub undated_runs_not_selected: u64,
    pub calls_examined: usize,
    pub calls_before_since: usize,
    pub calls_exported: usize,
    pub calls_verified: usize,
    pub digest_mismatches: usize,
    pub calls_skipped: BTreeMap<String, usize>,
}
impl Summary {
    fn skip(&mut self, reason: &str) {
        *self.calls_skipped.entry(reason.into()).or_default() += 1;
    }
}

pub fn parse_since(raw: &str) -> Result<u64, String> {
    if let Ok(ms) = raw.parse() {
        return Ok(ms);
    }
    let ms = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| "--since must be RFC3339 or epoch milliseconds")?
        .timestamp_millis();
    u64::try_from(ms).map_err(|_| "--since predates the Unix epoch".into())
}

fn private_directory(out: &Path) -> Result<(), String> {
    match fs::symlink_metadata(out) {
        Ok(meta) => {
            if !meta.is_dir() || meta.file_type().is_symlink() {
                return Err("output must be a directory, not a symlink".into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if meta.permissions().mode() & 0o077 != 0 {
                    return Err("output directory must have mode 0700".into());
                }
            }
            #[cfg(windows)]
            broca_owner_only::validate(out).map_err(|e| format!("output directory ACL: {e}"))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            broca_owner_only::create_dir(out)
                .map_err(|_| "cannot create private output directory")?;
        }
        Err(_) => return Err("cannot inspect output directory".into()),
    }
    Ok(())
}

fn private_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut options = broca_owner_only::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options
        .open(path)
        .map_err(|_| "cannot create output file (existing files are never overwritten)")?;
    file.write_all(bytes)
        .map_err(|_| "cannot write output file".into())
}

/// Invert the provider normalizers, preserving absent measurements.
pub fn wire_usage(family: WireFamilyId, usage: Usage) -> Option<Value> {
    if matches!(
        family,
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist
    ) {
        return Some(json!({"usage": gemini_usage_metadata(usage), "broca_usage": usage}));
    }
    let mut fields = serde_json::Map::new();
    let input = match family {
        // Messages reports fresh input directly; cache creation/read are separate totals.
        WireFamilyId::AnthropicMessages => usage.input_tokens,
        // Responses' inclusive input contains both reads and writes.
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
            usage.input_tokens.map(|n| {
                n.saturating_add(usage.cached_input_tokens.unwrap_or(0))
                    .saturating_add(usage.cache_write_tokens.unwrap_or(0))
            })
        }
        _ => return None,
    };
    if let Some(n) = input {
        fields.insert("input_tokens".into(), json!(n));
    }
    if let Some(n) = usage.output_tokens {
        fields.insert("output_tokens".into(), json!(n));
    }
    if family == WireFamilyId::AnthropicMessages {
        if let Some(n) = usage.cached_input_tokens {
            fields.insert("cache_read_input_tokens".into(), json!(n));
        }
        if let Some(n) = usage.cache_write_tokens {
            fields.insert("cache_creation_input_tokens".into(), json!(n));
        }
    } else if let Some(n) = usage.cached_input_tokens {
        fields.insert("input_tokens_details".into(), json!({"cached_tokens": n}));
    }
    Some(json!({"usage": fields, "broca_usage": usage}))
}

/// Rebuild Gemini's `usageMetadata` counts, which Code Assist also uses inside its
/// `{response: ...}` wrapper because it decodes through the Gemini normalizer.
///
/// The normalizer splits Gemini's `promptTokenCount`, which INCLUDES the
/// `cachedContentTokenCount` cache hits, so broca's `input_tokens` EXCLUDES cached tokens here:
/// the prompt count is fresh input plus cache reads. With no input measurement the prompt
/// count stays absent even when a cache count is known.
///
/// The normalizer also folds `thoughtsTokenCount`, a sibling of the visible-only
/// `candidatesTokenCount`, into `output_tokens`, so candidates are output minus thoughts. A
/// lone thoughts count normalizes to output equal to thoughts; Google's JSON omits zero-valued
/// counts, so that shape most likely came from an absent candidates count and stays absent.
/// Gemini reports no cache-write count, so `cache_write_tokens` has no field to return to.
fn gemini_usage_metadata(usage: Usage) -> serde_json::Map<String, Value> {
    let mut fields = serde_json::Map::new();
    if let Some(n) = usage.input_tokens {
        let prompt = n.saturating_add(usage.cached_input_tokens.unwrap_or(0));
        fields.insert("promptTokenCount".into(), json!(prompt));
    }
    if let Some(n) = usage.cached_input_tokens {
        fields.insert("cachedContentTokenCount".into(), json!(n));
    }
    let candidates = match (usage.output_tokens, usage.reasoning_tokens) {
        (Some(output), None) => Some(output),
        (Some(output), Some(thoughts)) => output.checked_sub(thoughts).filter(|n| *n > 0),
        (None, _) => None,
    };
    if let Some(n) = candidates {
        fields.insert("candidatesTokenCount".into(), json!(n));
    }
    if let Some(n) = usage.reasoning_tokens {
        fields.insert("thoughtsTokenCount".into(), json!(n));
    }
    fields
}

fn select_addresses(
    root: &Path,
    sessions: &[String],
    since: Option<u64>,
    direct: Option<&DirectBind>,
    summary: &mut Summary,
) -> Result<BTreeMap<String, BindIdentity>, String> {
    if let Some(bind) = direct {
        if since.is_some() {
            return Err("direct bind selection requires --session".into());
        }
        return Ok(sessions
            .iter()
            .map(|name| {
                let key = BindIdentity::new(&bind.project_root, &bind.harness, name);
                (session_addr(&key), key)
            })
            .collect());
    }
    let index = root.join("run-index.db");
    let failure = || format!("cannot read run index {}", index.display());
    let absolute = if index.is_absolute() {
        index.clone()
    } else {
        std::env::current_dir().map_err(|_| failure())?.join(&index)
    };
    let mut uri = url::Url::from_file_path(absolute).map_err(|_| failure())?;
    // mode=ro retains committed SQLite WAL-sidecar visibility. immutable=1 would hide
    // uncheckpointed runs, and the store's normal constructor would migrate/create data.
    uri.set_query(Some("mode=ro"));
    let connection = rusqlite::Connection::open_with_flags(
        uri.as_str(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| failure())?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|_| failure())?;
    let mut addresses = BTreeMap::new();
    let mut add = |raw: String| -> Result<(), String> {
        let bind: BindIdentity = serde_json::from_str(&raw).map_err(|_| failure())?;
        addresses.insert(session_addr(&bind), bind);
        Ok(())
    };
    if let Some(since) = since {
        let since =
            i64::try_from(since).map_err(|_| "--since exceeds the index timestamp range")?;
        // Terminal state_changed_ms is written after WAL sync, so it bounds the run's
        // last activity. Active/paused timestamps are not completion bounds: include
        // those runs regardless of admission time, then check attempt timestamps in WAL.
        let mut statement = connection.prepare("SELECT DISTINCT session FROM run_index WHERE state_changed_ms >= ?1 OR state IN ('active', 'paused')").map_err(|_| failure())?;
        for raw in statement
            .query_map([since], |row| row.get::<_, String>(0))
            .map_err(|_| failure())?
        {
            add(raw.map_err(|_| failure())?)?;
        }
        summary.undated_runs_not_selected = connection.query_row("SELECT count(*) FROM run_index WHERE state_changed_ms IS NULL AND state NOT IN ('active', 'paused')", [], |row| row.get(0)).map_err(|_| failure())?;
    } else {
        let mut statement = connection.prepare("SELECT DISTINCT session FROM run_index WHERE json_extract(session, '$.session') = ?1").map_err(|_| failure())?;
        for name in sessions.iter().collect::<BTreeSet<_>>() {
            let mut found = false;
            for raw in statement
                .query_map([name], |row| row.get::<_, String>(0))
                .map_err(|_| failure())?
            {
                found = true;
                add(raw.map_err(|_| failure())?)?;
            }
            if !found {
                summary.sessions_not_found.push(name.clone());
            }
        }
    }
    Ok(addresses)
}

pub fn export(
    root: &Path,
    sessions: &[String],
    since: Option<u64>,
    out: &Path,
) -> Result<Summary, String> {
    export_with_bind(root, sessions, since, out, None)
}

pub struct DirectBind {
    pub project_root: PathBuf,
    pub harness: String,
}

pub fn export_with_bind(
    root: &Path,
    sessions: &[String],
    since: Option<u64>,
    out: &Path,
    direct: Option<&DirectBind>,
) -> Result<Summary, String> {
    let store = FileWalStore::new(root.join("wal"));
    let clock = Clock::for_state_root(root).map_err(|_| "cannot read retention clock")?;
    let mut summary = Summary::default();
    let addresses = select_addresses(root, sessions, since, direct, &mut summary)?;
    // Preflight the whole selection before creating the output directory. A later expired
    // lineage must not leave a partial export of earlier sessions behind.
    let selected = addresses
        .into_iter()
        .map(|(address, key)| {
            exportable_records(&store, &key, clock.now_ms()).map(|records| (address, records))
        })
        .collect::<Result<Vec<_>, _>>()?;
    private_directory(out)?;
    for (address, records) in selected {
        summary.sessions_read += 1;
        export_records_since(&address, &records, out, &mut summary, since)?;
    }
    Ok(summary)
}

fn exportable_records(
    store: &FileWalStore,
    key: &BindIdentity,
    now_ms: u64,
) -> Result<Vec<WalRecord>, String> {
    store.check_move(key, true).map_err(|e| e.to_string())?;
    let read = store
        .read_records_range(key, 0, None, None)
        .map_err(|_| "cannot read WAL")?;
    let stones_path = store
        .ephemeral_root()
        .join(format!("{}.expired", session_addr(key)));
    let mut stones = Vec::new();
    match std::fs::read_dir(stones_path) {
        Ok(entries) => {
            for entry in entries {
                let path = entry
                    .map_err(|_| "cannot read retention tombstones")?
                    .path();
                let raw = std::fs::read(path).map_err(|_| "cannot read retention tombstone")?;
                let stone: broca_module::retention::Tombstone =
                    serde_json::from_slice(&raw).map_err(|_| "invalid retention tombstone")?;
                stones.push(stone);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("cannot read retention tombstones".into()),
    }
    // A matching tombstone is irreversible, even if the clock moved backwards. A newer
    // live or archived lineage supersedes only tombstones belonging to older lineages.
    let expired_at_ms = if matches!(read.records(), broca_wal::WalRangeRecords::Absent) {
        stones
            .iter()
            .max_by_key(|stone| stone.seq)
            .map(|stone| stone.expired_at_ms)
    } else {
        let lineage = read.lineage_id.map(|id| id.to_string());
        stones
            .iter()
            .find(|stone| Some(stone.lineage_id.as_str()) == lineage.as_deref())
            .map(|stone| stone.expired_at_ms)
            .or_else(|| match read.records() {
                broca_wal::WalRangeRecords::Complete(frames) => {
                    broca_module::retention::policy(frames)
                        .and_then(|(_, _, deadline)| deadline)
                        .filter(|deadline| *deadline <= now_ms)
                }
                _ => None,
            })
    };
    if let Some(expired_at_ms) = expired_at_ms {
        return Err(format!("expired {{expired_at_ms: {expired_at_ms}}}"));
    }
    match read.into_records() {
        broca_wal::WalRangeRecords::Complete(frames) => {
            Ok(frames.into_iter().map(|frame| frame.record).collect())
        }
        broca_wal::WalRangeRecords::Absent => Ok(Vec::new()),
        _ => Err("incomplete WAL read".into()),
    }
}

pub fn export_records(
    address: &str,
    records: &[WalRecord],
    out: &Path,
    summary: &mut Summary,
) -> Result<(), String> {
    export_records_since(address, records, out, summary, None)
}

fn export_records_since(
    _address: &str,
    records: &[WalRecord],
    out: &Path,
    summary: &mut Summary,
    since: Option<u64>,
) -> Result<(), String> {
    let starts: Vec<_> = records
        .iter()
        .enumerate()
        .filter_map(|(i, r)| matches!(r, WalRecord::RunStarted { .. }).then_some(i))
        .collect();
    for (position, &start) in starts.iter().enumerate() {
        let end = starts.get(position + 1).copied().unwrap_or(records.len());
        let WalRecord::RunStarted {
            run_id,
            session,
            config,
            ..
        } = &records[start]
        else {
            unreachable!()
        };
        let mut boundaries = BTreeMap::new();
        let mut attempts = BTreeMap::new();
        let mut completed = BTreeMap::new();
        let mut usages = BTreeMap::new();
        for (index, record) in records[start..end].iter().enumerate() {
            match record {
                WalRecord::StepStarted { step_id } => {
                    boundaries.insert(step_id.0, start + index);
                }
                WalRecord::ModelAttemptFinished {
                    step_id,
                    response_body_len,
                    outcome,
                    ..
                } if *response_body_len > 0 => {
                    attempts.insert(step_id.0, record);
                    if *outcome == AttemptOutcome::Complete {
                        completed.insert(step_id.0, record);
                    }
                }
                WalRecord::ModelStepFinished { step_id, usage, .. } => {
                    usages.insert(step_id.0, *usage);
                }
                _ => {}
            }
        }
        for (step, last_response) in attempts {
            let usage = usages.get(&step).copied();
            let selected = if usage.is_some() {
                completed.get(&step).copied().unwrap_or(last_response)
            } else {
                last_response
            };
            let WalRecord::ModelAttemptFinished {
                attempt,
                request_body_sha256,
                ts_ms,
                input_transformations,
                ..
            } = selected
            else {
                unreachable!()
            };
            if since.is_some_and(|cutoff| *ts_ms < cutoff) {
                summary.calls_before_since += 1;
                continue;
            }
            summary.calls_examined += 1;
            let filename =
                broca_provider::request_dump::request_dump_filename(None, &run_id.0, step);
            let name = filename
                .strip_suffix(".json")
                .ok_or("invalid request dump filename")?;
            let body_name = format!("{name}.body.json");
            let response_name = format!("{name}.response.json");
            let created = i64::try_from(*ts_ms)
                .ok()
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .ok_or("attempt timestamp out of range")?
                .to_rfc3339_opts(SecondsFormat::Millis, true);
            let mut meta = json!({"session": session.session, "createdAt": created, "files": {}, "run_id": run_id.0, "step": step, "attempt": attempt, "wire_family": config.frozen_render_config.get("family_id"), "model": config.model.model_id, "verified": false});
            let rendered = boundaries.get(&step).and_then(|boundary| {
                crate::wal_render::render_step(&records[..=*boundary], config, &run_id.0, step).ok()
            });
            if let Some((frozen, _, body)) = rendered {
                meta["wire_family"] = json!(frozen.family_id);
                let actual = format!("{:x}", Sha256::digest(&body));
                let verified = actual == *request_body_sha256;
                meta["verified"] = json!(verified);
                if verified {
                    summary.calls_verified += 1;
                }
                let supported = wire_usage(frozen.family_id, Usage::default()).is_some();
                let response = usage.and_then(|u| wire_usage(frozen.family_id, u));
                if !verified {
                    summary.digest_mismatches += 1;
                    let reason = if input_transformations.is_some() {
                        "transformed_request"
                    } else {
                        "digest_mismatch"
                    };
                    meta["reason"] = json!(reason);
                    meta["recorded_sha256"] = json!(request_body_sha256);
                    meta["rendered_sha256"] = json!(actual);
                } else if !supported {
                    meta["reason"] = json!("family not exported");
                }
                if verified && supported {
                    private_write(&out.join(&body_name), &body)?;
                    meta["files"] = json!({"body": body_name});
                    if let Some(response) = response {
                        private_write(
                            &out.join(&response_name),
                            &serde_json::to_vec(&response).map_err(|_| "encode usage")?,
                        )?;
                        meta["files"] = json!({"body": body_name, "response": response_name});
                        summary.calls_exported += 1;
                    }
                }
            } else {
                meta["reason"] = json!("render_unavailable");
            }
            // Verification failures outrank unsupported families and missing usage. Count only
            // the reason actually written in metadata, so each skipped call has one explanation.
            if usage.is_none() && meta.get("reason").is_none() {
                meta["reason"] = json!("missing_step_usage");
            }
            if let Some(reason) = meta.get("reason").and_then(Value::as_str) {
                summary.skip(reason);
            }
            private_write(
                &out.join(format!("{name}.meta.json")),
                &serde_json::to_vec(&meta).map_err(|_| "encode metadata")?,
            )?;
        }
    }
    Ok(())
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, broaden, deny_acl_read, reset_acl, BroadParent,
    };
    #[test]
    fn windows_cache_export_files_and_directory_are_private() {
        let fixture = BroadParent::new("cache");
        let dir = fixture.path.join("export");
        private_directory(&dir).unwrap();
        let path = dir.join("request.json");
        private_write(&path, b"request body").unwrap();
        assert_private(&dir);
        assert_private(&path);
    }
    #[test]
    fn windows_cache_export_refuses_insecure_or_unreadable_directory() {
        let fixture = BroadParent::new("cache-refusal");
        let dir = fixture.path.join("export");
        private_directory(&dir).unwrap();
        for unreadable in [false, true] {
            if unreadable {
                deny_acl_read(&dir);
            } else {
                broaden(&dir);
            }
            assert!(private_directory(&dir).is_err());
            reset_acl(&dir);
            broca_owner_only::repair(&dir).unwrap();
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        }
    }
}
