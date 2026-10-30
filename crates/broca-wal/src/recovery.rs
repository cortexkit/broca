//! Loss-preserving repair for overhaul logs. The visibility proof classifies
//! damage; it never authorizes destruction of the only copy of a suffix.

use crate::{
    framing::{self, DecodeStep},
    fs_ops::{self, File, OpenOptions},
    WalError, WalRecord,
};
use broca_protocol::{BatchId, RunId, StepId, ToolCallId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub const FEATURE: &str = "wal-recovery/v1";

/// A registered derived view is invalidated durably before any cut. Implementors
/// also finish its projection before a writable log can be returned to an actor.
pub trait RecoveryHooks: Send + Sync {
    fn before_cut(&self, cut: &RecoveryCut) -> Result<(), WalError>;
    fn after_repair(&self, log: &Path, cut_ids: &BTreeSet<String>) -> Result<(), WalError>;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPolicy {
    #[default]
    Repair,
    Refuse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutReason {
    UnpublishedTail,
    PublishedRegionCorruption,
    UnprovenCorruption,
}

impl CutReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnpublishedTail => "unpublished_tail",
            Self::PublishedRegionCorruption => "published_region_corruption",
            Self::UnprovenCorruption => "unproven_corruption",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCall {
    pub tool_call_id: ToolCallId,
    pub step_id: Option<StepId>,
    pub batch_id: Option<BatchId>,
    /// Exact Prepared/intent keys recovered from the record or its frame identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOutcome {
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnownedRecoveryCall {
    pub outcome: RecoveryOutcome,
    /// Evidence only: a model-facing ID does not identify a later run's call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<ToolCallId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<StepId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<BatchId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRun {
    pub run_id: RunId,
    pub calls: Vec<RecoveryCall>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCut {
    pub cut_id: String,
    pub sidecar_basename: String,
    pub cut_offset: u64,
    pub removed_len: u64,
    pub removed_sha256: String,
    pub reason: CutReason,
    pub active_run: Option<RunId>,
    pub runs: Vec<RecoveryRun>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unowned_calls: Vec<UnownedRecoveryCall>,
    pub unattributed_tool_risk: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CutManifest {
    pub version: u32,
    pub session: String,
    pub prefix_sha256: String,
    pub recovery: RecoveryCut,
    pub checksum: String,
}

impl CutManifest {
    fn expected_checksum(&self) -> Result<String, WalError> {
        serde_json::to_vec(&(
            self.version,
            &self.session,
            &self.prefix_sha256,
            &self.recovery,
        ))
        .map(|bytes| digest(&bytes))
        .map_err(|e| WalError::Io(std::io::Error::other(e)))
    }

    pub fn integrity_valid(&self) -> bool {
        self.version == 1
            && self
                .expected_checksum()
                .is_ok_and(|sum| sum == self.checksum)
    }
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Only call attribution is retained while inspecting the prefix. Content is
/// copied verbatim to the raw sidecar, never into a warning or a finding.
#[derive(Default)]
struct Calls {
    active: Option<RunId>,
    runs: Vec<RecoveryRun>,
    planned: Vec<RecoveryCall>,
    cut_ids: BTreeSet<String>,
    prepared: std::collections::BTreeMap<String, (Option<RunId>, RecoveryCall)>,
    unowned: Vec<UnownedRecoveryCall>,
}

impl Calls {
    fn anchor_prepared(
        &mut self,
        frame: &crate::FramedRecord,
        lineage: Option<framing::WalLineageId>,
    ) {
        let (
            Some(lineage),
            WalRecord::ToolCallDeferred {
                call_id, batch_id, ..
            },
        ) = (lineage, &frame.record)
        else {
            return;
        };
        let mut call = self
            .planned
            .iter()
            .find(|call| &call.tool_call_id == call_id)
            .cloned()
            .unwrap_or_else(|| RecoveryCall {
                tool_call_id: call_id.clone(),
                step_id: None,
                batch_id: None,
                call_keys: Vec::new(),
            });
        call.batch_id = Some(batch_id.clone());
        let key = format!("{lineage}:{}", frame.seq);
        call.call_keys.push(key.clone());
        call.call_keys.sort();
        call.call_keys.dedup();
        // The Prepared frame owns late execution even after its run completes or
        // another episode starts. Its sequence, not the active run, names the key.
        self.prepared
            .entry(key)
            .or_insert((self.active.clone(), call));
    }

    fn remember_unowned(&mut self, mut call: UnownedRecoveryCall) {
        call.call_keys.sort();
        call.call_keys.dedup();
        if !self.unowned.contains(&call) {
            self.unowned.push(call);
        }
    }

    fn remember(&mut self, run: Option<RunId>, call: RecoveryCall) {
        if let Some(run) = run {
            self.add(run, call);
        } else {
            self.remember_unowned(UnownedRecoveryCall {
                outcome: RecoveryOutcome::Unknown,
                tool_call_id: Some(call.tool_call_id),
                step_id: call.step_id,
                batch_id: call.batch_id,
                call_keys: call.call_keys,
            });
        }
    }

    fn merge_run(&mut self, run: &RecoveryRun) {
        if !self.runs.iter().any(|r| r.run_id == run.run_id) {
            self.runs.push(RecoveryRun {
                run_id: run.run_id.clone(),
                calls: Vec::new(),
            });
        }
        for call in &run.calls {
            self.add(run.run_id.clone(), call.clone());
        }
    }
    fn add(&mut self, run: RunId, call: RecoveryCall) {
        let index = self
            .runs
            .iter()
            .position(|r| r.run_id == run)
            .unwrap_or_else(|| {
                self.runs.push(RecoveryRun {
                    run_id: run,
                    calls: Vec::new(),
                });
                self.runs.len() - 1
            });
        if let Some(existing) = self.runs[index]
            .calls
            .iter_mut()
            .find(|c| c.tool_call_id == call.tool_call_id)
        {
            if existing.step_id.is_none() {
                existing.step_id = call.step_id;
            }
            if existing.batch_id.is_none() {
                existing.batch_id = call.batch_id;
            }
            existing.call_keys.extend(call.call_keys);
            existing.call_keys.sort();
            existing.call_keys.dedup();
        } else {
            self.runs[index].calls.push(call);
        }
    }
    fn observe(&mut self, record: &WalRecord, removed: bool, source_key: Option<String>) {
        match record {
            WalRecord::RunStarted { run_id, .. } => {
                self.active = Some(run_id.clone());
                self.planned.clear();
                if removed && !self.runs.iter().any(|run| run.run_id == *run_id) {
                    self.runs.push(RecoveryRun {
                        run_id: run_id.clone(),
                        calls: Vec::new(),
                    });
                }
            }
            WalRecord::ModelStepFinished {
                step_id,
                assistant_message,
                ..
            } if !removed => {
                self.planned = assistant_message
                    .tool_calls()
                    .map(|call| RecoveryCall {
                        tool_call_id: call.tool_call_id.clone(),
                        step_id: Some(*step_id),
                        batch_id: None,
                        call_keys: Vec::new(),
                    })
                    .collect();
            }
            WalRecord::ToolBatchStarted { batch_id, .. } if !removed => {
                for call in &mut self.planned {
                    call.batch_id = Some(batch_id.clone());
                }
            }
            WalRecord::ToolResult { result, .. } if !removed => self
                .planned
                .retain(|c| c.tool_call_id != result.tool_call_id),
            WalRecord::ToolNoDispatch { tool_call_id, .. } if !removed => {
                self.planned.retain(|c| &c.tool_call_id != tool_call_id)
            }
            WalRecord::ToolDispatchIntent {
                tool_call_id,
                batch_id,
                call_key,
                ..
            } => {
                let mut keys: Vec<_> = call_key.iter().cloned().chain(source_key).collect();
                keys.sort();
                keys.dedup();
                if removed {
                    self.remember(
                        self.active.clone(),
                        RecoveryCall {
                            tool_call_id: tool_call_id.clone(),
                            step_id: None,
                            batch_id: Some(batch_id.clone()),
                            call_keys: keys,
                        },
                    );
                } else if let Some(call) = self
                    .planned
                    .iter_mut()
                    .find(|call| &call.tool_call_id == tool_call_id)
                {
                    call.call_keys.extend(keys);
                    call.call_keys.sort();
                    call.call_keys.dedup();
                }
            }
            WalRecord::ToolResult {
                batch_id,
                result,
                provider_held,
                ..
            } if removed => {
                self.remember(
                    self.active.clone(),
                    RecoveryCall {
                        tool_call_id: result.tool_call_id.clone(),
                        step_id: None,
                        batch_id: Some(batch_id.clone()),
                        call_keys: provider_held
                            .iter()
                            .map(|held| held.call_key.clone())
                            .collect(),
                    },
                );
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } if removed => {
                self.remember(
                    self.active.clone(),
                    RecoveryCall {
                        tool_call_id: tool_call_id.clone(),
                        step_id: None,
                        batch_id: Some(batch_id.clone()),
                        call_keys: Vec::new(),
                    },
                );
            }
            WalRecord::ToolCallProviderHeld {
                batch_id,
                tool_call_id,
                call_key,
                ..
            } if removed => {
                self.remember(
                    self.active.clone(),
                    RecoveryCall {
                        tool_call_id: tool_call_id.clone(),
                        step_id: None,
                        batch_id: Some(batch_id.clone()),
                        call_keys: vec![call_key.clone()],
                    },
                );
            }
            WalRecord::LateDispatchIntent { call_key, .. }
            | WalRecord::LateResult { call_key, .. }
                if removed =>
            {
                if let Some((run, call)) = self.prepared.get(call_key).cloned() {
                    self.remember(run, call);
                } else {
                    self.remember_unowned(UnownedRecoveryCall {
                        outcome: RecoveryOutcome::Unknown,
                        tool_call_id: None,
                        step_id: None,
                        batch_id: None,
                        call_keys: vec![call_key.clone()],
                    });
                }
            }
            WalRecord::RecoveryCut { recovery } => {
                if !removed {
                    self.cut_ids.insert(recovery.cut_id.clone());
                }
                for run in &recovery.runs {
                    self.merge_run(run);
                }
                for call in &recovery.unowned_calls {
                    self.remember_unowned(call.clone());
                }
            }
            WalRecord::ToolBatchFinished { .. } if !removed => self.planned.clear(),
            WalRecord::RunFinished { .. } => {
                self.planned.clear();
                self.active = None;
            }
            _ => {}
        }
    }
    fn freeze_planned(&mut self) {
        if let Some(run) = self.active.clone() {
            for call in std::mem::take(&mut self.planned) {
                self.add(run.clone(), call);
            }
        }
    }
}

struct Prefix {
    end: usize,
    seq: u64,
    lineage: Option<framing::WalLineageId>,
    calls: Calls,
    damaged: bool,
}

fn prefix(bytes: &[u8]) -> Result<Prefix, WalError> {
    let mut scan = Prefix {
        end: 0,
        seq: 0,
        lineage: None,
        calls: Calls::default(),
        damaged: false,
    };
    while scan.end < bytes.len() {
        match framing::decode_next(&bytes[scan.end..]) {
            DecodeStep::Record { framed, consumed } => {
                if bytes[scan.end + 4] != framing::V4_FRAMING_VERSION {
                    return Err(WalError::FrameFormat {
                        log: crate::WalFormat::Overhaul,
                        offered: crate::WalFormat::Legacy,
                    });
                }
                if framed.seq != scan.seq + 1 {
                    scan.damaged = true;
                    break;
                }
                scan.seq = framed.seq;
                scan.calls.anchor_prepared(&framed, scan.lineage);
                scan.calls.observe(
                    &framed.record,
                    false,
                    scan.lineage.map(|id| format!("{id}:{}", framed.seq)),
                );
                scan.end += consumed;
            }
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } => {
                if let Some(first) = scan.lineage {
                    if first != lineage_id {
                        return Err(WalError::ConflictingLineageIds {
                            first,
                            found: lineage_id,
                        });
                    }
                }
                scan.lineage = Some(lineage_id);
                scan.end += consumed;
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: scan.end,
                    ver,
                    missing,
                })
            }
            DecodeStep::Corrupt(_) | DecodeStep::TornTail => {
                scan.damaged = true;
                break;
            }
        }
    }
    Ok(scan)
}

fn inspect_removed(bytes: &[u8], scan: &mut Prefix) -> Result<(), WalError> {
    scan.calls.freeze_planned();
    let mut lineage = scan.lineage;
    let mut offset = 0;
    while offset < bytes.len() {
        match framing::decode_next(&bytes[offset..]) {
            DecodeStep::Record { framed, consumed } => {
                scan.calls.anchor_prepared(&framed, lineage);
                scan.calls.observe(
                    &framed.record,
                    true,
                    lineage.map(|id| format!("{id}:{}", framed.seq)),
                );
                offset += consumed;
            }
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } => {
                if let Some(first) = lineage {
                    if first != lineage_id {
                        return Err(WalError::ConflictingLineageIds {
                            first,
                            found: lineage_id,
                        });
                    }
                } else {
                    lineage = Some(lineage_id);
                }
                offset += consumed;
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: scan.end + offset,
                    ver,
                    missing,
                })
            }
            _ => {
                // Salvage only a v4 envelope's complete JSON payload, never
                // arbitrary nested JSON in arguments or assistant content.
                let tail = &bytes[offset..];
                if tail.get(4) == Some(&framing::V4_FRAMING_VERSION) {
                    if let Some(header_len) = framing::v4_header_len(tail) {
                        let len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
                        if len <= framing::MAX_PAYLOAD_LEN as usize {
                            if let Some(payload) =
                                tail.get(header_len..header_len.saturating_add(len))
                            {
                                if let Ok(
                                    record @ (WalRecord::ToolDispatchIntent { .. }
                                    | WalRecord::LateDispatchIntent { .. }
                                    | WalRecord::ToolResult { .. }
                                    | WalRecord::ToolNoDispatch { .. }
                                    | WalRecord::LateResult { .. }
                                    | WalRecord::ToolCallProviderHeld { .. }
                                    | WalRecord::RunStarted { .. }),
                                ) = serde_json::from_slice::<WalRecord>(payload)
                                {
                                    scan.calls.observe(&record, true, None);
                                }
                            }
                        }
                    }
                }
                offset += 1;
            }
        }
    }
    Ok(())
}

fn read_capped(path: &Path, cap: u64) -> Result<Vec<u8>, WalError> {
    let mut bytes = Vec::new();
    let file = File::open(path).map_err(WalError::Io)?;
    let size = file.metadata().map_err(WalError::Io)?.len();
    if size > cap {
        return Err(WalError::TooLarge {
            max_bytes: cap,
            file_size: size,
        });
    }
    file.take(cap.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(WalError::Io)?;
    if bytes.len() as u64 > cap {
        return Err(WalError::TooLarge {
            max_bytes: cap,
            file_size: bytes.len() as u64,
        });
    }
    Ok(bytes)
}

fn atomic_file(path: &Path, bytes: &[u8]) -> Result<(), WalError> {
    let temporary = suffixed(path, ".tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(WalError::Io)?;
    file.write_all(bytes).map_err(WalError::Io)?;
    file.sync_all().map_err(WalError::Io)?;
    drop(file);
    fs_ops::rename(&temporary, path).map_err(WalError::Io)?;
    fs_ops::sync_dir(path.parent().unwrap_or_else(|| Path::new("."))).map_err(WalError::Io)
}

pub fn manifests(log: &Path, cap: u64) -> Result<Vec<CutManifest>, WalError> {
    let Some(parent) = log.parent() else {
        return Ok(Vec::new());
    };
    let prefix = format!(
        "{}.cut-",
        log.file_name().unwrap_or_default().to_string_lossy()
    );
    let entries = match fs_ops::read_dir(parent) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(WalError::Io(e)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(WalError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&prefix) && name.ends_with(".json") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let manifest: CutManifest =
                serde_json::from_slice(&read_capped(&path, cap)?).map_err(|e| {
                    WalError::Io(std::io::Error::other(format!(
                        "invalid recovery manifest {}: {e}",
                        path.display()
                    )))
                })?;
            let raw = parent.join(&manifest.recovery.sidecar_basename);
            if !manifest.integrity_valid()
                || raw.parent() != Some(parent)
                || manifest.recovery.sidecar_basename.contains(['/', '\\'])
            {
                return Err(WalError::Io(std::io::Error::other(format!(
                    "invalid recovery manifest {}",
                    path.display()
                ))));
            }
            let bytes = read_capped(&raw, cap)?;
            if bytes.len() as u64 != manifest.recovery.removed_len
                || digest(&bytes) != manifest.recovery.removed_sha256
            {
                return Err(WalError::Io(std::io::Error::other(format!(
                    "recovery sidecar {} is incomplete or corrupt",
                    raw.display()
                ))));
            }
            Ok(manifest)
        })
        .collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

fn next_sidecar(log: &Path) -> Result<PathBuf, WalError> {
    if log.file_name().and_then(|name| name.to_str()).is_none() {
        return Err(WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cannot encode recovery sidecar basename for {log:?}"),
        )));
    }
    let mut stamp = now_ms();
    loop {
        let path = suffixed(log, &format!(".cut-{stamp}"));
        if !fs_ops::try_exists(&path).map_err(WalError::Io)?
            && !fs_ops::try_exists(&suffixed(&path, ".tmp")).map_err(WalError::Io)?
            && !fs_ops::try_exists(&suffixed(&path, ".json")).map_err(WalError::Io)?
        {
            return Ok(path);
        }
        stamp = stamp.checked_add(1).ok_or_else(|| {
            WalError::Io(std::io::Error::other(
                "recovery sidecar timestamp exhausted",
            ))
        })?;
    }
}

fn read_head(log: &Path) -> Option<crate::head_proof::HeadProof> {
    let mut bytes = Vec::new();
    File::open(crate::head_proof::sidecar_path(log))
        .ok()?
        .take(broca_wal_reader::proof::PROOF_LEN as u64 + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    crate::head_proof::HeadProof::decode(&bytes)
}

/// Run before the append handle is opened. Only a supplied boot decision enables
/// overhaul recovery; unconfigured legacy opens must not probe recovery evidence.
#[cfg(test)]
pub(crate) fn before_open(
    log: &Path,
    config: Option<crate::WalConfig>,
    floor: Option<&Path>,
    session: &str,
    cap: u64,
) -> Result<(), WalError> {
    before_open_with_hooks(log, config, floor, session, cap, None)
}

pub(crate) fn before_open_with_hooks(
    log: &Path,
    config: Option<crate::WalConfig>,
    floor: Option<&Path>,
    session: &str,
    cap: u64,
    hooks: Option<&dyn RecoveryHooks>,
) -> Result<(), WalError> {
    let overhaul = config.is_some_and(|config| config.is_overhaul());
    if !overhaul {
        return Ok(());
    }
    let (bytes, missing) = match read_capped(log, cap) {
        Ok(bytes) => (bytes, false),
        Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (Vec::new(), true),
        Err(e) => return Err(e),
    };
    let mut scan = prefix(&bytes)?;
    let session_name = if session.is_empty() {
        log.display().to_string()
    } else {
        session.to_owned()
    };
    let mut pending = Vec::new();
    let stored_manifests = manifests(log, cap)?;
    let has_manifests = !stored_manifests.is_empty();
    for manifest in stored_manifests {
        if scan.calls.cut_ids.contains(&manifest.recovery.cut_id) {
            continue;
        }
        let end = usize::try_from(manifest.recovery.cut_offset).unwrap_or(usize::MAX);
        // A later disk fault may remove an earlier recovery marker too. The
        // immutable manifest still attributes those calls to this session. It
        // restores facts only; truncation always follows the current scan, not
        // an older manifest's offset.
        if manifest.session == session_name
            || (end <= scan.end && digest(&bytes[..end]) == manifest.prefix_sha256)
        {
            pending.push(manifest.recovery);
        }
    }
    if !scan.damaged && pending.is_empty() {
        // A previous marker write may have become visible before its sync
        // returned an error. A clean retry must still establish that barrier.
        if has_manifests {
            OpenOptions::new()
                .write(true)
                .open(log)
                .map_err(WalError::Io)?
                .sync_all()
                .map_err(WalError::Io)?;
        }
        if !scan.calls.cut_ids.is_empty() {
            if let Some(hooks) = hooks {
                hooks.after_repair(log, &scan.calls.cut_ids)?;
            }
        }
        return Ok(());
    }
    if config.unwrap_or(crate::WalConfig::OVERHAUL).recovery == RecoveryPolicy::Refuse {
        return Err(WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "wal_recovery=refuse: refusing damaged WAL {} at byte {}; file unchanged",
                log.display(),
                scan.end
            ),
        )));
    }
    let inferred_floor = log
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join(crate::floor::FEATURE_FLOOR_FILENAME);
    let floor = floor.unwrap_or(&inferred_floor);
    crate::floor::raise_floor(floor, &[FEATURE, framing::FRAME_STAMPS_FEATURE])
        .map_err(|e| WalError::Io(std::io::Error::other(e.to_string())))?;
    let repeat = scan.damaged
        && pending.iter().any(|cut| {
            cut.cut_offset == scan.end as u64
                && cut.removed_len == (bytes.len() - scan.end) as u64
                && cut.removed_sha256 == digest(&bytes[scan.end..])
        });
    if scan.damaged && !repeat {
        let active_run = scan.calls.active.clone();
        inspect_removed(&bytes[scan.end..], &mut scan)?;
        for prior in &pending {
            for run in &prior.runs {
                scan.calls.merge_run(run);
            }
            for call in &prior.unowned_calls {
                scan.calls.remember_unowned(call.clone());
            }
        }
        let proof = read_head(log);
        let reason = match proof {
            Some(proof)
                if proof.durable_end <= bytes.len() as u64
                    && scan
                        .lineage
                        .is_some_and(|id| id.as_bytes() == proof.lineage_id) =>
            {
                if (scan.end as u64) < proof.durable_end {
                    CutReason::PublishedRegionCorruption
                } else {
                    CutReason::UnpublishedTail
                }
            }
            _ => CutReason::UnprovenCorruption,
        };
        let raw = next_sidecar(log)?;
        let suffix = &bytes[scan.end..];
        let removed_sha256 = digest(suffix);
        let sidecar_basename = raw.file_name().unwrap().to_string_lossy().into_owned();
        let cut = RecoveryCut {
            cut_id: digest(format!("{sidecar_basename}:{}:{removed_sha256}", scan.end).as_bytes()),
            sidecar_basename,
            cut_offset: scan.end as u64,
            removed_len: suffix.len() as u64,
            removed_sha256,
            reason,
            active_run,
            runs: std::mem::take(&mut scan.calls.runs),
            unowned_calls: std::mem::take(&mut scan.calls.unowned),
            unattributed_tool_risk: true,
        };
        atomic_file(&raw, suffix)?;
        let mut manifest = CutManifest {
            version: 1,
            session: session_name.clone(),
            prefix_sha256: digest(&bytes[..scan.end]),
            recovery: cut.clone(),
            checksum: String::new(),
        };
        manifest.checksum = manifest.expected_checksum()?;
        let encoded =
            serde_json::to_vec(&manifest).map_err(|e| WalError::Io(std::io::Error::other(e)))?;
        atomic_file(&suffixed(&raw, ".json"), &encoded)?;
        pending.push(cut);
    }
    if let Some(hooks) = hooks {
        for cut in &pending {
            hooks.before_cut(cut)?;
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(missing)
        .open(log)
        .map_err(WalError::Io)?;
    if scan.damaged {
        file.set_len(scan.end as u64).map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        crate::file_wal::recovery_truncated();
    }
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::End(0)).map_err(WalError::Io)?;
    let lineage = match scan.lineage {
        Some(lineage) => lineage,
        None => {
            let lineage = crate::file_wal::mint_lineage_id()?;
            file.write_all(&framing::encode_lineage(lineage))
                .map_err(WalError::Io)?;
            lineage
        }
    };
    let mut writer_id = [0; 16];
    getrandom::fill(&mut writer_id)
        .map_err(|e| WalError::Io(std::io::Error::other(e.to_string())))?;
    for cut in pending {
        if scan.calls.cut_ids.contains(&cut.cut_id) {
            continue;
        }
        let record = WalRecord::RecoveryCut {
            recovery: Box::new(cut.clone()),
        };
        let prepared = framing::PreparedV4Frame::prepare(&record).map_err(WalError::Frame)?;
        scan.seq += 1;
        let mut frame = prepared
            .stamp(framing::v4::Metadata {
                seq: scan.seq,
                fence: 0,
                appended_ms: now_ms(),
                writer_id,
            })
            .map_err(WalError::Frame)?;
        frame.extend_from_slice(&framing::encode_lineage(lineage));
        file.write_all(&frame).map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        if missing {
            fs_ops::sync_dir(log.parent().unwrap_or_else(|| Path::new(".")))
                .map_err(WalError::Io)?;
        }
        tracing::warn!(session = %session_name,
            wal = %log.display(), reason = cut.reason.as_str(), cut_offset = cut.cut_offset,
            removed_bytes = cut.removed_len, unattributed_tool_risk = cut.unattributed_tool_risk,
            "WAL recovery preserved a removed suffix and recorded indeterminate-call evidence");
        scan.calls.cut_ids.insert(cut.cut_id.clone());
    }
    if let Some(hooks) = hooks {
        hooks.after_repair(log, &scan.calls.cut_ids)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_retains_manifest_tombstones_when_the_log_itself_is_missing() {
        let root = broca_test_support::scratch_root().join(format!(
            "missing-recovery-log-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let log = root.join("wal/log");
        let floor = root.join(crate::FEATURE_FLOOR_FILENAME);
        let start: WalRecord = serde_json::from_str(include_str!(
            "../tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        let intent: WalRecord = serde_json::from_value(serde_json::json!({"type":"tool_dispatch_intent", "batch_id":"b", "tool_call_id":"retained-id", "args":{}})).unwrap();
        let mut bytes =
            framing::encode_lineage(framing::WalLineageId::from_bytes([3; 16])).to_vec();
        for (index, record) in [start, intent].iter().enumerate() {
            if index == 1 {
                bytes.extend_from_slice(b"broken frame bytes");
            }
            bytes.extend_from_slice(
                &framing::PreparedV4Frame::prepare(record)
                    .unwrap()
                    .stamp(framing::v4::Metadata {
                        seq: index as u64 + 1,
                        fence: 0,
                        appended_ms: 1,
                        writer_id: [1; 16],
                    })
                    .unwrap(),
            );
            bytes.extend_from_slice(&framing::encode_lineage(framing::WalLineageId::from_bytes(
                [3; 16],
            )));
        }
        std::fs::write(&log, bytes).unwrap();
        before_open(
            &log,
            Some(crate::WalConfig::OVERHAUL),
            Some(&floor),
            "session",
            1_000_000,
        )
        .unwrap();
        let saved = manifests(&log, 1_000_000).unwrap();
        assert_eq!(saved.len(), 1);
        assert!(saved[0]
            .recovery
            .runs
            .iter()
            .flat_map(|run| &run.calls)
            .any(|call| call.tool_call_id.0 == "retained-id"));
        let raw = std::fs::read(
            log.parent()
                .unwrap()
                .join(&saved[0].recovery.sidecar_basename),
        )
        .unwrap();
        std::fs::remove_file(&log).unwrap();
        let mut refuse = crate::WalConfig::OVERHAUL;
        refuse.recovery = RecoveryPolicy::Refuse;
        assert!(before_open(&log, Some(refuse), Some(&floor), "session", 1_000_000).is_err());
        assert!(!log.exists());
        before_open(
            &log,
            Some(crate::WalConfig::OVERHAUL),
            Some(&floor),
            "session",
            1_000_000,
        )
        .unwrap();
        let records = crate::file_wal::replay_file(&log, 1_000_000).unwrap();
        assert!(records.iter().any(|frame| matches!(&frame.record, WalRecord::RecoveryCut { recovery }
            if recovery.cut_id == saved[0].recovery.cut_id && recovery.runs == saved[0].recovery.runs)));
        assert_eq!(
            std::fs::read(
                log.parent()
                    .unwrap()
                    .join(&saved[0].recovery.sidecar_basename)
            )
            .unwrap(),
            raw
        );
        let manifest_path = suffixed(
            &log.parent()
                .unwrap()
                .join(&saved[0].recovery.sidecar_basename),
            ".json",
        );
        let mut altered: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        altered["session"] = serde_json::json!("changed-without-a-new-checksum");
        std::fs::write(manifest_path, serde_json::to_vec(&altered).unwrap()).unwrap();
        let before = std::fs::read(&log).unwrap();
        assert!(before_open(
            &log,
            Some(crate::WalConfig::OVERHAUL),
            Some(&floor),
            "session",
            1_000_000
        )
        .is_err());
        assert_eq!(
            std::fs::read(&log).unwrap(),
            before,
            "corrupt attribution must never be silently trusted"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_does_not_interpret_nested_argument_json_as_a_dispatch_intent() {
        let fake = serde_json::json!({"type":"tool_dispatch_intent", "batch_id":"b",
            "tool_call_id":"nested-not-a-record", "args":{}});
        assert!(matches!(
            serde_json::from_value::<WalRecord>(fake.clone()).unwrap(),
            WalRecord::ToolDispatchIntent { .. }
        ));
        let payload = serde_json::to_vec(&serde_json::json!({"type":"tool_dispatch_intent", "batch_id":"b", "args":{"nested":fake}})).unwrap();
        assert!(
            serde_json::from_slice::<WalRecord>(&payload).is_err(),
            "outer record deliberately lacks its call id"
        );
        let bytes = framing::v4::encode(
            framing::v4::Metadata {
                seq: 1,
                fence: 0,
                appended_ms: 1,
                writer_id: [1; 16],
            },
            &[framing::FRAME_STAMPS_FEATURE],
            &payload,
        )
        .unwrap();
        let mut scan = Prefix {
            end: 0,
            seq: 0,
            lineage: None,
            calls: Calls {
                active: Some(RunId("r".into())),
                ..Calls::default()
            },
            damaged: true,
        };
        inspect_removed(&bytes, &mut scan).unwrap();
        assert!(
            scan.calls.runs.is_empty(),
            "nested user data is not WAL attribution"
        );
    }
}
