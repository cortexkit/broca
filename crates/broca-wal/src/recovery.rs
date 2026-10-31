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

/// Physical call identities keep their legacy spelling until a repair. The cut
/// identity separates new calls from removed records whose sequences may recur.
pub fn physical_call_key(lineage: &str, seq: u64, generation: Option<&str>) -> String {
    match generation {
        Some(generation) => format!("{lineage}~{generation}:{seq}"),
        None => format!("{lineage}:{seq}"),
    }
}

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
pub struct RecoveryMarker {
    pub offset: u64,
    pub prefix_sha256: String,
    pub bytes: Vec<u8>,
}

impl RecoveryMarker {
    fn lineage(&self) -> Option<framing::WalLineageId> {
        let mut bytes = self.bytes.as_slice();
        let mut owner = None;
        while !bytes.is_empty() {
            let consumed = match framing::decode_next(bytes) {
                DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } => {
                    if owner.is_some_and(|first| first != lineage_id) {
                        return None;
                    }
                    owner = Some(lineage_id);
                    consumed
                }
                DecodeStep::Record { consumed, .. } => consumed,
                _ => return None,
            };
            bytes = &bytes[consumed..];
        }
        owner
    }
    fn matches_prefix(&self, bytes: &[u8]) -> bool {
        let Ok(offset) = usize::try_from(self.offset) else {
            return false;
        };
        bytes
            .get(..offset)
            .is_some_and(|prefix| digest(prefix) == self.prefix_sha256)
            && bytes.get(offset..).is_some_and(|tail| {
                tail.len() <= self.bytes.len()
                    && tail
                        .iter()
                        .zip(&self.bytes)
                        .all(|(found, expected)| *found == 0 || found == expected)
            })
    }

    fn cuts(&self) -> Result<Vec<RecoveryCut>, WalError> {
        let mut bytes = self.bytes.as_slice();
        let mut cuts = Vec::new();
        while !bytes.is_empty() {
            let consumed = match framing::decode_next(bytes) {
                DecodeStep::Lineage { consumed, .. } => consumed,
                DecodeStep::Record { framed, consumed } => {
                    if let WalRecord::RecoveryCut { recovery } = framed.record {
                        cuts.push(*recovery);
                    } else {
                        return Err(WalError::Io(std::io::Error::other(
                            "recovery marker contains a non-recovery record",
                        )));
                    }
                    consumed
                }
                _ => {
                    return Err(WalError::Io(std::io::Error::other(
                        "recovery marker bytes are incomplete or corrupt",
                    )))
                }
            };
            bytes = &bytes[consumed..];
        }
        Ok(cuts)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CutManifest {
    pub version: u32,
    pub session: String,
    pub lineage_id: [u8; 16],
    pub prefix_sha256: String,
    pub recovery: RecoveryCut,
    pub marker: RecoveryMarker,
    pub checksum: String,
}

impl CutManifest {
    fn expected_checksum(&self) -> Result<String, WalError> {
        serde_json::to_vec(&(
            self.version,
            &self.session,
            &self.lineage_id,
            &self.prefix_sha256,
            &self.recovery,
            &self.marker,
        ))
        .map(|bytes| digest(&bytes))
        .map_err(|e| WalError::Io(std::io::Error::other(e)))
    }

    pub fn integrity_valid(&self) -> bool {
        self.version == 1
            && self
                .marker
                .lineage()
                .is_some_and(|lineage| lineage.as_bytes() == self.lineage_id)
            && self.marker.offset == self.recovery.cut_offset
            && self.marker.prefix_sha256 == self.prefix_sha256
            && self
                .marker
                .cuts()
                .is_ok_and(|cuts| cuts.contains(&self.recovery))
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
    recovery_generation: Option<String>,
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
        let key = physical_call_key(
            &lineage.to_string(),
            frame.seq,
            self.recovery_generation.as_deref(),
        );
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
                self.recovery_generation = Some(recovery.cut_id.clone());
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

fn prefix(bytes: &[u8], require_v4: bool) -> Result<Prefix, WalError> {
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
                if require_v4 && bytes[scan.end + 4] != framing::V4_FRAMING_VERSION {
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
                    scan.lineage.map(|id| {
                        physical_call_key(
                            &id.to_string(),
                            framed.seq,
                            scan.calls.recovery_generation.as_deref(),
                        )
                    }),
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

fn inspect_removed(bytes: &[u8], scan: &mut Prefix) -> Result<bool, WalError> {
    scan.calls.freeze_planned();
    let mut lineage = scan.lineage;
    let mut offset = 0;
    let mut undecodable = false;
    while offset < bytes.len() {
        match framing::decode_next(&bytes[offset..]) {
            DecodeStep::Record { framed, consumed } => {
                scan.calls.anchor_prepared(&framed, lineage);
                scan.calls.observe(
                    &framed.record,
                    true,
                    lineage.map(|id| {
                        physical_call_key(
                            &id.to_string(),
                            framed.seq,
                            scan.calls.recovery_generation.as_deref(),
                        )
                    }),
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
                undecodable = true;
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
    Ok(undecodable)
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
            let encoded = read_capped(&path, cap).map_err(|error| {
                WalError::Io(std::io::Error::other(format!(
                    "cannot read recovery manifest {}: {error}",
                    path.display()
                )))
            })?;
            let manifest: CutManifest = serde_json::from_slice(&encoded).map_err(|e| {
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
            // The manifest was published only after the raw bytes were synced.
            // Later loss of raw evidence must not erase the durable identities of
            // calls whose external effects are unknown and must not be repeated.
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

fn next_sidecar(log: &Path, lineage: framing::WalLineageId) -> Result<PathBuf, WalError> {
    if log.file_name().and_then(|name| name.to_str()).is_none() {
        return Err(WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cannot encode recovery sidecar basename for {log:?}"),
        )));
    }
    let mut stamp = now_ms();
    loop {
        let path = suffixed(log, &format!(".cut-{lineage}-{stamp}"));
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

fn proven_head(
    log: &Path,
    bytes: &[u8],
    lineage: Option<framing::WalLineageId>,
) -> Option<crate::head_proof::HeadProof> {
    let proof = read_head(log)?;
    if proof.durable_end > bytes.len() as u64
        || !lineage.is_some_and(|id| id.as_bytes() == proof.lineage_id)
    {
        return None;
    }
    match crate::head_proof::PublishedLog::open(bytes, &proof.encode()) {
        Ok(Some(_)) => Some(proof),
        Err(broca_wal_reader::proof::ProofError::Header { .. }) => Some(proof),
        Err(broca_wal_reader::proof::ProofError::CorruptPrefix {
            reason: "noncontiguous record sequence" | "corrupt or conflicting lineage metadata",
            ..
        }) => Some(proof),
        _ => None,
    }
}

/// Summary of the normal streaming scan. A decode refusal leaves no valid end
/// and enters the exceptional recovery path; a clean file reuses that scan.
pub(crate) struct OpenInspection {
    pub file_len: u64,
    pub valid_end: Option<u64>,
    pub lineage: Option<framing::WalLineageId>,
    pub legacy_records: bool,
    pub missing: bool,
}

fn prepare_marker(
    prefix: &[u8],
    mut seq: u64,
    lineage: framing::WalLineageId,
    initial_lineage: bool,
    cuts: &[RecoveryCut],
) -> Result<RecoveryMarker, WalError> {
    let mut bytes = Vec::new();
    if initial_lineage {
        bytes.extend_from_slice(&framing::encode_lineage(lineage));
    }
    let mut writer_id = [0; 16];
    getrandom::fill(&mut writer_id)
        .map_err(|e| WalError::Io(std::io::Error::other(e.to_string())))?;
    for cut in cuts {
        let prepared = framing::PreparedV4Frame::prepare(&WalRecord::RecoveryCut {
            recovery: Box::new(cut.clone()),
        })
        .map_err(WalError::Frame)?;
        seq += 1;
        bytes.extend_from_slice(
            &prepared
                .stamp(framing::v4::Metadata {
                    seq,
                    fence: 0,
                    appended_ms: now_ms(),
                    writer_id,
                })
                .map_err(WalError::Frame)?,
        );
        bytes.extend_from_slice(&framing::encode_lineage(lineage));
    }
    Ok(RecoveryMarker {
        offset: prefix.len() as u64,
        prefix_sha256: digest(prefix),
        bytes,
    })
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
    before_open_with_hooks(log, config, floor, session, cap, None, None).map(|_| ())
}

pub(crate) fn before_open_with_hooks(
    log: &Path,
    config: Option<crate::WalConfig>,
    floor: Option<&Path>,
    session: &str,
    cap: u64,
    hooks: Option<&dyn RecoveryHooks>,
    inspection: Option<&OpenInspection>,
) -> Result<bool, WalError> {
    let overhaul = config.is_some_and(|config| config.is_overhaul());
    if !overhaul {
        return Ok(false);
    }
    let refuse = config.is_some_and(|config| config.recovery == RecoveryPolicy::Refuse);
    let mut stored_manifests = manifests(log, cap)?;
    if inspection.is_some_and(|inspection| inspection.missing) && !stored_manifests.is_empty() {
        return Err(WalError::Io(std::io::Error::other(format!(
            "cannot bind recovery manifests for missing WAL {} to a lineage",
            log.display()
        ))));
    }
    if let Some(lineage) = inspection.and_then(|inspection| inspection.lineage) {
        stored_manifests.retain(|manifest| manifest.lineage_id == lineage.as_bytes());
    }
    if stored_manifests.is_empty() {
        if refuse {
            if inspection.is_none() {
                let bytes = match read_capped(log, cap) {
                    Ok(bytes) => bytes,
                    Err(WalError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        Vec::new()
                    }
                    Err(error) => return Err(error),
                };
                crate::file_wal::recover_prefix(&bytes, 0, None, None)?;
            }
            return Ok(false);
        }
        if let Some(inspection) = inspection {
            if inspection.legacy_records {
                return Err(WalError::FrameFormat {
                    log: crate::WalFormat::Overhaul,
                    offered: crate::WalFormat::Legacy,
                });
            }
            if inspection.valid_end == Some(inspection.file_len) {
                return Ok(false);
            }
            if let Some(end) = inspection.valid_end {
                if inspection.lineage.is_none()
                    && read_head(log).is_some_and(|proof| {
                        proof.durable_end <= inspection.file_len && proof.durable_end > end
                    })
                {
                    return Err(WalError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData,
                        format!("WAL {} has a head proof covering an unreadable lineage; refusing ambiguous tail loss", log.display()))));
                }
                let protected = read_head(log).is_some_and(|proof| {
                    proof.durable_end <= inspection.file_len
                        && proof.durable_end > end
                        && inspection
                            .lineage
                            .is_some_and(|id| id.as_bytes() == proof.lineage_id)
                });
                if !protected {
                    return Ok(false);
                }
            }
        }
    }
    let (bytes, missing) = match read_capped(log, cap) {
        Ok(bytes) => (bytes, false),
        Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => (Vec::new(), true),
        Err(e) => return Err(e),
    };
    if missing && !stored_manifests.is_empty() {
        return Err(WalError::Io(std::io::Error::other(format!(
            "cannot bind recovery manifests for missing WAL {} to a lineage",
            log.display()
        ))));
    }
    let mut scan = prefix(&bytes, !refuse)?;
    let session_name = if session.is_empty() {
        log.display().to_string()
    } else {
        session.to_owned()
    };
    let mut pending = Vec::new();
    let has_manifests = !stored_manifests.is_empty();
    let mut retry_candidates = Vec::new();
    for manifest in &stored_manifests {
        // Only byte-for-byte evidence at the original bound prefix can authorize
        // completing an interrupted repair. Zero bytes model an unpersisted block.
        let end = usize::try_from(manifest.recovery.cut_offset).unwrap_or(usize::MAX);
        let original_tail = end == scan.end
            && digest(&bytes[..scan.end]) == manifest.prefix_sha256
            && manifest.recovery.removed_len == (bytes.len() - scan.end) as u64
            && manifest.recovery.removed_sha256 == digest(&bytes[scan.end..]);
        let marker_tail = manifest.marker.matches_prefix(&bytes);
        let same_lineage = scan
            .lineage
            .is_some_and(|id| id.as_bytes() == manifest.lineage_id);
        let bound = same_lineage || (scan.lineage.is_none() && (marker_tail || original_tail));
        if !bound {
            continue;
        }
        if marker_tail || (scan.damaged && original_tail) {
            retry_candidates.push(manifest.marker.clone());
        }
        if scan.calls.cut_ids.contains(&manifest.recovery.cut_id) {
            continue;
        }
        // A later disk fault may remove an earlier recovery marker too. The
        // immutable manifest still attributes those calls to this session. It
        // restores facts only; truncation always follows the current scan, not
        // an older manifest's offset.
        pending.push(manifest.recovery.clone());
    }
    // A later repair can re-assert an earlier cut. Its marker must cover every
    // pending cut; filenames and wall-clock timestamps are not ordering authority.
    let retry = retry_candidates
        .iter()
        .filter(|marker| {
            marker.cuts().is_ok_and(|cuts| {
                pending
                    .iter()
                    .all(|required| cuts.iter().any(|cut| cut.cut_id == required.cut_id))
            })
        })
        .max_by_key(|marker| marker.offset)
        .cloned();
    if !retry_candidates.is_empty() && retry.is_none() {
        return Err(WalError::Io(std::io::Error::other(format!(
            "recovery markers for WAL {} do not cover all pending cuts; refusing ambiguous repair",
            log.display()
        ))));
    }
    if has_manifests {
        // The cut manifest rename may have succeeded before its directory sync failed.
        // Visibility alone must never authorize destroying the original suffix.
        fs_ops::sync_dir(log.parent().unwrap_or_else(|| Path::new("."))).map_err(WalError::Io)?;
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
        return Ok(false);
    }
    if retry.is_none() && pending.is_empty() {
        if let Ok(baseline) = crate::file_wal::recover_prefix(&bytes, 0, None, None) {
            if scan.lineage.is_none()
                && read_head(log).is_some_and(|proof| {
                    proof.durable_end <= bytes.len() as u64
                        && proof.durable_end > baseline.valid_end
                })
            {
                return Err(WalError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData,
                    format!("WAL {} has a head proof covering an unreadable lineage; refusing ambiguous tail loss", log.display()))));
            }
            if baseline.valid_end < bytes.len() as u64
                && !proven_head(log, &bytes, scan.lineage)
                    .is_some_and(|proof| proof.durable_end > baseline.valid_end)
            {
                return Ok(false);
            }
        }
    }
    if config.unwrap_or(crate::WalConfig::OVERHAUL).recovery == RecoveryPolicy::Refuse
        && retry.is_none()
    {
        // Preserve the original decoder's typed errors and final-tail behavior.
        // Refuse never invents a recovery cut for newly discovered damage.
        crate::file_wal::recover_prefix(&bytes, 0, None, None)?;
        if pending.is_empty() {
            return Ok(false);
        }
        return Err(WalError::Io(std::io::Error::other(format!(
            "wal_recovery=refuse: WAL {} no longer matches its pending recovery marker prefix",
            log.display()
        ))));
    }
    let inferred_floor = log
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .join(crate::floor::FEATURE_FLOOR_FILENAME);
    let floor = floor.unwrap_or(&inferred_floor);
    crate::floor::raise_floor(floor, &[FEATURE, framing::FRAME_STAMPS_FEATURE])
        .map_err(|e| WalError::Io(std::io::Error::other(e.to_string())))?;
    let completing = retry.is_some();
    let marker = if let Some(marker) = retry {
        // A partial marker may contain complete earlier records in the same
        // recovery batch. Rewind the batch, not just the first undecodable byte.
        scan = prefix(&bytes[..marker.offset as usize], !refuse)?;
        marker
    } else {
        let active_run = scan.calls.active.clone();
        let unattributed_tool_risk = inspect_removed(&bytes[scan.end..], &mut scan)?;
        for prior in &pending {
            for run in &prior.runs {
                scan.calls.merge_run(run);
            }
            for call in &prior.unowned_calls {
                scan.calls.remember_unowned(call.clone());
            }
        }
        let proof = proven_head(log, &bytes, scan.lineage);
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
        let lineage = match scan.lineage {
            Some(lineage) => lineage,
            None => crate::file_wal::mint_lineage_id()?,
        };
        let raw = next_sidecar(log, lineage)?;
        let suffix = &bytes[scan.end..];
        let removed_sha256 = digest(suffix);
        let prefix_sha256 = digest(&bytes[..scan.end]);
        let sidecar_basename = raw.file_name().unwrap().to_string_lossy().into_owned();
        let cut = RecoveryCut {
            // Invalidation can survive a crash before manifest publication. The
            // same WAL image must reproduce that fence ID, regardless of clocks
            // or the new raw-backup filename selected by its retry.
            cut_id: digest(
                format!(
                    "{session_name}:{}:{prefix_sha256}:{removed_sha256}",
                    scan.end
                )
                .as_bytes(),
            ),
            sidecar_basename,
            cut_offset: scan.end as u64,
            removed_len: suffix.len() as u64,
            removed_sha256,
            reason,
            active_run,
            runs: std::mem::take(&mut scan.calls.runs),
            unowned_calls: std::mem::take(&mut scan.calls.unowned),
            unattributed_tool_risk,
        };
        atomic_file(&raw, suffix)?;
        pending.push(cut.clone());
        let marker = prepare_marker(
            &bytes[..scan.end],
            scan.seq,
            lineage,
            scan.lineage.is_none(),
            &pending,
        )?;
        if let Some(hooks) = hooks {
            for cut in marker.cuts()? {
                hooks.before_cut(&cut)?;
            }
        }
        let mut manifest = CutManifest {
            version: 1,
            session: session_name.clone(),
            lineage_id: lineage.as_bytes(),
            prefix_sha256,
            recovery: cut.clone(),
            marker: marker.clone(),
            checksum: String::new(),
        };
        manifest.checksum = manifest.expected_checksum()?;
        let encoded =
            serde_json::to_vec(&manifest).map_err(|e| WalError::Io(std::io::Error::other(e)))?;
        atomic_file(&suffixed(&raw, ".json"), &encoded)?;
        marker
    };
    let pending = marker.cuts()?;
    if completing {
        if let Some(hooks) = hooks {
            for cut in &pending {
                hooks.before_cut(cut)?;
            }
        }
    }
    if read_head(log).is_some_and(|proof| proof.durable_end > marker.offset) {
        match fs_ops::remove_file(crate::head_proof::sidecar_path(log)) {
            Ok(()) => fs_ops::sync_dir(log.parent().unwrap_or_else(|| Path::new(".")))
                .map_err(WalError::Io)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(WalError::Io(error)),
        }
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(missing)
        .open(log)
        .map_err(WalError::Io)?;
    if bytes.len() as u64 != marker.offset {
        file.set_len(marker.offset).map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        crate::file_wal::recovery_truncated();
    }
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::End(0)).map_err(WalError::Io)?;
    file.write_all(&marker.bytes).map_err(WalError::Io)?;
    file.sync_all().map_err(WalError::Io)?;
    if missing {
        fs_ops::sync_dir(log.parent().unwrap_or_else(|| Path::new("."))).map_err(WalError::Io)?;
    }
    for cut in pending {
        tracing::warn!(session = %session_name,
            wal = %log.display(), cut_id = %cut.cut_id, completing,
            reason = cut.reason.as_str(), cut_offset = cut.cut_offset,
            removed_bytes = cut.removed_len, unattributed_tool_risk = cut.unattributed_tool_risk,
            "WAL recovery preserved a removed suffix and recorded indeterminate-call evidence");
        scan.calls.cut_ids.insert(cut.cut_id.clone());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_cannot_bind_old_tombstones_to_a_missing_or_fresh_lineage() {
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
        assert!(before_open(
            &log,
            Some(crate::WalConfig::OVERHAUL),
            Some(&floor),
            "session",
            1_000_000,
        )
        .is_err());
        assert!(!log.exists());
        drop(crate::FileWal::open(&log).unwrap());
        before_open(
            &log,
            Some(crate::WalConfig::OVERHAUL),
            Some(&floor),
            "session",
            1_000_000,
        )
        .unwrap();
        let records = crate::file_wal::replay_file(&log, 1_000_000).unwrap();
        assert!(
            records.is_empty(),
            "a fresh lineage must not inherit removed call identities"
        );
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
