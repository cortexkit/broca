//! `store-load-check`: whether a session of the new storage loads alike through today's load and
//! through a reference built the way loading worked before the load streamed its log.
//!
//! Each session is copied into memory first, so the check writes nothing where it read the
//! session from: a writer's load appends to the log it opens. On the copy, the check opens the
//! session three ways:
//!
//! - **the reference**: the whole open log read and judged at once (`damage::analyse`, which
//!   `log::read` feeds), its records folded from the checkpoint with `session::apply`, a writer's
//!   kept records cut from the whole list at the latest run start, and an id index holding a copy
//!   of every ledger record that names an id, as loading did before;
//! - **a view** (`Session::view`), which keeps every record it read;
//! - **a writer** (`Session::load`), which keeps its latest run's records and finds the records of
//!   tool calls before that run by their place in the log. Opening the writer appends to the copy,
//!   so the reference takes the records it appended in, as the writer did, before the two are
//!   compared.
//!
//! What is compared, for the view and for the writer against the reference: whether the load
//! opens, and the error when it doesn't; the folds (conversation and operations, by their encoded
//! bytes); the next seq and the log's length; the kept records, at every run start and around the
//! first kept seq; `kept_run_start` for every run; `find_id` for every id any open-log ledger
//! record names, call keys included, plus an id nothing names; `find_run_start` for every run an
//! open-log record names; and `ledger_record` for the open log's ledger records. Then the read
//! surfaces the store crate serves on its own, the view's against the writer's: the raw history
//! (`records_between`) and every entry (`entries`), as digests.
//!
//! Sealed history is read through code this check doesn't exercise twice: a lookup's sealed part
//! is taken as the session answers it, and only its open-log part is compared with the reference.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use broca_session_store::backend::SessionBackend;
use broca_session_store::backend::{BackendError, LocalBackend, MemoryBackend, SessionAddr};
use broca_session_store::checkpoint::Checkpoint;
use broca_session_store::codec::{Id128, Message as _};
use broca_session_store::damage::{self, Verdict};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::{self, Residence, SessionTriple};
use broca_session_store::idindex::{self, IdKind};
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::log::ChainBase;
use broca_session_store::record::{Record, Stream};
use broca_session_store::session::{self, Session, SessionError};
use serde::Serialize;

/// Examples kept per difference field and per failure reason.
const EXAMPLES: usize = 5;
/// Ledger records a writer reads back one by one, each a read of its whole open log.
const WRITER_LEDGER_SAMPLE: usize = 8;

/// How one session's check came out.
#[derive(Debug, Default)]
pub struct SessionCheck {
    pub outcome: Outcome,
    /// The open log's records the reference read.
    pub records: u64,
    pub ids_looked_up: u64,
    pub run_starts_looked_up: u64,
    pub ledger_records_looked_up: u64,
    pub surfaces_compared: u64,
    /// Every difference found, as (field, detail). A detail names kinds, seqs and counts, never
    /// an id or a record's content.
    pub differences: Vec<(String, String)>,
}

/// Whether a session was checked, and how its loads ended.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum Outcome {
    /// Both paths opened the session, or both refused it alike; see `differences`.
    #[default]
    Opened,
    /// Both paths refused the session with this same error.
    FailedAlike(String),
    /// An encrypted session, and the deployment key isn't loaded: neither path can open it, so
    /// nothing was compared.
    KeyUnavailable,
    /// The check itself couldn't run: the session's files couldn't be read, for example.
    Failed(String),
}

impl SessionCheck {
    fn differ(&mut self, field: impl Into<String>, detail: impl Into<String>) {
        self.differences.push((field.into(), detail.into()));
    }
}

/// What `store-load-check` reports over every session it checked.
#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub sessions: u64,
    /// Sessions both paths opened with no difference.
    pub agreed: u64,
    /// Sessions with at least one difference.
    pub differed: u64,
    pub records: u64,
    pub ids_looked_up: u64,
    pub run_starts_looked_up: u64,
    pub ledger_records_looked_up: u64,
    pub surfaces_compared: u64,
    /// Differences by field (`view.find_id`, `writer.kept`, ...).
    pub differences: BTreeMap<String, u64>,
    /// The first few differences of each field: the session, then what differed.
    pub difference_examples: BTreeMap<String, Vec<String>>,
    /// Sessions both paths refused with the same error, by error: they agree, and are listed
    /// apart from the sessions that opened.
    pub failed_alike: BTreeMap<String, u64>,
    pub failed_alike_sessions: BTreeMap<String, Vec<String>>,
    /// Encrypted sessions, which can't be opened without the deployment key: not checked, never
    /// counted as agreeing.
    pub key_unavailable: u64,
    pub key_unavailable_sessions: Vec<String>,
    /// Sessions the check couldn't run on, by reason.
    pub failures: BTreeMap<String, u64>,
    pub failure_examples: BTreeMap<String, Vec<String>>,
    /// With `--from-legacy`: what staging the legacy logs did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy: Option<LegacyReport>,
}

/// What staging legacy logs into plain sessions did, with `--from-legacy`.
#[derive(Debug, Default, Serialize)]
pub struct LegacyReport {
    pub logs: u64,
    /// Logs staged and checked.
    pub staged: u64,
    /// Logs not staged, by reason: the reasons a move refuses, a log that starts no run, or a log
    /// that couldn't be read.
    pub not_staged: BTreeMap<String, u64>,
    pub not_staged_examples: BTreeMap<String, Vec<String>>,
}

fn example(map: &mut BTreeMap<String, Vec<String>>, key: &str, value: String) {
    let examples = map.entry(key.to_owned()).or_default();
    if examples.len() < EXAMPLES {
        examples.push(value);
    }
}

impl Report {
    /// Adds the check of the session `name`.
    pub fn add(&mut self, name: &str, check: SessionCheck) {
        self.sessions += 1;
        self.records += check.records;
        self.ids_looked_up += check.ids_looked_up;
        self.run_starts_looked_up += check.run_starts_looked_up;
        self.ledger_records_looked_up += check.ledger_records_looked_up;
        self.surfaces_compared += check.surfaces_compared;
        match check.outcome {
            Outcome::KeyUnavailable => {
                self.key_unavailable += 1;
                self.key_unavailable_sessions.push(name.to_owned());
                return;
            }
            Outcome::Failed(reason) => {
                let class = class_of(&reason);
                *self.failures.entry(class.clone()).or_default() += 1;
                example(
                    &mut self.failure_examples,
                    &class,
                    format!("{name}: {reason}"),
                );
                return;
            }
            Outcome::FailedAlike(error) if check.differences.is_empty() => {
                let class = class_of(&error);
                *self.failed_alike.entry(class.clone()).or_default() += 1;
                example(&mut self.failed_alike_sessions, &class, name.to_owned());
                return;
            }
            Outcome::FailedAlike(_) | Outcome::Opened => {}
        }
        if check.differences.is_empty() {
            self.agreed += 1;
            return;
        }
        self.differed += 1;
        for (field, detail) in check.differences {
            *self.differences.entry(field.clone()).or_default() += 1;
            example(
                &mut self.difference_examples,
                &field,
                format!("{name}: {detail}"),
            );
        }
    }

    /// Whether no session differed.
    pub fn passed(&self) -> bool {
        self.differed == 0
    }
}

/// An error's class: its text up to the first `(`, `{` or `:`, so errors of one kind group
/// whatever they carry.
fn class_of(error: &str) -> String {
    let end = error.find(['(', '{', ':']).unwrap_or(error.len());
    error[..end].trim().chars().take(80).collect()
}

/// The sessions under `root`, a folder of the new storage (it holds `heads/`), or a folder of
/// such folders.
pub fn store_roots(path: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    if path.join("heads").is_dir() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut roots = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let child = entry?.path();
        if child.join("heads").is_dir() {
            roots.push(child);
        }
    }
    roots.sort();
    Ok(roots)
}

/// Every session the store at `root` holds a head for.
pub fn stored_sessions(root: &Path) -> Result<Vec<SessionAddr>, BackendError> {
    LocalBackend::new(root).stored_sessions()
}

/// Copies the session `addr` of the store at `root` into memory: its head, its logs and its
/// objects, read from their files. Nothing under `root` is written.
pub fn copy_from_root(root: &Path, addr: &SessionAddr) -> Result<MemoryBackend, String> {
    let copy = MemoryBackend::default();
    let local = LocalBackend::new(root);
    let (head, version) = local
        .read_stored_head(addr)
        .map_err(|error| format!("head_unreadable: {error:?}"))?
        .ok_or("head_missing")?;
    futures::executor::block_on(copy.write_head(addr, &head, None, version))
        .map_err(|error| format!("copy_failed: {error:?}"))?;
    let logs = root.join("logs").join(&addr.0);
    if logs.is_dir() {
        for entry in std::fs::read_dir(&logs).map_err(|e| format!("logs_unreadable: {e}"))? {
            let path = entry.map_err(|e| format!("logs_unreadable: {e}"))?.path();
            let Some(log) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(id_of_hex)
            else {
                continue;
            };
            let bytes = std::fs::read(&path).map_err(|e| format!("log_unreadable: {e}"))?;
            copy.set_log(addr, &log, bytes);
        }
    }
    let objects = root.join("objects").join(&addr.0);
    let mut folders = vec![objects.clone()];
    while let Some(folder) = folders.pop() {
        let Ok(entries) = std::fs::read_dir(&folder) else {
            continue;
        };
        for entry in entries {
            let path = entry
                .map_err(|e| format!("objects_unreadable: {e}"))?
                .path();
            if path.is_dir() {
                folders.push(path);
                continue;
            }
            let Some(name) = path
                .strip_prefix(&objects)
                .ok()
                .and_then(|name| name.to_str())
                .map(|name| name.replace(std::path::MAIN_SEPARATOR, "/"))
            else {
                continue;
            };
            let bytes = std::fs::read(&path).map_err(|e| format!("object_unreadable: {e}"))?;
            copy.set_object(addr, &name, bytes);
        }
    }
    Ok(copy)
}

/// Copies the session `addr` of `source` into a new memory backend, for a check that must leave
/// `source` as it was.
pub async fn copy_from_memory(
    source: &MemoryBackend,
    addr: &SessionAddr,
) -> Result<MemoryBackend, String> {
    let copy = MemoryBackend::default();
    let (head, version) = source
        .read_head(addr)
        .await
        .map_err(|error| format!("head_unreadable: {error:?}"))?
        .ok_or("head_missing")?;
    copy.write_head(addr, &head, None, version)
        .await
        .map_err(|error| format!("copy_failed: {error:?}"))?;
    for log in source.log_names(addr) {
        let end = source
            .log_end(addr, &log)
            .await
            .map_err(|e| format!("log_unreadable: {e:?}"))?;
        let bytes = source
            .read_log(addr, &log, 0..end)
            .await
            .map_err(|e| format!("log_unreadable: {e:?}"))?;
        copy.set_log(addr, &log, bytes);
    }
    for name in source.object_names(addr) {
        if let Some(bytes) = source
            .get_object(addr, &name)
            .await
            .map_err(|e| format!("object_unreadable: {e:?}"))?
        {
            copy.set_object(addr, &name, bytes);
        }
    }
    Ok(copy)
}

/// A log's id from its file name: 32 lowercase hex characters.
fn id_of_hex(name: &str) -> Option<Id128> {
    if name.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (byte, pair) in id.iter_mut().zip(name.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Id128(id))
}

/// The records a writer or a view kept before loading streamed its log, kept the old way: a
/// writer's cut from the whole list at its latest run start, a view's the whole list.
#[derive(Debug, Clone)]
pub(crate) struct OldKept {
    pub(crate) from: u64,
    pub(crate) records: Vec<(u64, Record)>,
    pub(crate) whole: bool,
    latest_start: Option<(u64, String)>,
    per_run: bool,
}

impl OldKept {
    fn whole_log(records: Vec<(u64, Record)>) -> Self {
        let latest_start = latest_start(&records);
        Self {
            from: 0,
            records,
            whole: true,
            latest_start,
            per_run: false,
        }
    }

    fn writer_of(mut records: Vec<(u64, Record)>, next_seq: u64) -> Self {
        match records
            .iter()
            .rposition(|(_, record)| matches!(record, Record::RunStart(_)))
        {
            Some(at) => {
                let whole = at == 0;
                records.drain(..at);
                let latest_start = latest_start(&records);
                Self {
                    from: records[0].0,
                    records,
                    whole,
                    latest_start,
                    per_run: true,
                }
            }
            None => Self {
                from: next_seq,
                records: Vec::new(),
                whole: records.is_empty(),
                latest_start: None,
                per_run: true,
            },
        }
    }

    fn note(&mut self, seq: u64, record: &Record) {
        if let Record::RunStart(start) = record {
            if self.per_run {
                self.whole &= self.records.is_empty() && self.from == seq;
                self.records.clear();
                self.from = seq;
            }
            self.latest_start = Some((seq, start.run_id.clone()));
        }
        self.records.push((seq, record.clone()));
    }

    fn between(&self, first: u64, last: u64) -> Option<Vec<(u64, Record)>> {
        if first < self.from && !self.whole {
            return None;
        }
        let start = self.records.partition_point(|(seq, _)| *seq < first);
        Some(
            self.records[start..]
                .iter()
                .take_while(|(seq, _)| *seq <= last)
                .cloned()
                .collect(),
        )
    }

    fn start_of(&self, run_id: &str) -> Option<u64> {
        self.latest_start
            .as_ref()
            .filter(|(_, id)| id == run_id)
            .map(|(seq, _)| *seq)
    }
}

fn latest_start(records: &[(u64, Record)]) -> Option<(u64, String)> {
    records.iter().rev().find_map(|(seq, record)| match record {
        Record::RunStart(start) => Some((*seq, start.run_id.clone())),
        _ => None,
    })
}

/// The open log's id index as it was before it kept places: a copy of every ledger record that
/// names an id, by the id's text.
#[derive(Debug, Clone, Default)]
pub(crate) struct OldIds {
    pub(crate) ids: BTreeMap<(IdKind, String), Vec<u64>>,
    pub(crate) records: BTreeMap<u64, Record>,
}

impl OldIds {
    fn add(&mut self, seq: u64, record: &Record) {
        let ids = idindex::record_ids(record);
        if ids.is_empty() {
            return;
        }
        for id in ids {
            self.ids.entry(id).or_default().push(seq);
        }
        self.records.insert(seq, record.clone());
    }

    fn find(&self, kind: IdKind, id: &str) -> Vec<(u64, Record)> {
        self.ids
            .get(&(kind, id.to_owned()))
            .into_iter()
            .flatten()
            .map(|seq| (*seq, self.records[seq].clone()))
            .collect()
    }
}

/// A session loaded the old way (see the module's notes).
#[derive(Debug, Clone)]
pub(crate) struct Reference {
    /// Every record of the open log's complete batches.
    pub(crate) records: Vec<(u64, Record)>,
    /// Where the complete batches end.
    pub(crate) log_len: u64,
    pub(crate) next_seq: u64,
    /// The last ledger seq the session's sealed history holds, if any.
    pub(crate) sealed_ledger_through: Option<u64>,
    pub(crate) conversation: Conversation,
    pub(crate) operations: Operations,
    pub(crate) ids: OldIds,
    pub(crate) kept: OldKept,
    /// The cut id of the move that owns the session's writer, when one does.
    moving: Option<String>,
}

/// What the head of a session names, read without its key.
enum Headed {
    /// A session whose creation never committed: a view refuses it `Missing`, a writer finishes
    /// the creation.
    Creating,
    Open(Box<Reference>),
}

/// Loads the session `addr` of `backend` the old way, as a view (`per_run` false) or a writer
/// keeps it, with the errors `Session::read` gives.
async fn reference(
    backend: &MemoryBackend,
    addr: &SessionAddr,
    triple: &SessionTriple,
    deployment: &DeploymentKey,
    per_run: bool,
) -> Result<Headed, SessionError> {
    let (stored, _) = backend
        .read_head(addr)
        .await?
        .ok_or(SessionError::Missing)?;
    let (head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
    if &head.triple != triple {
        return Err(SessionError::TripleMismatch);
    }
    let moving = match &head.residence {
        Residence::Recovering(_) => return Err(SessionError::NeedsRecovery),
        Residence::Moved(moved) => {
            return Err(SessionError::SessionMoved {
                holder: moved.holder.clone(),
            })
        }
        Residence::Creating(_) => return Ok(Headed::Creating),
        Residence::Moving(cut_id) => Some(cut_id.clone()),
        _ => None,
    };
    let open = head
        .open_log
        .clone()
        .ok_or(SessionError::Backend(BackendError::Corrupt(
            "local head without a log",
        )))?;
    let end = backend.log_end(addr, &open.change).await?;
    let bytes = backend.read_log(addr, &open.change, 0..end).await?;
    let mut bind = head.session_id.0.to_vec();
    bind.extend_from_slice(&open.change.0);
    let read = match damage::analyse(
        &bytes,
        &key.frame_key(),
        &bind,
        open.synced_len,
        chain_base,
        None,
    )? {
        Verdict::Opens(read) => read,
        Verdict::Debris(_) => return Err(SessionError::NeedsRecovery),
        Verdict::Corrupt { at, durable_bound } => {
            return Err(SessionError::Corrupt { at, durable_bound })
        }
    };
    let checkpoint = Checkpoint::decode(&read.checkpoint).map_err(SessionError::Checkpoint)?;
    let last_seq = read.records.last().map(|(seq, _)| *seq);
    let next_seq = head
        .next_seq
        .max(checkpoint.next_seq)
        .max(last_seq.map_or(0, |seq| seq + 1));
    let mut conversation = checkpoint.conversation;
    let mut operations = checkpoint.operational;
    let mut ids = OldIds::default();
    for (seq, record) in &read.records {
        if record.stream() == Stream::Ledger {
            if *seq <= checkpoint.covers_through {
                continue;
            }
            ids.add(*seq, record);
        }
        session::apply(&mut conversation, &mut operations, *seq, record)
            .map_err(SessionError::Fold)?;
    }
    let kept = if per_run {
        OldKept::writer_of(read.records.clone(), next_seq)
    } else {
        OldKept::whole_log(read.records.clone())
    };
    Ok(Headed::Open(Box::new(Reference {
        records: read.records,
        log_len: read.complete_through,
        next_seq,
        sealed_ledger_through: head.ledger_dir.as_ref().map(|dir| dir.last_seq),
        conversation,
        operations,
        ids,
        kept,
        moving,
    })))
}

fn chain_base(body: &[u8]) -> Result<ChainBase, String> {
    let checkpoint = Checkpoint::decode(body).map_err(|e| e.to_string())?;
    Ok(ChainBase {
        transcript: checkpoint.transcript_base,
        ledger: checkpoint.ledger_base,
    })
}

impl Reference {
    /// Takes in the records a writer appended when it opened, read back from the log after the
    /// open, as the writer took them in: folded, filed and kept.
    async fn opened_writer(
        &mut self,
        backend: &MemoryBackend,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<(), String> {
        let (stored, _) = backend
            .read_head(addr)
            .await
            .map_err(|e| format!("{e:?}"))?
            .ok_or("the opened writer's head is missing")?;
        let (head, key) = head::open(&stored, deployment).map_err(|e| format!("{e:?}"))?;
        let open = head
            .open_log
            .ok_or("the opened writer's head names no log")?;
        let end = backend
            .log_end(addr, &open.change)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let bytes = backend
            .read_log(addr, &open.change, 0..end)
            .await
            .map_err(|e| format!("{e:?}"))?;
        let mut bind = head.session_id.0.to_vec();
        bind.extend_from_slice(&open.change.0);
        let read = broca_session_store::log::read(
            &bytes,
            &key.frame_key(),
            &bind,
            open.synced_len,
            chain_base,
        )
        .map_err(|e| format!("{e:?}"))?;
        for (seq, record) in read.records {
            if seq < self.next_seq {
                continue;
            }
            session::apply(&mut self.conversation, &mut self.operations, seq, &record)?;
            if record.stream() == Stream::Ledger {
                self.ids.add(seq, &record);
            }
            self.kept.note(seq, &record);
            self.next_seq = seq + 1;
            self.records.push((seq, record));
        }
        self.log_len = read.complete_through;
        Ok(())
    }
}

/// Checks the session `addr` of `copy`, a copy of it the check may write to (see the module's
/// notes), opening encrypted sessions with `deployment`.
pub async fn check(
    copy: &MemoryBackend,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
) -> SessionCheck {
    check_with(copy, addr, deployment, &|_| {}).await
}

/// [`check`], with `plant` changing the reference before each comparison: the tests plant a
/// difference with it to see the report name it.
pub(crate) async fn check_with(
    copy: &MemoryBackend,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
    plant: &dyn Fn(&mut Reference),
) -> SessionCheck {
    let mut check = SessionCheck::default();
    let stored = match copy.read_head(addr).await {
        Ok(Some((stored, _))) => stored,
        Ok(None) => {
            check.outcome = Outcome::Failed("head_missing".into());
            return check;
        }
        Err(error) => {
            check.outcome = Outcome::Failed(format!("head_unreadable: {error:?}"));
            return check;
        }
    };
    if head::stored_cipher(&stored).is_ok_and(|cipher| cipher == Cipher::XChaCha20Poly1305)
        && !deployment.is_loaded()
    {
        check.outcome = Outcome::KeyUnavailable;
        return check;
    }
    // A head whose triple doesn't read fails to open on both paths, before any triple is
    // compared, so any triple serves.
    let triple = head::stored_triple(&stored).unwrap_or(SessionTriple {
        project_root: String::new(),
        harness: String::new(),
        session: String::new(),
    });

    // The view, then its surfaces, before the writer's open appends to the copy.
    let expected = reference(copy, addr, &triple, deployment, false).await;
    let view = Session::view(copy, addr.clone(), &triple, deployment).await;
    let mut surfaces = None;
    match (expected, view) {
        (Ok(Headed::Open(mut expected)), Ok(view)) => {
            plant(&mut expected);
            check.records = expected.records.len() as u64;
            compare(&mut check, "view", &view, &expected, false).await;
            let through = view.next_seq().saturating_sub(1);
            surfaces = Some((through, digest_surfaces(&view, through).await));
        }
        (Ok(Headed::Creating), Err(SessionError::Missing)) => {
            check.outcome = Outcome::FailedAlike("Missing: a creation that never committed".into());
        }
        (Err(expected), Err(error)) if format!("{expected:?}") == format!("{error:?}") => {
            check.outcome = Outcome::FailedAlike(format!("{error:?}"));
        }
        (expected, view) => check.differ(
            "view.outcome",
            format!(
                "reference {}, view {}",
                outcome_of(&expected.map(|_| ())),
                outcome_of(&view.map(|_| ()))
            ),
        ),
    }

    let expected = reference(copy, addr, &triple, deployment, true).await;
    let writer = Session::load(copy, addr.clone(), &triple, deployment).await;
    match (expected, writer) {
        (Ok(Headed::Open(mut expected)), Ok(writer)) if expected.moving.is_none() => {
            if let Err(error) = expected.opened_writer(copy, addr, deployment).await {
                check.differ(
                    "writer.opened",
                    format!("the opened log doesn't fold: {error}"),
                );
                return check;
            }
            plant(&mut expected);
            compare(&mut check, "writer", &writer, &expected, true).await;
            if let Some((through, view_surfaces)) = surfaces {
                let writer_surfaces = digest_surfaces(&writer, through).await;
                for ((name, view), (_, writer)) in view_surfaces.iter().zip(&writer_surfaces) {
                    check.surfaces_compared += 1;
                    if view != writer {
                        check.differ(
                            format!("surface.{name}"),
                            format!("view {view}, writer {writer}"),
                        );
                    }
                }
            }
        }
        (Ok(Headed::Open(expected)), Err(SessionError::SessionMoving { cut_id }))
            if expected.moving.as_ref() == Some(&cut_id) => {}
        // A writer finishes a creation that never committed: there is nothing loaded to compare.
        (Ok(Headed::Creating), Ok(_)) => {}
        (Err(expected), Err(error)) if format!("{expected:?}") == format!("{error:?}") => {}
        (expected, writer) => check.differ(
            "writer.outcome",
            format!(
                "reference {}, writer {}",
                outcome_of(&expected.map(|_| ())),
                outcome_of(&writer.map(|_| ()))
            ),
        ),
    }
    check
}

fn outcome_of(result: &Result<(), SessionError>) -> String {
    match result {
        Ok(()) => "opened".into(),
        Err(error) => format!("refused {}", class_of(&format!("{error:?}"))),
    }
}

/// `records`, as a difference names them: how many, and their first and last seqs.
fn described(records: Option<&[(u64, Record)]>) -> String {
    match records {
        None => "none kept".into(),
        Some([]) => "0 records".into(),
        Some(records) => format!(
            "{} records, seqs {}..={}",
            records.len(),
            records[0].0,
            records[records.len() - 1].0
        ),
    }
}

fn seqs(records: &[(u64, Record)]) -> Vec<u64> {
    records.iter().map(|(seq, _)| *seq).collect()
}

/// Compares the loaded `session` with `expected`, the reference the same way kept, naming each
/// difference `<side>.<field>`.
async fn compare(
    check: &mut SessionCheck,
    side: &str,
    session: &Session<&MemoryBackend>,
    expected: &Reference,
    writer: bool,
) {
    let field = |name: &str| format!("{side}.{name}");
    if session.conversation.encode() != expected.conversation.encode() {
        check.differ(field("conversation"), "the encoded folds differ");
    }
    if session.operations.encode() != expected.operations.encode() {
        check.differ(field("operations"), "the encoded folds differ");
    }
    if session.next_seq() != expected.next_seq {
        check.differ(
            field("next_seq"),
            format!("{} vs reference {}", session.next_seq(), expected.next_seq),
        );
    }
    if session.open_log().1 != expected.log_len {
        check.differ(
            field("log_len"),
            format!("{} vs reference {}", session.open_log().1, expected.log_len),
        );
    }

    // The kept records, read from every place a reader may start.
    let starts: Vec<(u64, String)> = expected
        .records
        .iter()
        .filter_map(|(seq, record)| match record {
            Record::RunStart(start) => Some((*seq, start.run_id.clone())),
            _ => None,
        })
        .collect();
    let mut probes: BTreeSet<u64> = starts.iter().map(|(seq, _)| *seq).collect();
    let from = expected.kept.from;
    probes.extend([0, from.saturating_sub(1), from, from + 1, expected.next_seq]);
    if session.kept_records() != expected.kept.records.len() {
        check.differ(
            field("kept"),
            format!(
                "{} records kept vs reference {}",
                session.kept_records(),
                expected.kept.records.len()
            ),
        );
    }
    for first in probes {
        let kept = session.kept_from(first);
        let reference = expected.kept.between(first, u64::MAX);
        if kept != reference {
            check.differ(
                field("kept"),
                format!(
                    "from {first}: {} vs reference {}",
                    described(kept.as_deref()),
                    described(reference.as_deref())
                ),
            );
        }
    }
    for (index, (_, run)) in starts.iter().enumerate() {
        let kept = session.kept_run_start(run);
        let reference = expected.kept.start_of(run);
        if kept != reference {
            check.differ(
                field("kept_run_start"),
                format!("run #{index}: {kept:?} vs reference {reference:?}"),
            );
        }
    }

    // Every id any open-log ledger record names, and one nothing names.
    let open: BTreeSet<u64> = expected.records.iter().map(|(seq, _)| *seq).collect();
    let mut ids: BTreeSet<(IdKind, String)> = expected
        .records
        .iter()
        .filter(|(_, record)| record.stream() == Stream::Ledger)
        .flat_map(|(_, record)| idindex::record_ids(record))
        .collect();
    ids.insert((
        IdKind::CallKey,
        "store-load-check: no record names this".into(),
    ));
    let mut sealed_runs: BTreeMap<String, Vec<(u64, Record)>> = BTreeMap::new();
    for (index, (kind, id)) in ids.iter().enumerate() {
        check.ids_looked_up += 1;
        let found = match session.find_id(*kind, id).await {
            Ok(found) => found,
            Err(error) => {
                check.differ(
                    field("find_id"),
                    format!(
                        "{kind:?} #{index}: refused {}",
                        class_of(&format!("{error:?}"))
                    ),
                );
                continue;
            }
        };
        let sealed: Vec<(u64, Record)> = found
            .iter()
            .filter(|(seq, _)| !open.contains(seq))
            .cloned()
            .collect();
        let mut reference = expected.ids.find(*kind, id);
        reference.extend(sealed.iter().cloned());
        reference.sort_by_key(|(seq, _)| *seq);
        if found != reference {
            let detail = if seqs(&found) == seqs(&reference) {
                format!(
                    "{kind:?} #{index}: the records at seqs {:?} differ",
                    seqs(&found)
                )
            } else {
                format!(
                    "{kind:?} #{index}: seqs {:?} vs reference {:?}",
                    seqs(&found),
                    seqs(&reference)
                )
            };
            check.differ(field("find_id"), detail);
        }
        if *kind == IdKind::RunId {
            sealed_runs.insert(id.clone(), sealed);
        }
    }

    // Every run an open-log record names: its start, open or sealed.
    for (index, (run, sealed)) in sealed_runs.iter().enumerate() {
        check.run_starts_looked_up += 1;
        let start_of = |records: Vec<(u64, Record)>| {
            records.into_iter().find_map(|(_, record)| match record {
                Record::RunStart(start) => Some(*start),
                _ => None,
            })
        };
        let reference =
            start_of(expected.ids.find(IdKind::RunId, run)).or_else(|| start_of(sealed.clone()));
        match session.find_run_start(run).await {
            Ok(found) if found == reference => {}
            Ok(found) => check.differ(
                field("find_run_start"),
                format!(
                    "run #{index}: found {} vs reference {}",
                    found.is_some(),
                    reference.is_some()
                ),
            ),
            Err(error) => check.differ(
                field("find_run_start"),
                format!("run #{index}: refused {}", class_of(&format!("{error:?}"))),
            ),
        }
    }

    // The open log's ledger records by seq, and a seq past the log. A writer reads its whole log
    // for each one outside memory, so it reads a sample.
    let unsealed = |seq: u64| {
        expected
            .sealed_ledger_through
            .is_none_or(|through| seq > through)
    };
    let mut lookups: Vec<(u64, Option<Record>)> = expected
        .records
        .iter()
        .filter(|(seq, _)| unsealed(*seq))
        .map(|(seq, record)| {
            (
                *seq,
                (record.stream() == Stream::Ledger).then(|| record.clone()),
            )
        })
        .collect();
    if writer {
        let (kept, rest): (Vec<_>, Vec<_>) = lookups
            .into_iter()
            .partition(|(seq, _)| *seq >= expected.kept.from);
        lookups = kept
            .into_iter()
            .take(WRITER_LEDGER_SAMPLE)
            .chain(rest.into_iter().take(WRITER_LEDGER_SAMPLE))
            .collect();
    }
    lookups.push((expected.next_seq, None));
    for (seq, reference) in lookups {
        check.ledger_records_looked_up += 1;
        match session.ledger_record(seq).await {
            Ok(found) if found == reference => {}
            Ok(found) => check.differ(
                field("ledger_record"),
                format!(
                    "seq {seq}: found {} vs reference {}",
                    found.is_some(),
                    reference.is_some()
                ),
            ),
            Err(error) => check.differ(
                field("ledger_record"),
                format!("seq {seq}: refused {}", class_of(&format!("{error:?}"))),
            ),
        }
    }
}

/// Digests of the read surfaces the store crate serves on its own: the raw history through seq
/// `through`, and every entry of the conversation. An error is its own digest, so two sides that
/// fail alike agree.
async fn digest_surfaces(
    session: &Session<&MemoryBackend>,
    through: u64,
) -> Vec<(&'static str, String)> {
    let history = match session.records_between(0, through).await {
        Ok(records) => digest(records.iter()),
        Err(error) => format!("refused {}", class_of(&format!("{error:?}"))),
    };
    let placed = usize::try_from(session.conversation.placed).unwrap_or(usize::MAX);
    let entries = match session.entries(1, placed).await {
        Ok(entries) => digest(entries.iter()),
        Err(error) => format!("refused {}", class_of(&format!("{error:?}"))),
    };
    vec![("history", history), ("entries", entries)]
}

/// The SHA-256 of `items` as their debug text, and how many there were.
fn digest<T: std::fmt::Debug>(items: impl Iterator<Item = T>) -> String {
    use sha2::Digest as _;
    struct Hashing(sha2::Sha256);
    impl std::fmt::Write for Hashing {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0.update(text.as_bytes());
            Ok(())
        }
    }
    let mut hashing = Hashing(sha2::Sha256::new());
    let mut count = 0u64;
    for item in items {
        count += 1;
        let _ = write!(hashing, "{item:?}\u{0}");
    }
    let sum = hashing.0.finalize();
    format!(
        "{count} items, sha256 {}",
        &broca_session_store::codec::hex(&sum)[..16]
    )
}

#[cfg(test)]
#[path = "load_check_tests.rs"]
mod tests;
