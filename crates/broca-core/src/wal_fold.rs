//! Two-phase folding shared by operator and scheduled passes. Live bytes remain authoritative until a
//! sealed, digest-verified member is durable and the session lease protects unlink.
use crate::{
    lease::acquire_archive_pass, lease_key_for, wal_writer::session_addr, FileLeaseStore,
    LeaseError, LeaseStore,
};
use broca_owner_only::OpenOptions;
use broca_protocol::BindIdentity;
use broca_wal::{
    archive::{ArchiveReader, ArchiveWriter, ContainerState},
    WalRecord,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const COLD_MS: u64 = 14 * 24 * 60 * 60 * 1000;
const MAX_BYTES: u64 = broca_wal::file_wal::DEFAULT_MAX_READ_BYTES;

#[derive(Clone, Copy)]
enum Tree {
    Ordinary,
    Durable,
}
impl Tree {
    fn resume_name(self, name: &str) -> String {
        if matches!(self, Self::Durable) {
            format!("wal-archive-durable/{name}")
        } else {
            name.into()
        }
    }
    fn live(self, root: &Path) -> PathBuf {
        root.join(match self {
            Self::Ordinary => "wal",
            Self::Durable => "wal-durable",
        })
    }
    fn archive(self, root: &Path) -> PathBuf {
        root.join(match self {
            Self::Ordinary => "wal-archive",
            Self::Durable => "wal-archive-durable",
        })
    }
    fn for_journal(journal: &Journal) -> Self {
        if journal
            .path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "wal-archive-durable")
        {
            Self::Durable
        } else {
            Self::Ordinary
        }
    }
}

#[derive(Default)]
pub struct FoldOptions {
    pub write: bool,
    /// Canonical journal basename, optionally prefixed with `wal-archive-durable/`.
    /// Other directory prefixes and arbitrary filesystem paths are refused.
    pub resume: Option<String>,
    /// Explicitly record abandonment, including any missing live sources.
    pub abandon: bool,
    pub unfold: bool,
}

/// Optional policy envelope preserves existing operator options and crash-recovery callers.
/// Passing plain `&FoldOptions` to `fold_with_identities` still selects exactly 14 days.
pub struct FoldSettings<'a> {
    pub options: &'a FoldOptions,
    pub cold_ms: u64,
}
impl<'a> From<&'a FoldOptions> for FoldSettings<'a> {
    fn from(options: &'a FoldOptions) -> Self {
        Self {
            options,
            cold_ms: COLD_MS,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct FoldReport {
    pub candidates: Vec<String>,
    pub skipped: BTreeMap<String, Vec<String>>,
    pub removed: usize,
    pub restored: usize,
    pub superseded: usize,
    pub journal: Option<String>,
    pub resolved_from_store: Vec<String>,
    /// Candidates admitted by intact pre-schema provenance rather than timestamp age.
    pub cold_pre_schema: Vec<String>,
    pub permanent_skip_notice: Option<&'static str>,
}
impl FoldReport {
    fn skip(&mut self, reason: &str, addr: &str) {
        if reason == "no_authoritative_identity" {
            self.permanent_skip_notice = Some("Sessions skipped as no_authoritative_identity can never be archived without an authoritative identity and will stay in the backup manifest.");
        }
        self.skipped
            .entry(reason.into())
            .or_default()
            .push(addr.into());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Intended,
    Archived,
    Done,
    Superseded,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    Session {
        addr: String,
        session: BindIdentity,
        digest: [u8; 32],
        state: State,
    },
    Complete,
    Restarted,
    Abandoned {
        live_present: BTreeMap<String, bool>,
    },
}
struct Journal {
    path: PathBuf,
    events: Vec<Event>,
    valid_len: u64,
}
/// Annotate an io error with the path it happened on.
///
/// `std::fs` errors carry no path, so a fold that fails reports bare text like
/// "Access is denied" with nothing an operator can act on — and nothing a MAINTAINER can
/// act on either, which is how a Windows-only failure in this file cost two CI cycles to
/// locate. Every fallible filesystem call here names its subject.
fn at(path: &Path) -> impl FnOnce(io::Error) -> io::Error + '_ {
    move |error| io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
/// Make a rename or unlink in `path` durable.
///
/// WINDOWS CANNOT OPEN A DIRECTORY VIA `File::open`, so the POSIX pattern below returns
/// `PermissionDenied` there and propagating it fails the whole fold — which is exactly what CI
/// caught. `file_wal.rs` hit this first and resolved it as a blanket best-effort sync.
///
/// This is deliberately STRICTER than that precedent: on unix the directory sync is load-bearing
/// (it is what makes the rename or unlink survive a crash), so a genuine failure there must
/// propagate rather than be swallowed by an `if let Ok(..)`. Only the PLATFORM that cannot perform
/// the operation at all is excused, and it is excused by construction rather than by ignoring an
/// error that might mean something else.
#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)
        .map_err(at(path))?
        .sync_all()
        .map_err(at(path))
}

/// See the unix arm. NTFS does not require a directory handle to make a rename or unlink durable,
/// and no such handle can be obtained, so there is nothing to do rather than something to skip.
#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}
fn now_ms() -> io::Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| invalid(e.to_string()))?
            .as_millis(),
    )
    .map_err(|e| invalid(e.to_string()))
}
impl Journal {
    fn load(path: PathBuf) -> io::Result<Self> {
        let bytes = fs::read(&path)?;
        let valid_len = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |n| n + 1);
        let events = bytes[..valid_len]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).map_err(|e| invalid(e.to_string())))
            .collect::<io::Result<Vec<Event>>>()?;
        for event in &events {
            if let Event::Session { addr, session, .. } = event {
                if *addr != session_addr(session) {
                    return Err(invalid("journal identity/address mismatch"));
                }
            }
        }
        Ok(Self {
            path,
            events,
            valid_len: valid_len as u64,
        })
    }
    fn terminal(&self) -> bool {
        matches!(
            self.events.last(),
            Some(Event::Complete | Event::Restarted | Event::Abandoned { .. })
        )
    }
    fn latest(&self) -> BTreeMap<String, Event> {
        let mut entries = BTreeMap::new();
        for event in &self.events {
            if let Event::Session { addr, .. } = event {
                entries.insert(addr.clone(), event.clone());
            }
        }
        entries
    }
    fn append(
        &mut self,
        event: Event,
        boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(&self.path)?;
        // An incomplete last JSON line has no durable state transition. Trim only
        // that tail before appending, preserving every complete forensic record.
        file.set_len(self.valid_len)?;
        use std::io::{Seek, SeekFrom};
        file.seek(SeekFrom::End(0))?;
        let mut bytes = serde_json::to_vec(&event).map_err(|e| invalid(e.to_string()))?;
        bytes.push(b'\n');
        file.write_all(&bytes)?;
        file.sync_all().inspect(|_| {
            #[cfg(test)]
            JOURNAL_SYNCS.with(|count| count.set(count.get() + 1));
        })?;
        self.valid_len += bytes.len() as u64;
        self.events.push(event);
        boundary(&self.path)
    }
}

fn live_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    use std::io::Read;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if file.metadata()?.len() > MAX_BYTES {
        return Err(invalid("WAL exceeds fold read limit"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(invalid("WAL exceeds fold read limit"));
    }
    Ok(Some(bytes))
}
fn eligible(
    bytes: &[u8],
    start: u64,
    cold_ms: u64,
) -> Result<(Option<BindIdentity>, bool), &'static str> {
    if bytes.is_empty() {
        return Err("zero_byte");
    }
    // A WAL a newer build wrote is intact but unreadable here. It gets its own reason so an
    // operator reading the report does not go looking for damage, and it is never folded:
    // this build cannot prove its age or identity from records it cannot interpret.
    let range =
        broca_wal::file_wal::read_verified_member_range(bytes, 1, 0, None).map_err(|error| {
            match error {
                broca_wal::WalError::NewerWriter { .. } => "newer_writer",
                _ => "undecodable",
            }
        })?;
    let records = match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => records,
        _ => return Err("undecodable"),
    };
    if records.is_empty() {
        return Err("lineage_only");
    }
    if let Some(lineage_id) = range.lineage_id {
        if !crate::replay::deferred_call_obligations(lineage_id, records).is_empty() {
            return Err("deferred_call_open");
        }
    } else if records
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
    {
        // Prepared anchors require a lineage. Without it, no reader can prove debt discharged.
        return Err("deferred_call_open");
    }
    let mut identity = None;
    let mut last_ts = None;
    for framed in records {
        if let WalRecord::RunStarted { session, .. } = &framed.record {
            identity = Some(session.clone());
        }
        let ts = match &framed.record {
            WalRecord::RunStarted { ts_ms, .. }
            | WalRecord::RunFinished { ts_ms, .. }
            | WalRecord::RunPaused { ts_ms, .. }
            | WalRecord::RunConfigRefrozen { ts_ms, .. }
            | WalRecord::RunAuthRefrozen { ts_ms, .. } => *ts_ms,
            _ => None,
        };
        if ts.is_some() {
            last_ts = ts;
        }
    }
    match last_ts {
        Some(ts) => {
            if start.checked_sub(ts).is_none_or(|age| age < cold_ms) {
                return Err("warm");
            }
            Ok((identity, false))
        }
        None => {
            // A torn newer append could hide the timestamp that disproves pre-schema
            // age. Ordinary timestamp eligibility tolerates torn tails; this inference
            // cannot. The range reader above discards tails, so check framing here.
            let mut remaining = bytes;
            while !remaining.is_empty() {
                use broca_wal::framing::{decode_next, DecodeStep};
                match decode_next(remaining) {
                    DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                        remaining = &remaining[consumed..]
                    }
                    DecodeStep::TornTail => return Err("age_unproven_torn_tail"),
                    DecodeStep::Corrupt(_) => return Err("undecodable"),
                    DecodeStep::NewerWriter { .. } => return Err("newer_writer"),
                }
            }
            // Anchor provenance at the head: a tear can remove the tail, not a
            // decoded opening RunStarted. With no later lifecycle timestamp and
            // no hidden trailing append, this is evidence of pre-schema age.
            // Exception: a pre-1970 system clock makes epoch_millis() write None
            // even today, so those sessions can be mistaken for pre-schema ones.
            if !matches!(
                records.first().map(|frame| &frame.record),
                Some(WalRecord::RunStarted { ts_ms: None, .. })
            ) {
                return Err("age_unproven_without_run_start");
            }
            Ok((identity, true))
        }
    }
}

fn move_protected(root: &Path, session: &BindIdentity) -> bool {
    crate::FileWalStore::new(root.join("wal"))
        .check_move(session, true)
        .is_err()
}

// Store rows are hints until their triple reproduces the filename. A stale embedded
// triple is provenance, not the lease identity; never rewrite it to repair addressing.
fn candidate(
    bytes: &[u8],
    addr: &str,
    start: u64,
    identities: &BTreeMap<String, BindIdentity>,
    cold_ms: u64,
) -> Result<(BindIdentity, bool, bool), &'static str> {
    let (embedded, pre_schema) = eligible(bytes, start, cold_ms)?;
    if let Some(session) = identities.get(addr).filter(|s| session_addr(s) == addr) {
        return Ok((session.clone(), true, pre_schema));
    }
    embedded
        .filter(|s| session_addr(s) == addr)
        .map(|session| (session, false, pre_schema))
        .ok_or("no_authoritative_identity")
}
fn journal_path(dir: &Path, name: &str) -> io::Result<PathBuf> {
    let stem = name
        .strip_suffix(".journal")
        .ok_or_else(|| invalid("resume requires fold-<unix-ms>.journal"))?;
    let ark = format!("{stem}.ark");
    if Path::new(&ark).file_name().and_then(|s| s.to_str()) != Some(&ark) {
        return Err(invalid("resume requires a journal basename"));
    }
    broca_wal::archive::fold_timestamp(&ark)?;
    Ok(dir.join(name))
}
/// The basename of an unfinished pass, if one is blocking a fresh fold.
///
/// A fresh fold REFUSES while an unfinished journal exists, and that gate is correct: a
/// half-retired pass must be finished or abandoned deliberately, not driven over. The
/// operator meets it as a message naming `--resume`. AN AUTOMATIC CALLER CANNOT READ A
/// MESSAGE, so without this it can only ever ask for fresh folds and is refused forever
/// -- one transient phase-2 lease collision would stop scheduled archiving permanently,
/// across restarts, for every session under the root.
///
/// Read-only, and deliberately returns the NAME rather than a journal: `resume` takes a
/// canonical basename, so this hands back exactly what the option wants and nothing that
/// would let a caller reach into pass state it should not touch.
pub fn unfinished_pass(root: &Path) -> io::Result<Option<String>> {
    for tree in [Tree::Ordinary, Tree::Durable] {
        if let Some(path) = unfinished(&tree.archive(root))?.first() {
            let name = path.file_name().unwrap().to_string_lossy();
            return Ok(Some(tree.resume_name(&name)));
        }
    }
    Ok(None)
}

fn unfinished(dir: &Path) -> io::Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "journal") {
            let journal = Journal::load(path.clone())?;
            // Before the first admission, a crash can leave only a journal. No
            // session state or archive bytes need recovery, so it cannot block folds.
            if !journal.terminal()
                && !(journal.latest().is_empty() && !path.with_extension("ark").try_exists()?)
            {
                result.push(path);
            }
        }
    }
    Ok(result)
}

/// Report-only unless explicitly authorized. The pass lock spans recovery and both
/// phases; session locks are held only while observing or mutating that session.
pub fn fold<'a>(state_root: &Path, options: impl Into<FoldSettings<'a>>) -> io::Result<FoldReport> {
    fold_with_identities(state_root, options, &BTreeMap::new())
}

/// Fold using a caller-supplied snapshot of store meta identities by WAL address.
/// Every supplied triple is independently checked against the filename before leasing;
/// missing or invalid entries fall back to matching embedded provenance. Keeping the
/// store adapter here at the API boundary leaves byte eligibility store-independent.
pub fn fold_with_identities<'a>(
    state_root: &Path,
    options: impl Into<FoldSettings<'a>>,
    identities: &BTreeMap<String, BindIdentity>,
) -> io::Result<FoldReport> {
    let settings = options.into();
    run_with_threshold(
        state_root,
        settings.options,
        now_ms()?,
        &mut |_| Ok(()),
        identities,
        settings.cold_ms,
    )
}

#[cfg(test)]
fn run(
    root: &Path,
    options: &FoldOptions,
    start: u64,
    boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
) -> io::Result<FoldReport> {
    run_with_identities(root, options, start, boundary, &BTreeMap::new())
}

#[cfg(test)]
fn run_with_identities(
    root: &Path,
    options: &FoldOptions,
    start: u64,
    boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
    identities: &BTreeMap<String, BindIdentity>,
) -> io::Result<FoldReport> {
    run_with_threshold(root, options, start, boundary, identities, COLD_MS)
}

fn run_with_threshold(
    root: &Path,
    options: &FoldOptions,
    start: u64,
    boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
    identities: &BTreeMap<String, BindIdentity>,
    cold_ms: u64,
) -> io::Result<FoldReport> {
    if let Some(name) = options
        .resume
        .as_deref()
        .and_then(|name| name.strip_prefix("wal-archive-durable/"))
    {
        let options = FoldOptions {
            resume: Some(name.into()),
            write: options.write,
            abandon: options.abandon,
            unfold: options.unfold,
        };
        return run_tree(
            root,
            &options,
            start,
            boundary,
            identities,
            cold_ms,
            Tree::Durable,
        );
    }
    if options.resume.is_some() {
        return run_tree(
            root,
            options,
            start,
            boundary,
            identities,
            cold_ms,
            Tree::Ordinary,
        );
    }
    if !Tree::Ordinary.live(root).is_dir() && !Tree::Durable.live(root).is_dir() {
        return Err(invalid("state root must contain wal/ or wal-durable/"));
    }
    let mut report = FoldReport::default();
    // Only durable archives are backed up. A container holding both populations
    // would either back up disposable sessions or omit durable ones.
    for tree in [Tree::Ordinary, Tree::Durable] {
        if !tree.live(root).is_dir() {
            continue;
        }
        let next = run_tree(root, options, start, boundary, identities, cold_ms, tree)?;
        report.candidates.extend(next.candidates);
        for (reason, sessions) in next.skipped {
            report.skipped.entry(reason).or_default().extend(sessions);
        }
        report.removed += next.removed;
        report.restored += next.restored;
        report.superseded += next.superseded;
        if next.journal.is_some() {
            report.journal = next.journal;
        }
        report.resolved_from_store.extend(next.resolved_from_store);
        report.cold_pre_schema.extend(next.cold_pre_schema);
        report.permanent_skip_notice = report.permanent_skip_notice.or(next.permanent_skip_notice);
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn run_tree(
    root: &Path,
    options: &FoldOptions,
    start: u64,
    boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
    identities: &BTreeMap<String, BindIdentity>,
    cold_ms: u64,
    tree: Tree,
) -> io::Result<FoldReport> {
    if options.unfold && (options.resume.is_none() || options.abandon) {
        return Err(invalid(
            "--unfold requires --resume and cannot combine with --abandon",
        ));
    }
    if options.abandon && options.resume.is_none() {
        return Err(invalid("--abandon requires --resume"));
    }
    let dir = tree.archive(root);
    let _pass = if options.write {
        Some(acquire_archive_pass(root).map_err(|e| match e {
            LeaseError::Held { .. } => {
                let path = crate::lease::archive_pass_lock_path(root);
                let quoted = format!("'{}'", path.display().to_string().replace('\'', "'\\''"));
                io::Error::new(io::ErrorKind::InvalidData, ArchivePassContended(format!("another mutating archive pass holds the fold lock at {}. Identify its current holder with: lsof -- {quoted}", path.display())))
            }
            other => invalid(format!("cannot acquire archive pass lease: {other}")),
        })?)
    } else {
        None
    };
    let mut report = FoldReport::default();
    if let Some(name) = &options.resume {
        let mut journal = Journal::load(journal_path(&dir, name)?)?;
        let resume_name = tree.resume_name(name);
        report.journal = Some(resume_name.clone());
        let ark = journal.path.with_extension("ark");
        if options.unfold {
            return unfold(root, &journal, &ark, options.write);
        }
        if matches!(journal.events.last(), Some(Event::Abandoned { .. })) {
            return Ok(report);
        }
        if matches!(journal.events.last(), Some(Event::Restarted)) && !options.abandon {
            // A crash can follow retirement but precede creation of the replacement
            // pass. Resume must still process the live candidates, not mistake the
            // retired journal for evidence that archiving finished.
            //
            // FINISH THE PREDECESSOR'S RETIREMENT FIRST. `Restarted` is appended and
            // fsynced BEFORE the unlink below, so a crash in that window leaves a
            // durable `Restarted` beside a container that still exists -- and this arm
            // used to recurse straight past the unlink, while `Journal::terminal`
            // treats `Restarted` as terminal so no later pass recovers it either. The
            // orphan then never leaves the directory.
            //
            // Idempotent by construction: the unlink is the step that may or may not
            // have run, so an already-absent file is the SUCCESS case, not an error.
            if options.write {
                match fs::remove_file(&ark) {
                    Ok(()) => sync_dir(&dir)?,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(invalid(format!(
                            "cannot retire superseded container {}: {e}",
                            ark.display()
                        )))
                    }
                }
            }
            drop(_pass);
            return run_tree(
                root,
                &FoldOptions {
                    write: options.write,
                    ..Default::default()
                },
                start,
                boundary,
                identities,
                cold_ms,
                tree,
            );
        }
        if options.abandon {
            let mut live_present = BTreeMap::new();
            for addr in journal.latest().keys() {
                live_present.insert(
                    addr.clone(),
                    live_bytes(&tree.live(root).join(format!("{addr}.wal")))?.is_some(),
                );
            }
            for (addr, present) in &live_present {
                report.skip(
                    if *present {
                        "abandoned_live_present"
                    } else {
                        "abandoned_live_missing"
                    },
                    addr,
                );
            }
            if options.write {
                journal.append(Event::Abandoned { live_present }, boundary)?;
            }
            return Ok(report);
        }
        if journal.latest().is_empty() && !ark.try_exists()? {
            finish(
                root,
                &mut journal,
                &ark,
                options.write,
                &mut report,
                boundary,
            )?;
            return Ok(report);
        }
        let mut reader = match ArchiveReader::open(&ark, MAX_BYTES) {
            Ok(reader) => reader,
            Err(error) => {
                let mut missing = Vec::new();
                for addr in journal.latest().keys() {
                    if live_bytes(&tree.live(root).join(format!("{addr}.wal")))?.is_none() {
                        missing.push(addr.clone());
                    }
                }
                return Err(invalid(format!("cannot resume {}: {error}. Missing live sessions: {missing:?}. If the container is gone these sessions have LOST DATA. --resume {resume_name} --abandon --write records explicit abandonment without deleting evidence", ark.display())));
            }
        };
        if reader.state()? == ContainerState::Sealed {
            if journal.terminal() {
                return Ok(report);
            }
            finish(
                root,
                &mut journal,
                &ark,
                options.write,
                &mut report,
                boundary,
            )?;
            return Ok(report);
        }
        if !options.write {
            return preview(root, start, identities, cold_ms, tree);
        }
        // No source was unlinked before the seal. Retire this attempt and start
        // from live bytes with a FRESH stamp: rebuilding under the old start time
        // could lose precedence to a stale member from a later interrupted pass.
        // Keep the retired journal as evidence rather than deleting its history.
        for addr in journal.latest().keys() {
            if live_bytes(&tree.live(root).join(format!("{addr}.wal")))?.is_none() {
                return Err(invalid(format!("unsealed pass {} has missing live session {addr}; refusing to discard potentially recoverable bytes. Repair the container or explicitly --abandon after investigation", ark.display())));
            }
        }
        journal.append(Event::Restarted, boundary)?;
        fs::remove_file(&ark)?;
        sync_dir(&dir)?;
    }
    if !options.write {
        return preview(root, start, identities, cold_ms, tree);
    }
    if let Some(path) = unfinished(&dir)?.first() {
        return Err(invalid(format!("unfinished archive pass {}; use --resume {} (or explicit --abandon) before starting another", path.display(), tree.resume_name(&path.file_name().unwrap().to_string_lossy()))));
    }
    broca_owner_only::create_dir_all(&dir)?;
    sync_dir(root)?;
    let mut stamp = start;
    for entry in fs::read_dir(&dir)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|e| e == "ark" || e == "journal")
        {
            let ts = broca_wal::archive::fold_timestamp(path.with_extension("ark"))?;
            stamp = stamp.max(
                ts.checked_add(1)
                    .ok_or_else(|| invalid("archive timestamp exhausted"))?,
            );
        }
    }
    let path = dir.join(format!("fold-{stamp}.journal"));
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    opts.open(&path)?.sync_all()?;
    sync_dir(&dir)?;
    let mut journal = Journal {
        path,
        events: Vec::new(),
        valid_len: 0,
    };
    let ark = journal.path.with_extension("ark");
    let mut writer = None;
    let leases = FileLeaseStore::new(root.join("leases"));
    for path in wal_paths(root, tree)? {
        let addr = path.file_stem().unwrap().to_string_lossy().into_owned();
        let Some(bytes) = live_bytes(&path)? else {
            report.skip("missing", &addr);
            continue;
        };
        let (session, from_store, _) = match candidate(&bytes, &addr, start, identities, cold_ms) {
            Ok(candidate) => candidate,
            Err(reason) => {
                report.skip(reason, &addr);
                continue;
            }
        };
        let _lease = match leases.acquire(&lease_key_for(&session)) {
            Ok(lease) => lease,
            Err(LeaseError::Held { .. }) => {
                report.skip("lease_held_phase1", &addr);
                continue;
            }
            Err(e) => return Err(invalid(e.to_string())),
        };
        if move_protected(root, &session) {
            report.skip("move_fenced", &addr);
            continue;
        }
        let Some(bytes) = live_bytes(&path)? else {
            report.skip("missing", &addr);
            continue;
        };
        let (_, _, pre_schema) = match candidate(&bytes, &addr, start, identities, cold_ms) {
            Ok(candidate) => candidate,
            Err(reason) => {
                report.skip(reason, &addr);
                continue;
            }
        };
        // Create before Intended: a durable session record must always have a
        // container to recover, even if the first append is interrupted.
        if writer.is_none() {
            // Before writing a v2 index, the seal adds `archive-index/v2` to the WAL feature
            // floor in the state root (next to `wal/`), which `scripts/place-release.sh` reads
            // to refuse builds that cannot read v2 indexes.
            writer = Some(
                ArchiveWriter::create(&ark)?
                    .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME)),
            );
            sync_dir(&dir)?;
        }
        let writer = writer.as_mut().unwrap();
        let digest = digest(&bytes);
        journal.append(
            Event::Session {
                addr: addr.clone(),
                session: session.clone(),
                digest,
                state: State::Intended,
            },
            boundary,
        )?;
        writer.append(&addr, &bytes)?;
        // Seal syncs these bytes on the same handle before any live source is
        // unlinked, so a per-member archive sync would be redundant. The journal
        // syncs are not: recovery needs Intended and Archived to decide what to
        // resume; a lost Archived record cannot be reconstructed from the archive.
        journal.append(
            Event::Session {
                addr: addr.clone(),
                session,
                digest,
                state: State::Archived,
            },
            boundary,
        )?;
        if from_store {
            report.resolved_from_store.push(addr.clone());
        }
        if pre_schema {
            report.cold_pre_schema.push(addr.clone());
        }
        report.candidates.push(addr);
    }
    if let Some(writer) = writer {
        writer.seal()?;
        sync_dir(&dir)?;
    }
    #[cfg(test)]
    AFTER_SEAL.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    report.journal = Some(tree.resume_name(&journal.path.file_name().unwrap().to_string_lossy()));
    finish(root, &mut journal, &ark, true, &mut report, boundary)?;
    Ok(report)
}
fn wal_paths(root: &Path, tree: Tree) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    // Retained WALs never enter either archive, even before their deletion deadline.
    for entry in fs::read_dir(tree.live(root))? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|e| e == "wal") && entry.file_type()?.is_file() {
            if entry.path().with_extension("moving").try_exists()?
                || entry.path().with_extension("moved").try_exists()?
            {
                continue;
            }
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}
fn preview(
    root: &Path,
    start: u64,
    identities: &BTreeMap<String, BindIdentity>,
    cold_ms: u64,
    tree: Tree,
) -> io::Result<FoldReport> {
    let mut report = FoldReport::default();
    for path in wal_paths(root, tree)? {
        let addr = path.file_stem().unwrap().to_string_lossy().into_owned();
        let Some(bytes) = live_bytes(&path)? else {
            report.skip("missing", &addr);
            continue;
        };
        match candidate(&bytes, &addr, start, identities, cold_ms) {
            Ok((session, from_store, pre_schema)) => {
                if move_protected(root, &session) {
                    report.skip("move_fenced", &addr);
                    continue;
                }
                if from_store {
                    report.resolved_from_store.push(addr.clone());
                }
                if pre_schema {
                    report.cold_pre_schema.push(addr.clone());
                }
                report.candidates.push(addr);
            }
            Err(reason) => report.skip(reason, &addr),
        }
    }
    Ok(report)
}
fn finish(
    root: &Path,
    journal: &mut Journal,
    ark: &Path,
    write: bool,
    report: &mut FoldReport,
    boundary: &mut dyn FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    // Empty journals have no sources to unlink and need no seal. Complete them
    // durably so an explicit resume of a pre-admission crash is idempotent.
    if journal.latest().is_empty() {
        if write && !journal.terminal() {
            journal.append(Event::Complete, boundary)?;
        }
        return Ok(());
    }
    let leases = FileLeaseStore::new(root.join("leases"));
    let mut reader = ArchiveReader::open(ark, MAX_BYTES)?;
    if reader.state()? != ContainerState::Sealed {
        return Err(invalid("cannot unlink before archive seal"));
    }
    if write {
        // A process may die after writing the seal but before its fsync returns.
        // Recovery must establish durability, not infer it from readable magic.
        // A WRITABLE handle: Windows FlushFileBuffers needs write access, so a
        // read-only File::open fails here with PermissionDenied while working on unix.
        OpenOptions::new()
            .write(true)
            .open(ark)
            .map_err(at(ark))?
            .sync_all()
            .map_err(at(ark))?;
        sync_dir(ark.parent().unwrap())?;
    }
    for (addr, event) in journal.latest() {
        let Event::Session {
            session,
            digest: before,
            state,
            ..
        } = event
        else {
            unreachable!()
        };
        if matches!(state, State::Done | State::Superseded) {
            continue;
        }
        if state != State::Archived {
            return Err(invalid("sealed pass contains an intended member"));
        }
        if !write {
            report.candidates.push(addr);
            continue;
        }
        let _lease = match leases.acquire(&lease_key_for(&session)) {
            Ok(lease) => lease,
            Err(LeaseError::Held { .. }) => {
                report.skip("lease_held_phase2", &addr);
                continue;
            }
            Err(e) => return Err(invalid(e.to_string())),
        };
        if move_protected(root, &session) {
            report.skip("move_fenced", &addr);
            continue;
        }
        let member = reader
            .read_member(&addr)?
            .ok_or_else(|| invalid(format!("archive missing {addr}")))?;
        if digest(&member.payload) != before {
            return Err(invalid(format!("journal/member digest mismatch: {addr}")));
        }
        let path = Tree::for_journal(journal)
            .live(root)
            .join(format!("{addr}.wal"));
        let state = match live_bytes(&path)? {
            Some(bytes) if digest(&bytes) != before => {
                report.superseded += 1;
                State::Superseded
            }
            Some(_) => {
                fs::remove_file(&path).map_err(at(&path))?;
                sync_dir(&Tree::for_journal(journal).live(root))?;
                report.removed += 1;
                State::Done
            }
            // A crash after unlink but before done, or another completed pass,
            // leaves an absent live file. The verified sealed member is sufficient.
            None => State::Done,
        };
        journal.append(
            Event::Session {
                addr,
                session,
                digest: before,
                state,
            },
            boundary,
        )?;
    }
    if write
        && journal.latest().values().all(|e| {
            matches!(
                e,
                Event::Session {
                    state: State::Done | State::Superseded,
                    ..
                }
            )
        })
    {
        journal.append(Event::Complete, boundary)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    // Pins journal barrier calls in tests, not persistence across a power loss.
    static JOURNAL_SYNCS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static AFTER_SEAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
}

fn unfold(root: &Path, journal: &Journal, ark: &Path, write: bool) -> io::Result<FoldReport> {
    let mut reader = ArchiveReader::open(ark, MAX_BYTES).map_err(|e| {
        invalid(format!(
            "cannot unfold absent or unreadable container {}: {e}",
            ark.display()
        ))
    })?;
    let mut report = FoldReport::default();
    if reader.state()? == ContainerState::Unsealed {
        return Ok(report);
    }
    let entries = journal.latest();
    let leases = FileLeaseStore::new(root.join("leases"));
    let scanned = reader.rescan()?;
    let addresses = match reader.verified_members()? {
        // A verified index lists every member, while the header walk stops at the first
        // damaged member magic and silently skips every member after it. Enumerate from the
        // index, after checking that every header the walk did read agrees with it; a member
        // whose header is damaged then fails loudly in `read_member` below.
        Some(indexed) => {
            if scanned.len() > indexed.len()
                || scanned.iter().zip(&indexed).any(|(header, entry)| {
                    header.session_addr != entry.session_addr
                        || header.offset != entry.offset
                        || header.payload_len != entry.payload_len
                })
            {
                return Err(invalid(format!(
                    "cannot unfold {}: its index disagrees with its member headers",
                    ark.display()
                )));
            }
            indexed
                .into_iter()
                .map(|member| member.session_addr)
                .collect::<Vec<_>>()
        }
        None => scanned.into_iter().map(|info| info.session_addr).collect(),
    };
    for session_addr in addresses {
        let Some(Event::Session {
            session,
            digest: before,
            ..
        }) = entries.get(&session_addr)
        else {
            return Err(invalid(format!(
                "container member {session_addr} lacks journal identity"
            )));
        };
        let member = reader
            .read_member(&session_addr)?
            .ok_or_else(|| invalid("missing member"))?;
        if digest(&member.payload) != *before {
            return Err(invalid("unfold journal/member digest mismatch"));
        }
        let _lease = if write {
            match leases.acquire(&lease_key_for(session)) {
                Ok(lease) => Some(lease),
                Err(LeaseError::Held { .. }) => {
                    report.skip("lease_held_unfold", &session_addr);
                    continue;
                }
                Err(e) => return Err(invalid(e.to_string())),
            }
        } else {
            None
        };
        if move_protected(root, session) {
            report.skip("move_fenced", &session_addr);
            continue;
        }
        let archives = fs::read_dir(ark.parent().unwrap())?
            .map(|entry| entry.map(|e| e.path()))
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e == "ark"))
            .collect::<Vec<_>>();
        let newest = broca_wal::archive::read_newest_member(&archives, &session_addr, MAX_BYTES)?
            .ok_or_else(|| invalid("unfold member is no longer reachable"))?;
        if digest(&newest.payload) != *before {
            return Err(invalid(format!(
                "refusing stale unfold of newer archived session {}",
                session_addr
            )));
        }
        let path = Tree::for_journal(journal)
            .live(root)
            .join(format!("{}.wal", session_addr));
        if let Some(bytes) = live_bytes(&path)? {
            if digest(&bytes) != *before {
                return Err(invalid(format!(
                    "refusing to overwrite appended live session {}",
                    session_addr
                )));
            }
            continue;
        }
        report.candidates.push(session_addr.clone());
        if !write {
            continue;
        }
        let temp = path.with_extension("unfold-tmp");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        if temp.exists() {
            // Only our deterministic temp pathname is replaced, under the session
            // lease. A failed prior extraction never made it authoritative.
            fs::remove_file(&temp)?;
        }
        let mut file = options.open(&temp)?;
        file.write_all(&member.payload)?;
        file.sync_all()?;
        fs::rename(&temp, &path)?;
        sync_dir(&Tree::for_journal(journal).live(root))?;
        report.restored += 1;
    }
    Ok(report)
}

#[cfg(test)]
mod tests;

/// Typed lease contention, retaining the operator-facing error text and kind.
#[derive(Debug)]
pub struct ArchivePassContended(String);
impl std::fmt::Display for ArchivePassContended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ArchivePassContended {}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    use broca_wal::Wal;
    #[test]
    fn windows_fold_journal_archive_and_unfold_temp_are_private() {
        let fixture = BroadParent::new("fold");
        let mut records = crate::retention::tests::frames(None);
        let session = match &mut records[0].record {
            WalRecord::RunStarted { session, ts_ms, .. } => {
                *ts_ms = Some(1);
                session.clone()
            }
            _ => panic!("fixture lacks run start"),
        };
        let path = fixture
            .path
            .join("wal")
            .join(format!("{}.wal", session_addr(&session)));
        let mut wal = broca_wal::FileWal::open(&path).unwrap();
        for frame in records {
            wal.append(frame.fence, &frame.record, true).unwrap();
        }
        drop(wal);
        let seen = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.borrow_mut().push(path.to_path_buf());
            },
            || {
                let report = run(
                    &fixture.path,
                    &FoldOptions {
                        write: true,
                        ..Default::default()
                    },
                    COLD_MS * 3,
                    &mut |_| Ok(()),
                )
                .unwrap();
                assert_eq!(report.removed, 1);
                let journal = std::fs::read_dir(fixture.path.join("wal-archive"))
                    .unwrap()
                    .map(|e| e.unwrap().path())
                    .find(|p| p.extension().is_some_and(|e| e == "journal"))
                    .unwrap();
                let report = run(
                    &fixture.path,
                    &FoldOptions {
                        write: true,
                        unfold: true,
                        resume: Some(journal.file_name().unwrap().to_string_lossy().into_owned()),
                        ..Default::default()
                    },
                    COLD_MS * 3 + 1,
                    &mut |_| Ok(()),
                )
                .unwrap();
                assert_eq!(report.restored, 1);
            },
        );
        for extension in ["journal", "ark", "unfold-tmp"] {
            assert!(
                seen.borrow()
                    .iter()
                    .any(|p| p.extension().is_some_and(|e| e == extension)),
                "{extension} was not inspected before cleanup"
            );
        }
        assert_private(&path);
        assert_private(&fixture.path.join("wal-archive"));
    }
}
