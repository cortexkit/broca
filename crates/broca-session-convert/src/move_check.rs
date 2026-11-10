//! `move-check`: what moving each log's session into the new storage would do, run on copies in
//! a scratch folder, never a daemon's state root. For each log, the WAL's own preconditions
//! (`preconditions`), then the staging writer writes the session under
//! `<scratch>/session-store/staging/` with the setup parts in `<scratch>/session-store/setup/`,
//! then the verifier checks it, in its move mode or, with `--all-steps`, its offline mode, which
//! also rebuilds every recorded model request. Each staged session is deleted after its check.
//!
//! A session's media are read from today's media store, as a move reads them: the store of the
//! class the folder of the session's log chooses, in the state root that folder sits in (see
//! [`todays_media`]). So a rehearsal over a copy of a whole state root names its log folders,
//! `wal`, `wal-durable`, `wal-ephemeral`, `wal-archive` and `wal-archive-durable`, and checks each
//! session with its media. A log outside those folders has no media store, so a session naming
//! media is refused `media_missing`, as a move with no bytes for them would be.
//!
//! The report counts the logs verified, and the refusals and first mismatches by reason, then
//! every log that wouldn't move by class, and how long reading, staging and verifying took. A
//! mismatch's reason is its check and where the answers first differ, never a value.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use broca_core::media_store::{MediaBytes, MediaReadError, MediaStoreClass, ReadOnlyMediaStore};
use broca_protocol::{BindIdentity, MediaRef};
use broca_session_convert::preconditions::{
    unsupported_in_wal, FIRST_PROTOCOL_COMPACTION, UNSUPPORTED_FEATURE,
};
use broca_session_convert::stage::{Stage, StageError};
use broca_session_convert::verify::{MediaRoot, Mode, Verify};
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde::Serialize;

use super::{each_log, percentiles, write_report, Args, Item, Percentiles};

#[cfg(test)]
mod tests;

#[derive(Default, Serialize)]
struct MoveCheckReport {
    /// How much each staged session was verified: `move`, as a move verifies, or `offline`
    /// (`--all-steps`), which also rebuilds every recorded model request.
    mode: &'static str,
    logs: u64,
    /// Logs that couldn't be read, by error.
    unreadable: BTreeMap<String, u64>,
    /// Logs that start no run, so hold no session to move.
    no_run: u64,
    verified: u64,
    verified_runs: u64,
    verified_messages: u64,
    /// The media objects the verified sessions were staged with.
    verified_media: u64,
    /// Checked steps that agree with today's offline replay but not the historical digest.
    legacy_replay_limit: u64,
    legacy_replay_limit_examples: Vec<String>,
    /// Refusals before anything is written, by reason (`not_movable {reason}`).
    not_movable: BTreeMap<String, u64>,
    /// Conversions that would not be exact, by what was lost.
    lossy: BTreeMap<String, u64>,
    /// Staging writes that failed, by error.
    stage_failed: BTreeMap<String, u64>,
    /// Verifications that failed, by check and where the answers first differ.
    mismatches: BTreeMap<String, u64>,
    /// Every log that wouldn't move, by class: `unreadable`, `no_run`, each `not_movable`
    /// reason (`media_missing` among them), `conversion_lossy`, `stage_failed` and
    /// `verification_failed`. With `verified`, they add up to `logs`.
    refused: BTreeMap<String, u64>,
    /// The `unsupported_feature` refusals by the feature refused: `approval_questions`,
    /// `input_token_budget` or `first_protocol_compaction`.
    unsupported_features: BTreeMap<String, u64>,
    timing: Timing,
    /// Up to five log names for each full refusal or mismatch, without truncating its error text.
    examples: BTreeMap<String, Vec<String>>,
}

/// How long the check took, in microseconds.
#[derive(Default, Serialize)]
struct Timing {
    /// The whole check, from reading the first log to the report.
    wall_us: u64,
    /// Reading the logs, each counted in whole milliseconds.
    read_us: u64,
    /// Checking the preconditions, converting, reading the media and writing each staged
    /// session, refused ones included.
    stage_us: u64,
    /// Verifying the staged sessions.
    verify_us: u64,
    /// Each verified session's read, stage and verify together: what converting it takes.
    per_verified_session_us: Percentiles,
}

fn micros(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_micros()).unwrap_or(u64::MAX)
}

impl MoveCheckReport {
    fn count(map: &mut BTreeMap<String, u64>, key: &str) {
        *map.entry(key.chars().take(120).collect()).or_default() += 1;
    }

    fn example(&mut self, key: &str, name: &str) {
        let examples = self.examples.entry(key.to_owned()).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }

    /// Counts a log that wouldn't move under its class.
    fn refuse(&mut self, class: &str) {
        Self::count(&mut self.refused, class);
    }
}

/// A mismatch's reason without what's particular to one log: the check, and where the answers
/// first differ, without the run, page or message it was found in.
fn reason(mismatch: &broca_session_convert::verify::Mismatch) -> String {
    match mismatch.detail.rsplit_once("differs at ") {
        Some((_, path)) => format!("{} at {path}", mismatch.check),
        None => format!("{}: {}", mismatch.check, mismatch.detail),
    }
}

/// Counts a session staging refused or failed.
fn refused(report: &mut MoveCheckReport, name: &str, error: StageError) {
    let (map, key, class) = match &error {
        StageError::NotMovable(reason) => (&mut report.not_movable, (*reason).to_owned(), *reason),
        StageError::MediaMissing(_) => (
            &mut report.not_movable,
            "media_missing".to_owned(),
            "media_missing",
        ),
        StageError::Lossy(lost) => (&mut report.lossy, lost.clone(), "conversion_lossy"),
        StageError::Failed(failed) => (&mut report.stage_failed, failed.clone(), "stage_failed"),
    };
    MoveCheckReport::count(map, &key);
    report.refuse(class);
    report.example(&error.to_string(), name);
}

/// Today's media store of one class, opened read only, so a check never creates one.
#[derive(Debug)]
struct ReadOnly(ReadOnlyMediaStore);

impl MediaBytes for ReadOnly {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        self.0.read_verified(reference)
    }
}

/// Today's media store for one session: the store, and where it lives.
struct TodaysMedia {
    store: ReadOnly,
    state_root: std::path::PathBuf,
    class: MediaStoreClass,
}

/// The class of media store that holds the media of a log in the state root's folder `folder`:
/// the folders today's storage keeps live and archived logs in.
fn class_of_folder(folder: &str) -> Option<MediaStoreClass> {
    match folder {
        "wal" | "wal-archive" => Some(MediaStoreClass::NonDurable),
        "wal-durable" | "wal-archive-durable" => Some(MediaStoreClass::Durable),
        "wal-ephemeral" => Some(MediaStoreClass::Ephemeral),
        _ => None,
    }
}

/// Today's media store for the session whose log was read from `source` (the `.wal` file, or the
/// archive container holding it), as a move finds it for a live log: the store of the class the
/// log's folder chooses, in the state root that folder sits in. `None` when the session's
/// `records` name no medium, when the log isn't in one of a state root's log folders, or when
/// that store doesn't exist; a session naming media is then refused `media_missing`.
fn todays_media(source: &Path, records: &[FramedRecord]) -> Option<TodaysMedia> {
    if broca_session_convert::stage::media_digests(records).is_empty() {
        return None;
    }
    let folder = source.parent()?;
    let class = class_of_folder(folder.file_name()?.to_str()?)?;
    let state_root = folder.parent()?.to_path_buf();
    let root = state_root.join(class.directory());
    if !(root.join("objects").is_dir() && root.join("refs").is_dir()) {
        return None;
    }
    let store = ReadOnlyMediaStore::open_existing(&state_root, class).ok()?;
    Some(TodaysMedia {
        store: ReadOnly(store),
        state_root,
        class,
    })
}

/// The rehearsal never allocates a served-history row in its source. An existing row is
/// authoritative; without one it uses `LineageRegistry::served_snapshot`'s no-source fallback.
/// A publishing conversion instead calls `served_snapshot`, whose row allocation has a clock floor.
fn source_history_version(
    source: &Path,
    session: &BindIdentity,
    has_log: bool,
) -> Result<u64, String> {
    use broca_core::ServedHistory as _;
    let fallback = u64::from(has_log);
    let Some(folder) = source.parent() else {
        return Ok(fallback);
    };
    if folder
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(class_of_folder)
        .is_none()
    {
        return Ok(fallback);
    }
    let Some(root) = folder.parent() else {
        return Ok(fallback);
    };
    let index = root.join("run-index.db");
    if !index.try_exists().map_err(|error| error.to_string())? {
        return Ok(fallback);
    }
    let store =
        broca_store::SqliteStore::open_read_only(index).map_err(|error| error.to_string())?;
    Ok(store
        .history_read_version(&broca_core::session_addr(session))?
        .unwrap_or(fallback))
}

pub(super) fn run(args: &Args) {
    write_report(&check(args), args.out_file.as_deref());
}

fn check(args: &Args) -> MoveCheckReport {
    let started = Instant::now();
    let scratch = args
        .dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("move-check-{}", std::process::id())))
        .join("move-check");
    let store_root = scratch.join("session-store");
    let staging = store_root.join("staging").join("store-move-check");
    // `--plain` chooses the mode the staged sessions are created in, as
    // `session_storage.encryption` does for a daemon's move.
    let deployment = crate::deployment();
    let mode = if args.all_steps {
        Mode::Offline
    } else {
        Mode::Move
    };
    let mut report = MoveCheckReport {
        mode: match mode {
            Mode::Move => "move",
            Mode::Offline => "offline",
        },
        ..MoveCheckReport::default()
    };
    let mut per_session = Vec::new();
    let mut index = 0usize;
    each_log(args, |item| {
        // `--every N` checks one log in N, in the order the logs are visited.
        if matches!(item, Item::Log { .. }) {
            index += 1;
            if !(index - 1).is_multiple_of(args.every) {
                return;
            }
        }
        let (name, old, read_ms) = match item {
            Item::Log {
                name,
                old: Ok(old),
                read_ms,
                ..
            } => (name, old, read_ms),
            Item::Log {
                name, old: Err(e), ..
            } => {
                report.logs += 1;
                MoveCheckReport::count(&mut report.unreadable, &e);
                report.refuse("unreadable");
                report.example(&e, &name);
                return;
            }
            Item::Note(..) => return,
        };
        report.logs += 1;
        let read_us = u64::try_from(read_ms.saturating_mul(1000)).unwrap_or(u64::MAX);
        report.timing.read_us += read_us;
        let Some(session) = old.records.iter().find_map(|frame| match &frame.record {
            WalRecord::RunStarted { session, .. } => Some(session.clone()),
            _ => None,
        }) else {
            report.no_run += 1;
            report.refuse("no_run");
            return;
        };
        let staging_started = Instant::now();
        let staged = (|| {
            if let Some(refusal) =
                broca_session_convert::preconditions::wal_refusal(&old.records, old.lineage_id)
            {
                if refusal == UNSUPPORTED_FEATURE {
                    if let Some(feature) = unsupported_in_wal(&old.records) {
                        MoveCheckReport::count(&mut report.unsupported_features, feature);
                    }
                }
                return Err(StageError::NotMovable(refusal));
            }
            let media = todays_media(&old.source, &old.records);
            let prepared = broca_session_convert::stage::prepare(
                &old.records,
                old.lineage.as_deref(),
                media.as_ref().map(|media| &media.store as &dyn MediaBytes),
            )?;
            match block_on(broca_session_convert::preconditions::converted_refusal(
                &prepared,
            )) {
                Ok(None) => {}
                Ok(Some(refusal)) => {
                    if refusal == UNSUPPORTED_FEATURE {
                        MoveCheckReport::count(
                            &mut report.unsupported_features,
                            FIRST_PROTOCOL_COMPACTION,
                        );
                    }
                    return Err(StageError::NotMovable(refusal));
                }
                Err(error) => return Err(StageError::Failed(error)),
            }
            let _ = std::fs::remove_dir_all(&staging);
            // As a move records it: the last update today's head serves for the session.
            let todays_updated_at =
                broca_module::head_time::updated_at_of_wal(&old.records, old.written_ms);
            let moved = broca_session_store::record::Moved {
                source_lineage: None,
                move_id: "store-move:move-check".into(),
                source_activity_ms: todays_updated_at,
            };
            let stage = Stage {
                source_history_version: source_history_version(
                    &old.source,
                    &session,
                    !old.records.is_empty(),
                )
                .map_err(StageError::Failed)?,
                records: &old.records,
                lineage: old.lineage.as_deref(),
                session: &session,
                deployment: &deployment,
                store_root: &store_root,
                staging: &staging,
                media: media.as_ref().map(|media| &media.store as &dyn MediaBytes),
                moved: &moved,
            };
            let staged = block_on(broca_session_convert::stage::write(&stage, prepared))?;
            Ok((staged, media, todays_updated_at))
        })();
        let stage_us = micros(staging_started);
        report.timing.stage_us += stage_us;
        let (staged, media, todays_updated_at) = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                return refused(&mut report, &name, error);
            }
        };
        let verify_started = Instant::now();
        let verified = block_on(broca_session_convert::verify::verify(Verify {
            records: &old.records,
            lineage: old.lineage_id,
            session: &session,
            deployment: &deployment,
            store_root: &store_root,
            staging: &staging,
            converted: &staged.converted,
            mode,
            media: media.as_ref().map(|media| &media.store as &dyn MediaBytes),
            media_root: media.as_ref().map(|media| MediaRoot {
                state_root: &media.state_root,
                class: media.class,
            }),
            todays_updated_at,
        }));
        let verify_us = micros(verify_started);
        report.timing.verify_us += verify_us;
        match verified {
            Ok(verified) => {
                report.verified += 1;
                report.verified_runs += verified.runs;
                report.verified_messages += verified.messages;
                report.verified_media += verified.media;
                report.legacy_replay_limit += verified.legacy_replay_limit;
                if verified.legacy_replay_limit > 0 {
                    eprintln!("WARN legacy_replay_limit: {name}: {} checked step(s); converted requests equal today's offline render", verified.legacy_replay_limit);
                    for step in &verified.legacy_replay_limit_examples {
                        if report.legacy_replay_limit_examples.len() < 5 {
                            report
                                .legacy_replay_limit_examples
                                .push(format!("{name}: step {} of {}", step.step, step.run_id));
                        }
                    }
                }
                per_session.push(read_us + stage_us + verify_us);
            }
            Err(mismatch) => {
                let reason = reason(&mismatch);
                MoveCheckReport::count(&mut report.mismatches, &reason);
                report.refuse("verification_failed");
                report.example(&mismatch.to_string(), &name);
            }
        }
        let _ = std::fs::remove_dir_all(&staging);
    });
    let _ = std::fs::remove_dir_all(&scratch);
    report.timing.per_verified_session_us = percentiles(per_session);
    report.timing.wall_us = micros(started);
    report
}
