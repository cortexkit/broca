//! `move-check`: what moving each log's session into the new storage would do, run on copies in
//! a scratch folder, never a daemon's state root. For each log, the WAL's own preconditions
//! (`preconditions`), then the staging writer writes the session under
//! `<scratch>/session-store/staging/` with the setup parts in `<scratch>/session-store/setup/`,
//! then the verifier checks it, in its move mode or, with `--all-steps`, its offline mode, which
//! also rebuilds every recorded model request. Each staged session is deleted after its check.
//!
//! The report counts the logs verified, and the refusals and first mismatches by reason. A
//! mismatch's reason is its check and where the answers first differ, never a value.

use std::collections::BTreeMap;

use broca_session_convert::stage::{Stage, StageError};
use broca_session_convert::verify::{Mode, Verify};
use broca_session_store::keys::DeploymentKey;
use broca_wal::WalRecord;
use futures::executor::block_on;
use serde::Serialize;

use super::{each_log, write_report, Args, Item};

#[derive(Default, Serialize)]
struct MoveCheckReport {
    logs: u64,
    /// Logs that couldn't be read, by error.
    unreadable: BTreeMap<String, u64>,
    /// Logs that start no run, so hold no session to move.
    no_run: u64,
    verified: u64,
    verified_runs: u64,
    verified_messages: u64,
    /// Refusals before anything is written, by reason (`not_movable {reason}`).
    not_movable: BTreeMap<String, u64>,
    /// Conversions that would not be exact, by what was lost.
    lossy: BTreeMap<String, u64>,
    /// Staging writes that failed, by error.
    stage_failed: BTreeMap<String, u64>,
    /// Verifications that failed, by check and where the answers first differ.
    mismatches: BTreeMap<String, u64>,
    /// Up to five log names for each refusal or mismatch above.
    examples: BTreeMap<String, Vec<String>>,
}

impl MoveCheckReport {
    fn count(map: &mut BTreeMap<String, u64>, key: &str) {
        *map.entry(key.chars().take(120).collect()).or_default() += 1;
    }

    fn example(&mut self, key: &str, name: &str) {
        let examples = self
            .examples
            .entry(key.chars().take(120).collect())
            .or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
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
    let (map, key) = match &error {
        StageError::NotMovable(reason) => (&mut report.not_movable, (*reason).to_owned()),
        StageError::Lossy(lost) => (&mut report.lossy, lost.clone()),
        StageError::Failed(failed) => (&mut report.stage_failed, failed.clone()),
    };
    MoveCheckReport::count(map, &key);
    report.example(&key, name);
}

pub(super) fn run(args: &Args) {
    let scratch = args
        .dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("move-check-{}", std::process::id())))
        .join("move-check");
    let store_root = scratch.join("session-store");
    let staging = store_root.join("staging").join("store-move-check");
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let mode = if args.all_steps {
        Mode::Offline
    } else {
        Mode::Move
    };
    let mut report = MoveCheckReport::default();
    let mut index = 0usize;
    each_log(args, |item| {
        // `--every N` checks one log in N, in the order the logs are visited.
        if matches!(item, Item::Log { .. }) {
            index += 1;
            if !(index - 1).is_multiple_of(args.every) {
                return;
            }
        }
        let (name, old) = match item {
            Item::Log {
                name, old: Ok(old), ..
            } => (name, old),
            Item::Log {
                name, old: Err(e), ..
            } => {
                report.logs += 1;
                MoveCheckReport::count(&mut report.unreadable, &e);
                report.example(&e, &name);
                return;
            }
            Item::Note(..) => return,
        };
        report.logs += 1;
        let Some(session) = old.records.iter().find_map(|frame| match &frame.record {
            WalRecord::RunStarted { session, .. } => Some(session.clone()),
            _ => None,
        }) else {
            report.no_run += 1;
            return;
        };
        if let Some(refusal) =
            broca_session_convert::preconditions::wal_refusal(&old.records, old.lineage_id)
        {
            MoveCheckReport::count(&mut report.not_movable, refusal);
            report.example(refusal, &name);
            return;
        }
        let prepared =
            match broca_session_convert::stage::prepare(&old.records, old.lineage.as_deref())
                .and_then(|prepared| {
                    match block_on(broca_session_convert::preconditions::converted_refusal(
                        &prepared,
                    )) {
                        Ok(None) => Ok(prepared),
                        Ok(Some(refusal)) => Err(StageError::NotMovable(refusal)),
                        Err(error) => Err(StageError::Failed(error)),
                    }
                }) {
                Ok(prepared) => prepared,
                Err(error) => return refused(&mut report, &name, error),
            };
        let _ = std::fs::remove_dir_all(&staging);
        let stage = Stage {
            records: &old.records,
            lineage: old.lineage.as_deref(),
            session: &session,
            deployment: &deployment,
            store_root: &store_root,
            staging: &staging,
        };
        let staged = match block_on(broca_session_convert::stage::write(&stage, prepared)) {
            Ok(staged) => staged,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                return refused(&mut report, &name, error);
            }
        };
        let verified = block_on(broca_session_convert::verify::verify(Verify {
            records: &old.records,
            lineage: old.lineage_id,
            session: &session,
            deployment: &deployment,
            store_root: &store_root,
            staging: &staging,
            converted: &staged.converted,
            mode,
        }));
        match verified {
            Ok(verified) => {
                report.verified += 1;
                report.verified_runs += verified.runs;
                report.verified_messages += verified.messages;
            }
            Err(mismatch) => {
                let reason = reason(&mismatch);
                MoveCheckReport::count(&mut report.mismatches, &reason);
                report.example(&reason, &name);
            }
        }
        let _ = std::fs::remove_dir_all(&staging);
    });
    let _ = std::fs::remove_dir_all(&scratch);
    write_report(&report, args.out_file.as_deref());
}
