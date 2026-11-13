//! `media-check`: the image engine fed each log two ways, compared. For each log, today's records
//! are folded by today's entry point, and the log's conversion by the session store's media fold
//! and the events it hands over, appended as the staging writer appends them; the two are compared
//! before every run starts and at the end, field by field (see
//! `broca_session_convert::media_compare`). Nothing is staged or written but the scratch copy of
//! an archive member, under `--dir`.
//!
//! The report counts the logs checked, the run starts compared, the media captured and the
//! differences, by field, and lists each log that captured a medium or differs with its counts.
//! It shows the first fifty differences whole: capture ids, media references, positions and
//! counts, never a message's text. A line on standard error sums it up. Any difference, or a log
//! that couldn't be compared, makes the exit code non-zero.

use std::collections::BTreeMap;

use broca_session_convert::media_compare::{self, Difference};
use broca_wal::WalRecord;
use serde::Serialize;

use super::{each_log, write_report, Args, Item};

#[derive(Default, Serialize)]
struct MediaCheckReport {
    /// Logs converted and compared.
    logs: u64,
    /// Run starts compared at.
    runs: u64,
    /// Media today's fold captured, over every log checked.
    images: u64,
    differences: u64,
    /// Differences by field.
    by_field: BTreeMap<String, u64>,
    /// Each log that captured a medium or differs: what was compared, and its differences by
    /// field.
    sessions: Vec<Session>,
    /// The first fifty differences, each with its log.
    examples: Vec<Example>,
    /// Logs that start no run, so hold no session to compare.
    no_run: u64,
    /// Logs that couldn't be read, converted or folded, by error.
    failed: BTreeMap<String, u64>,
    /// Up to five log names for each failure.
    failed_examples: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize)]
struct Session {
    log: String,
    runs: u64,
    images: u64,
    differences: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct Example {
    log: String,
    #[serde(flatten)]
    difference: Difference,
}

impl MediaCheckReport {
    fn fail(&mut self, name: &str, error: &str) {
        let key: String = error.chars().take(120).collect();
        *self.failed.entry(key.clone()).or_default() += 1;
        let examples = self.failed_examples.entry(key).or_default();
        if examples.len() < 5 {
            examples.push(name.to_owned());
        }
    }
}

pub(super) fn run(args: &Args) {
    let report = check(args);
    write_report(&report, args.out_file.as_deref());
    eprintln!(
        "media-check: {} logs checked, {} runs compared, {} images, {} differences; {} logs \
         failed",
        report.logs,
        report.runs,
        report.images,
        report.differences,
        report.failed.values().sum::<u64>(),
    );
    if report.differences > 0 || !report.failed.is_empty() {
        std::process::exit(1);
    }
}

fn check(args: &Args) -> MediaCheckReport {
    let mut report = MediaCheckReport::default();
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
            } => return report.fail(&name, &e),
            Item::Note(..) => return,
        };
        if !old
            .records
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
        {
            report.no_run += 1;
            return;
        }
        let mut converted =
            match broca_session_convert::convert(&old.records, old.lineage.as_deref()) {
                Ok(converted) => converted,
                Err(error) => return report.fail(&name, &format!("convert: {}", error.0)),
            };
        // As the staging writer creates the session: with `appends_in_conversation` off for a log
        // written under today's current compaction contract (`broca_core::is_mode3_log`), on for
        // any other.
        let appends_in_conversation =
            !broca_core::is_mode3_log(old.records.iter().map(|frame| &frame.record));
        let checked =
            match media_compare::check(&old.records, &mut converted, appends_in_conversation) {
                Ok(checked) => checked,
                Err(error) => return report.fail(&name, &format!("fold: {error}")),
            };
        report.logs += 1;
        report.runs += checked.runs;
        report.images += checked.images;
        report.differences += checked.differences.len() as u64;
        let mut by_field = BTreeMap::new();
        for difference in &checked.differences {
            *by_field.entry(difference.field.clone()).or_default() += 1;
            *report.by_field.entry(difference.field.clone()).or_default() += 1;
        }
        if checked.images > 0 || !by_field.is_empty() {
            report.sessions.push(Session {
                log: name.clone(),
                runs: checked.runs,
                images: checked.images,
                differences: by_field,
            });
        }
        for difference in checked.differences {
            if report.examples.len() < 50 {
                report.examples.push(Example {
                    log: name.clone(),
                    difference,
                });
            }
        }
    });
    report
}
