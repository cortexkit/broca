//! `session-convert store-load-check [--out FILE] [--every N] ROOT...`: checks, read-only, that
//! every session of the new storage under each `ROOT` (a `session-store` folder, or a folder of
//! them) loads alike through today's load and through a reference built the old way (see
//! `broca_session_convert::load_check`). Each session is copied into memory and checked there, so
//! nothing under `ROOT` is written.
//!
//! The command reads no deployment key. The daemon reads it from the vault (record
//! `opaque:broca-deployment:<id>`) over its connection to the CortexKit module daemon (subc), as
//! the module `broca` that daemon spawned, and the vault grants that read to that module only; a command run from a shell has no
//! such identity, and the key is never written to a file. Plain sessions are checked whole; an
//! encrypted session is listed under `key_unavailable` and never counted as agreeing.
//!
//! `session-convert store-load-check --from-legacy --dir SCRATCH [--every N] [--keep] PATH...`:
//! stages each legacy log under `PATH` (`.wal`, `.ark` or folders of them) as a plain session of
//! the new storage under `SCRATCH`, with the staging writer `move-check` uses and the session's
//! media from today's media store beside the log, then checks it the same way. It reads only the
//! legacy files and writes only under `SCRATCH`, deleting each staged session after its check
//! unless `--keep` is given.
//!
//! The report names counts, fields, seqs and error classes, never an id or a record's content.
//! The command exits 1 when any session differs.

use std::path::Path;

use broca_core::media_store::MediaBytes;
use broca_session_convert::load_check::{self, LegacyReport, Report};
use broca_session_convert::stage::{Stage, StageError};
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_wal::WalRecord;
use futures::executor::block_on;

use super::{each_log, write_report, Args, Item};

/// The deployment key id the daemon uses when its configuration names none. The command never
/// holds the key itself.
const KEY_ID: u16 = 1;

pub(super) fn run(args: &Args) {
    let mut report = Report::default();
    if args.from_legacy {
        legacy(args, &mut report);
    } else {
        stores(args, &mut report);
    }
    write_report(&report, args.out_file.as_deref());
    if !report.passed() {
        std::process::exit(1);
    }
}

/// Checks every session of the stores under `args.roots`.
fn stores(args: &Args, report: &mut Report) {
    let deployment = DeploymentKey::unloaded(KEY_ID);
    let mut index = 0usize;
    for path in &args.roots {
        let roots = match load_check::store_roots(path) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                eprintln!(
                    "{}: no session store (a folder holding heads/)",
                    path.display()
                );
                std::process::exit(2);
            }
            Err(error) => {
                eprintln!("{}: {error}", path.display());
                std::process::exit(2);
            }
        };
        for root in roots {
            let sessions = match load_check::stored_sessions(&root) {
                Ok(sessions) => sessions,
                Err(error) => {
                    eprintln!("{}: {error:?}", root.display());
                    std::process::exit(2);
                }
            };
            let label = root.file_name().map_or_else(
                || root.display().to_string(),
                |name| name.to_string_lossy().into(),
            );
            for addr in sessions {
                index += 1;
                if !(index - 1).is_multiple_of(args.every) {
                    continue;
                }
                let name = format!("{label}/{}", addr.0);
                let check = match load_check::copy_from_root(&root, &addr) {
                    Ok(copy) => block_on(load_check::check(&copy, &addr, &deployment)),
                    Err(error) => load_check::SessionCheck {
                        outcome: load_check::Outcome::Failed(error),
                        ..Default::default()
                    },
                };
                report.add(&name, check);
            }
        }
    }
}

/// Stages every legacy log of `args.files` as a plain session under `args.dir` and checks it.
fn legacy(args: &Args, report: &mut Report) {
    let scratch = args
        .dir
        .clone()
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("store-load-check-{}", std::process::id()))
        })
        .join("store-load-check");
    let store_root = scratch.join("session-store");
    // Plain, so the staged sessions open without any deployment key.
    let deployment = DeploymentKey::unloaded(KEY_ID).creating(Cipher::None);
    let mut legacy = LegacyReport::default();
    let mut index = 0usize;
    each_log(args, |item| {
        let Item::Log { name, old, .. } = item else {
            return;
        };
        index += 1;
        if !(index - 1).is_multiple_of(args.every) {
            return;
        }
        legacy.logs += 1;
        let old = match old {
            Ok(old) => old,
            Err(error) => return not_staged(&mut legacy, "unreadable", &name, &error),
        };
        let Some(session) = old.records.iter().find_map(|frame| match &frame.record {
            WalRecord::RunStarted { session, .. } => Some(session.clone()),
            _ => None,
        }) else {
            return not_staged(&mut legacy, "no_run", &name, "the log starts no run");
        };
        let staging = store_root.join("staging").join(if args.keep {
            format!("{index:06}")
        } else {
            "current".to_owned()
        });
        let staged = (|| {
            if let Some(refusal) =
                broca_session_convert::preconditions::wal_refusal(&old.records, old.lineage_id)
            {
                return Err(StageError::NotMovable(refusal));
            }
            let media = super::move_check::todays_media(&old.source, &old.records);
            let media = media.as_ref().map(|media| &media.store as &dyn MediaBytes);
            let prepared =
                broca_session_convert::stage::prepare(&old.records, old.lineage.as_deref(), media)?;
            match block_on(broca_session_convert::preconditions::converted_refusal(
                &prepared,
            )) {
                Ok(None) => {}
                Ok(Some(refusal)) => return Err(StageError::NotMovable(refusal)),
                Err(error) => return Err(StageError::Failed(error)),
            }
            let _ = std::fs::remove_dir_all(&staging);
            let moved = broca_session_store::record::Moved {
                source_lineage: None,
                move_id: "store-move:store-load-check".into(),
                source_activity_ms: broca_module::head_time::updated_at_of_wal(
                    &old.records,
                    old.written_ms,
                ),
            };
            let stage = Stage {
                source_history_version: super::move_check::source_history_version(
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
                media,
                moved: &moved,
            };
            block_on(broca_session_convert::stage::write(&stage, prepared))
        })();
        match staged {
            Ok(_) => {
                legacy.staged += 1;
                check_staged(report, &staging, &name, &deployment);
            }
            Err(error) => {
                let class = match &error {
                    StageError::NotMovable(reason) => format!("not_movable {reason}"),
                    StageError::MediaMissing(_) => "not_movable media_missing".to_owned(),
                    StageError::Lossy(_) => "conversion_lossy".to_owned(),
                    StageError::Failed(_) => "stage_failed".to_owned(),
                };
                not_staged(&mut legacy, &class, &name, &error.to_string());
            }
        }
        if !args.keep {
            let _ = std::fs::remove_dir_all(&staging);
        }
    });
    if !args.keep {
        let _ = std::fs::remove_dir_all(&scratch);
    }
    report.legacy = Some(legacy);
}

/// Checks every session staged under `staging`: one, the log's.
fn check_staged(report: &mut Report, staging: &Path, name: &str, deployment: &DeploymentKey) {
    match load_check::stored_sessions(staging) {
        Ok(sessions) => {
            for addr in sessions {
                let check = match load_check::copy_from_root(staging, &addr) {
                    Ok(copy) => block_on(load_check::check(&copy, &addr, deployment)),
                    Err(error) => load_check::SessionCheck {
                        outcome: load_check::Outcome::Failed(error),
                        ..Default::default()
                    },
                };
                report.add(name, check);
            }
        }
        Err(error) => report.add(
            name,
            load_check::SessionCheck {
                outcome: load_check::Outcome::Failed(format!("staged_unreadable: {error:?}")),
                ..Default::default()
            },
        ),
    }
}

fn not_staged(legacy: &mut LegacyReport, reason: &str, name: &str, detail: &str) {
    let key: String = reason.chars().take(120).collect();
    *legacy.not_staged.entry(key.clone()).or_default() += 1;
    let examples = legacy.not_staged_examples.entry(key).or_default();
    if examples.len() < 5 {
        examples.push(format!(
            "{name}: {}",
            detail.chars().take(200).collect::<String>()
        ));
    }
}
