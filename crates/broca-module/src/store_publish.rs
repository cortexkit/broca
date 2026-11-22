//! Publishing a session written in a private staging folder into a class root of the session
//! store: its log and objects are renamed into place, then its head is written as the live head,
//! the one point of no return. A move publishes the session it staged where no head exists
//! (`broca-module-serve`'s `store_move`); an import publishes the session it staged over the
//! empty session it created (`store_import_stage`).
//!
//! The staging folder is on the state root's filesystem, so each rename is atomic, and outside
//! every class root, so nothing serves or backs up a half-staged session.

use std::path::Path;

use broca_records::move_fence;
use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend as _, Version};

use crate::registry::store_move::MoveRefusal;

mod directories;
pub(crate) use directories::PublicationDirs;

fn unavailable(what: &str, error: impl std::fmt::Display) -> MoveRefusal {
    MoveRefusal::new("unavailable", format!("{what}: {error}"))
}

fn io_failed(what: &'static str) -> impl Fn(std::io::Error) -> MoveRefusal {
    move |error| unavailable(what, error)
}

/// Renames the staged log and objects into the destination root, then syncs `logs/`,
/// `objects/` and their parent. A missing staged folder (a session with no objects) is
/// skipped. `step` is told each boundary as it is passed, and stops the publication when it
/// errs.
///
/// Where the destination has no folder for the session, the staged folder is renamed whole.
/// Repeating a rename after a crash accepts only the already-published directory, never a
/// second copy. Where it has one (an import replacing the empty session it created), each
/// staged entry is renamed into it under its own name, which the session there doesn't use: a
/// staged log has a new id, and a staged object's folder is named by it. Each renamed entry and
/// both folders are synced.
pub fn publish_data_at<E: From<MoveRefusal>>(
    state_root: &Path,
    staging: &Path,
    store: &Path,
    address: &str,
    directories: &mut PublicationDirs,
    mut step: impl FnMut(&'static str) -> Result<(), E>,
) -> Result<(), E> {
    directories
        .ensure(state_root, store)
        .map_err(io_failed("the class store couldn't be initialized"))?;
    let live_logs = store.join("logs").join(address);
    let live_objects = store.join("objects").join(address);
    for (folder, live, boundary) in [
        ("logs", &live_logs, "rename_logs"),
        ("objects", &live_objects, "rename_objects"),
    ] {
        let staged = staging.join(folder).join(address);
        if !staged
            .try_exists()
            .map_err(io_failed("the staged session couldn't be checked"))?
        {
            if folder == "logs"
                && !live
                    .try_exists()
                    .map_err(io_failed("the published log couldn't be checked"))?
            {
                return Err(unavailable(
                    "the verified session log",
                    "absent from staging and destination",
                )
                .into());
            }
            continue;
        }
        if live
            .try_exists()
            .map_err(io_failed("the live session couldn't be checked"))?
        {
            install_entries(&staged, live, || step(boundary))?;
            continue;
        }
        std::fs::rename(&staged, live)
            .map_err(io_failed("the staged session couldn't be published"))?;
        // The online move's fault boundary remains immediately after the rename.
        // Startup conversion (`session_conversion`) passes the same boundary before it
        // publishes the head.
        step(boundary)?;
    }
    for folder in ["logs", "objects"] {
        // A visible destination on restart is not proof that its rename was synced. Replay
        // both changed-parent barriers even when the session directory already left staging.
        if store
            .join(folder)
            .join(address)
            .try_exists()
            .map_err(io_failed("the published data couldn't be checked"))?
        {
            for (parent, boundary) in [
                (
                    staging.join(folder),
                    if folder == "logs" {
                        "sync_logs_source_parent"
                    } else {
                        "sync_objects_source_parent"
                    },
                ),
                (
                    store.join(folder),
                    if folder == "logs" {
                        "sync_logs_destination_parent"
                    } else {
                        "sync_objects_destination_parent"
                    },
                ),
            ] {
                move_fence::sync_dir(&parent)
                    .map_err(io_failed("a data rename's parent couldn't be synced"))?;
                step(boundary)?;
            }
        }
    }
    step("sync_data")?;
    Ok(())
}

/// Renames every entry of `staged` into `live`, which exists, telling `renamed` after each.
/// An entry whose name `live` already holds is refused, never replaced. Each renamed entry is
/// synced, then both folders, and the emptied staged folder is removed.
fn install_entries<E: From<MoveRefusal>>(
    staged: &Path,
    live: &Path,
    mut renamed: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let mut entries = std::fs::read_dir(staged)
        .map_err(io_failed("the staged session couldn't be listed"))?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(io_failed("the staged session couldn't be listed"))?;
    entries.sort();
    for name in entries {
        let to = live.join(&name);
        match std::fs::symlink_metadata(&to) {
            Ok(_) => {
                return Err(unavailable(
                    "the staged session",
                    format!("{} is already in the live session", to.display()),
                )
                .into())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(unavailable("the live session couldn't be checked", error).into())
            }
        }
        std::fs::rename(staged.join(&name), &to)
            .map_err(io_failed("the staged session couldn't be published"))?;
        if to.is_dir() {
            move_fence::sync_dir(&to)
        } else {
            broca_session_store::durable::sync_file(&to)
        }
        .map_err(io_failed("a published entry couldn't be synced"))?;
        renamed()?;
    }
    move_fence::sync_dir(live)
        .map_err(io_failed("the live session's folder couldn't be synced"))?;
    move_fence::sync_dir(staged)
        .map_err(io_failed("the staged session's folder couldn't be synced"))?;
    std::fs::remove_dir(staged)
        .map_err(io_failed("the emptied staged folder couldn't be removed"))?;
    Ok(())
}

/// Writes the staged head as the live head, through the backend's versioned head write, then
/// syncs `heads/` and reads the head back. Returns the version written.
///
/// With `expected` `None` there must be no live head: the staged head is written at its own
/// version. With `expected` `Some`, the live head must be at that version, and the staged head
/// replaces it at the next one, as any new head version of the session is written. A live head
/// already holding exactly what would be written is accepted, so a repeat after a crash
/// changes nothing.
pub async fn publish_head_at<E: From<MoveRefusal>>(
    staging: &Path,
    store: &Path,
    address: &str,
    expected: Option<Version>,
) -> Result<Version, E> {
    let addr = SessionAddr(address.to_owned());
    crate::store_open::recovery::register_candidate(store, &addr).map_err(io_failed(
        "the installed session's recovery couldn't be registered",
    ))?;
    let staged = LocalBackend::new(staging);
    let (bytes, staged_version) = staged
        .read_head(&addr)
        .await
        .map_err(|error| unavailable("the staged head couldn't be read", format!("{error:?}")))?
        .ok_or_else(|| unavailable("the staged head", "absent"))?;
    let live = LocalBackend::new(store);
    let existing = live
        .read_head(&addr)
        .await
        .map_err(|error| unavailable("the live head couldn't be read", format!("{error:?}")))?;
    let version = expected.map_or(staged_version, |at| at + 1);
    match (existing, expected) {
        (None, None) => {
            live.write_head(&addr, &bytes, None, version)
                .await
                .map_err(|error| {
                    unavailable("the head couldn't be written", format!("{error:?}"))
                })?;
        }
        (Some((_, at)), Some(expected)) if at == expected => {
            live.write_head(&addr, &bytes, Some(expected), version)
                .await
                .map_err(|error| {
                    unavailable("the head couldn't be written", format!("{error:?}"))
                })?;
        }
        (Some(existing), _) if existing == (bytes.clone(), version) => {}
        (Some(_), None) => {
            return Err(
                unavailable("the live head", "differs from the verified staged head").into(),
            )
        }
        (_, Some(_)) => {
            return Err(unavailable(
                "the live head",
                "isn't the version the staged session replaces",
            )
            .into())
        }
    }
    move_fence::sync_dir(&store.join("heads"))
        .map_err(io_failed("the new storage's heads couldn't be synced"))?;
    let read = live
        .read_head(&addr)
        .await
        .map_err(|error| unavailable("the head couldn't be read back", format!("{error:?}")))?;
    if read != Some((bytes, version)) {
        return Err(unavailable("the head", "it reads back other than it was written").into());
    }
    Ok(version)
}
