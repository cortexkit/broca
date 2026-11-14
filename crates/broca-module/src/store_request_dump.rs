//! `session.request_dump` for a session kept in the new storage: rebuild the request each step
//! of one run sent, and write the bodies that rebuild to the digest the step recorded.
//!
//! Each step is rebuilt as a warm-up rebuilds a recorded request
//! (`store_actor::recorded_warm::request`), from the session's records read once, and rendered by the wire family the run
//! froze, as a fork renders its base (`fork_answer::render`). Only a body whose SHA-256 equals
//! the step's last attempt's recorded digest is written, so a written file is the request the
//! provider received. A request carrying media isn't rebuilt: rendering it needs the media
//! delivery only the session's actor holds while a run runs.
//!
//! The read runs over a read-only view of the session as stored (`StoreRuntime::current_view`),
//! taking no lease and commanding no actor. The bodies go to `request-dumps/<session address>/
//! <run id>/` under the state root, in folders and files only the owner can read.

use std::path::{Path, PathBuf};

use broca_protocol::BindIdentity;
use broca_session_store::record::Record;
use broca_wire::request_dump::{RequestDumpResponse, StepDump, StepDumpOutcome};

use crate::serve::ServeError;
use crate::store_open::StoreRuntime;

/// The folder under the state root that holds every dump.
pub const DUMP_FOLDER: &str = "request-dumps";

/// Rebuild and write the requests of run `run_id` of the session `key`. `Ok(None)` when the new
/// storage doesn't keep the session.
pub async fn serve(
    runtime: &StoreRuntime,
    key: &BindIdentity,
    run_id: &str,
) -> Result<Option<RequestDumpResponse>, ServeError> {
    let unreadable = |what: &str, error: String| ServeError::Transient(format!("{what}: {error}"));
    let Some(class) = runtime
        .class_of(key)
        .map_err(|error| unreadable("find the session", format!("{error:?}")))?
    else {
        return Ok(None);
    };
    let Some(view) = runtime
        .current_view(key)
        .await
        .map_err(|error| unreadable("read the session", format!("{error:?}")))?
    else {
        return Ok(None);
    };
    let readers = runtime.class_backend(class);
    let parts = runtime.parts_for(class, view.head().cipher());
    // A step is rebuilt as the session stood then, from every record before it (see
    // `recorded_warm::request_as_of`), so the whole history is read, sealed segments included.
    let records = view
        .stored_records(&readers)
        .records_between(1, view.next_seq().saturating_sub(1))
        .await
        .map_err(|error| unreadable("read the session's history", format!("{error:?}")))?;
    if !starts(&records, run_id) {
        return Err(ServeError::UnknownRun(run_id.to_owned()));
    }
    let steps = attempted_steps(&records, run_id);
    if steps.is_empty() {
        return Err(ServeError::UnknownRun(run_id.to_owned()));
    }
    let folder = runtime
        .state_root()
        .join(DUMP_FOLDER)
        .join(broca_core::session_addr(key))
        .join(file_safe(run_id));
    broca_owner_only::create_dir_all(&folder)
        .map_err(|error| unreadable("create the dump folder", error.to_string()))?;
    let mut dumps = Vec::with_capacity(steps.len());
    for (step, recorded_sha256) in steps {
        dumps.push(
            dump_step(
                &view,
                &records,
                parts,
                key,
                run_id,
                step,
                recorded_sha256,
                &folder,
            )
            .await,
        );
    }
    Ok(Some(RequestDumpResponse {
        run_id: run_id.to_owned(),
        folder: folder.display().to_string(),
        steps: dumps,
    }))
}

/// Rebuild step `step` of run `run_id` from `records`, the session's whole history, as the session
/// stood when the step sent, and write it when it renders to `recorded_sha256`.
#[allow(clippy::too_many_arguments)]
async fn dump_step(
    view: &crate::store_read::StoredView<broca_session_store::backend::LocalBackend>,
    records: &[(u64, Record)],
    parts: &dyn broca_session_loop::parts::Parts,
    key: &BindIdentity,
    run_id: &str,
    step: u64,
    recorded_sha256: String,
    folder: &Path,
) -> StepDump {
    let not_rebuilt = |detail: String| StepDump {
        step,
        recorded_sha256: recorded_sha256.clone(),
        outcome: StepDumpOutcome::NotRebuilt,
        bytes: None,
        detail: Some(detail),
    };
    let request = match crate::store_actor::recorded_warm::request_as_of(
        // The session itself, under its stored view: the rebuild takes the conversation from it,
        // and everything else from `records` as of the step.
        &***view, records, parts, key, run_id, step,
    )
    .await
    {
        Ok(request) => request,
        Err(error) => return not_rebuilt(error),
    };
    if request
        .opts
        .prompt
        .iter()
        .any(|message| !broca_session_store::session::message_media(message).is_empty())
    {
        return not_rebuilt("the request carries media, which an export doesn't render".into());
    }
    let body = match crate::fork_answer::render(&request.config, &request.opts) {
        Ok(body) => body,
        Err(error) => return not_rebuilt(error),
    };
    let digest = broca_core::render::request_digest_hex(&body);
    if digest != request.request_body_sha256 {
        return StepDump {
            step,
            recorded_sha256,
            outcome: StepDumpOutcome::DigestMismatch,
            bytes: None,
            detail: Some(format!("rebuilt to {digest}")),
        };
    }
    match write_private(&folder.join(format!("s{step}.json")), &body) {
        Ok(()) => StepDump {
            step,
            recorded_sha256,
            outcome: StepDumpOutcome::Written,
            bytes: Some(body.len() as u64),
            detail: None,
        },
        Err(error) => not_rebuilt(format!("write the body: {error}")),
    }
}

/// Whether `records` hold the start of run `run_id`.
fn starts(records: &[(u64, Record)], run_id: &str) -> bool {
    records
        .iter()
        .any(|(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id))
}

/// Each step run `run_id` attempted, in step order, with the request digest its last attempt
/// recorded.
fn attempted_steps(records: &[(u64, Record)], run_id: &str) -> Vec<(u64, String)> {
    let mut steps = std::collections::BTreeMap::new();
    for (_, record) in records {
        if let Record::Attempt(attempt) = record {
            if attempt.run_id == run_id {
                steps.insert(attempt.step_id, hex(&attempt.request_body_sha256.0));
            }
        }
    }
    steps.into_iter().collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `name` with every character outside `[A-Za-z0-9_-]` replaced, so a run id names exactly one
/// folder inside the session's: no separator, and no `.` to make `..`.
fn file_safe(name: &str) -> PathBuf {
    PathBuf::from(
        name.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>(),
    )
}

fn write_private(path: &Path, body: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(body)?;
    file.sync_all()
}
