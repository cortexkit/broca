//! Writes one session from today's storage as a complete, normal session of the new storage, in
//! a private folder: its head, its log and its objects, under the session's real address and
//! bind triple, sealed with the deployment's key. Nothing reads that folder, so the session there
//! is invisible until a move publishes it (see `broca-module-serve`'s `store_move`).
//!
//! The setup parts the session's runs name go to the new storage's shared setup store instead.
//! They are write-once and keyed by their content, so one no published session names is
//! harmless.

use std::collections::BTreeSet;
use std::path::Path;

use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_session_store::setup::SetupStore;
use broca_wal::FramedRecord;
use serde_json::Value;

use crate::Converted;

/// What to stage.
pub struct Stage<'a> {
    /// The WAL prefix to copy: every record of the session up to its frozen end.
    pub records: &'a [FramedRecord],
    /// The WAL's lineage, which today's message ids and call keys are made of.
    pub lineage: Option<&'a str>,
    pub session: &'a BindIdentity,
    pub deployment: &'a DeploymentKey,
    /// The new storage's root (`<state root>/session-store`), whose setup store takes the
    /// session's setup parts.
    pub store_root: &'a Path,
    /// The private folder the session is written into, as a backend root of its own.
    pub staging: &'a Path,
}

/// A session written into its staging folder.
#[derive(Debug)]
pub struct Staged {
    /// The conversation layout the session was written with: true off the current compaction
    /// contract, which appends text delivered with a tool result to the result, and false on it,
    /// as the source WAL's own contract says.
    pub appends_in_conversation: bool,
    /// The converted records and setup parts, as written.
    pub converted: Converted,
    /// The runs the session started.
    pub runs: u64,
    /// The messages of its transcript.
    pub messages: u64,
    /// The media objects it stores.
    pub media: u64,
}

/// Why a session wasn't staged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageError {
    /// The session holds something the move can't carry yet, named by the reason.
    NotMovable(&'static str),
    /// The conversion dropped or couldn't read part of the WAL, so the copy wouldn't be exact.
    Lossy(String),
    /// Writing the staged session failed.
    Failed(String),
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMovable(reason) => write!(f, "not_movable {{{reason}}}"),
            Self::Lossy(lost) => write!(f, "the conversion is not exact: {lost}"),
            Self::Failed(reason) => f.write_str(reason),
        }
    }
}

/// The session's bind triple as the new storage records it in the head.
pub fn triple(session: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: session.project_root.to_string_lossy().into_owned(),
        harness: session.harness.clone(),
        session: session.session.clone(),
    }
}

/// Whether the session at `records` holds media its move can't carry yet. The new storage can
/// store them (`Session::append_media` for a tool result's, `Session::append_input_media` for a
/// message's), but its run loop has no request renderer that reads a session's own media
/// objects: its executor refuses a run whose provider renders images while the session holds
/// media. A moved session's next request then couldn't be the one today's renderer builds, so
/// every session that names any medium is refused, by this function alone, until that renderer
/// exists.
pub fn media_refusal(records: &[FramedRecord]) -> Option<&'static str> {
    (!media_digests(records).is_empty()).then_some("media")
}

/// The digests of every medium the WAL's records name: a message's media block, or a tool
/// result's media part, wherever a record carries one.
pub fn media_digests(records: &[FramedRecord]) -> BTreeSet<String> {
    fn walk(value: &Value, out: &mut BTreeSet<String>) {
        match value {
            Value::Object(object) => {
                // A message's media block (`ContentBlock::Media`).
                if object.get("type").and_then(Value::as_str) == Some("media") {
                    if let Some(digest) = object.get("sha256").and_then(Value::as_str) {
                        out.insert(digest.to_owned());
                    }
                }
                // A tool result's media part (`ToolOutputPart::Media`).
                if object.get("kind").and_then(Value::as_str) == Some("media") {
                    if let Some(digest) = object
                        .get("media")
                        .and_then(|media| media.get("sha256"))
                        .and_then(Value::as_str)
                    {
                        out.insert(digest.to_owned());
                    }
                }
                object.values().for_each(|member| walk(member, out));
            }
            Value::Array(items) => items.iter().for_each(|item| walk(item, out)),
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    for frame in records {
        if let Ok(value) = serde_json::to_value(&frame.record) {
            walk(&value, &mut out);
        }
    }
    out
}

/// A session converted and ready to write: nothing is written yet.
#[derive(Debug)]
pub struct Prepared {
    pub converted: Converted,
    /// See [`Staged::appends_in_conversation`].
    pub appends_in_conversation: bool,
}

/// Converts the WAL `records`, refusing what the move can't carry exactly. Writes nothing.
pub fn prepare(records: &[FramedRecord], lineage: Option<&str>) -> Result<Prepared, StageError> {
    if let Some(reason) = media_refusal(records) {
        return Err(StageError::NotMovable(reason));
    }
    let converted = crate::convert(records, lineage).map_err(|error| StageError::Lossy(error.0))?;
    if !converted.lost.is_empty() {
        let lost: Vec<&str> = converted.lost.keys().map(String::as_str).collect();
        return Err(StageError::Lossy(format!("lost {}", lost.join(", "))));
    }
    Ok(Prepared {
        converted,
        appends_in_conversation: !broca_core::is_mode3_log(
            records.iter().map(|frame| &frame.record),
        ),
    })
}

/// Prepares and writes the session `stage` names (see [`prepare`] and [`write`]).
pub async fn stage(stage: Stage<'_>) -> Result<Staged, StageError> {
    let prepared = prepare(stage.records, stage.lineage)?;
    write(&stage, prepared).await
}

/// Writes the prepared session `stage` names into its staging folder. The folder must not hold
/// the session yet: the head is created there, as for any new session.
pub async fn write(stage: &Stage<'_>, prepared: Prepared) -> Result<Staged, StageError> {
    let Prepared {
        converted,
        appends_in_conversation,
    } = prepared;
    let failed = |what: &str| {
        let what = what.to_owned();
        move |error: broca_session_store::session::SessionError| {
            StageError::Failed(format!("{what}: {error:?}"))
        }
    };
    let setup_backend = LocalBackend::new(stage.store_root);
    // The setup parts go to the name space of the mode the session is created in.
    let setup_key = stage
        .deployment
        .setup_key_for(stage.deployment.new_sessions())
        .ok_or_else(|| {
            StageError::Failed(
                "an encrypted session can't be staged before the deployment key has loaded".into(),
            )
        })?;
    let setup = SetupStore::new(&setup_backend, &setup_key);
    let mut session = Session::create(
        LocalBackend::new(stage.staging),
        SessionAddr(broca_core::session_addr(stage.session)),
        triple(stage.session),
        stage.deployment,
        appends_in_conversation,
    )
    .await
    .map_err(failed("create the staged session"))?;
    // The records are appended as the run loop appends them: each group up to a barrier as one
    // synced batch, after the setup parts it names are stored.
    let records = &converted.records;
    let mut start = 0;
    for (index, record) in records.iter().enumerate() {
        if !(crate::measure::barrier_after(record, records.get(index + 1))
            || index + 1 == records.len())
        {
            continue;
        }
        for part in records[start..=index]
            .iter()
            .flat_map(crate::measure::named_parts)
        {
            let bytes = converted.parts.get(&part.digest).ok_or_else(|| {
                StageError::Failed("a record names a setup part the conversion didn't keep".into())
            })?;
            let (stored, _) = setup
                .put(part.kind, bytes)
                .await
                .map_err(|error| StageError::Failed(format!("store a setup part: {error:?}")))?;
            if stored != *part {
                return Err(StageError::Failed(
                    "a stored setup part's reference differs from the record's".into(),
                ));
            }
        }
        session
            .append(&records[start..=index], true)
            .await
            .map_err(failed("append to the staged session"))?;
        start = index + 1;
    }
    let messages = broca_module::store_read::transcript_len(&session);
    session
        .close()
        .await
        .map_err(failed("close the staged session"))?;
    Ok(Staged {
        appends_in_conversation,
        runs: converted.runs,
        converted,
        messages,
        media: 0,
    })
}
