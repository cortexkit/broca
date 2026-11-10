//! Writes one session from today's storage as a complete, normal session of the new storage, in
//! a private folder: its head, its log and its objects, under the session's real address and
//! bind triple, sealed with the deployment's key. Nothing reads that folder, so the session there
//! is invisible until a move publishes it (see `broca-module-serve`'s `store_move`).
//!
//! The setup parts the session's runs name go to the new storage's shared setup store instead.
//! They are write-once and keyed by their content, so one no published session names is
//! harmless.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::{BindIdentity, MediaRef, ToolOutput, ToolOutputPart};
use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Moved, Record};
use broca_session_store::session::{NewInputMedia, NewMedia, Session};
use broca_session_store::setup::SetupStore;
use broca_wal::FramedRecord;
use serde_json::Value;

use crate::Converted;

/// What to stage.
pub struct Stage<'a> {
    /// The WAL prefix to copy: every record of the session up to its frozen end.
    pub records: &'a [FramedRecord],
    /// The WAL's lineage, which the source message ids and call keys are made of. Offline
    /// conversion must publish through `stage` too, so its move records this reference namespace.
    pub lineage: Option<&'a str>,
    /// The version `session.read` serves for the source session. Publishing conversions obtain it through
    /// `ServedHistory::served_snapshot`, including allocation when no row exists. The read-only
    /// move-check rehearsal uses an existing row, otherwise the registry's no-source fallback
    /// (`u64::from(has_log)`), which can differ from a row allocation's wall-clock floor.
    pub source_history_version: u64,
    pub session: &'a BindIdentity,
    pub deployment: &'a DeploymentKey,
    /// The new storage's root (`<state root>/session-store`), whose setup store takes the
    /// session's setup parts.
    pub store_root: &'a Path,
    /// The private folder the session is written into, as a backend root of its own.
    pub staging: &'a Path,
    /// Today's media store for the session, which the bytes of every medium it names are read
    /// from; `None` where there is none, which refuses a session naming any medium.
    pub media: Option<&'a dyn MediaBytes>,
    /// The record of the move, written in the batch that holds the last converted record: its
    /// id, and the last update today's `session.head` serves for the session
    /// (`broca_module::head_time`), which the moved session's head goes on serving.
    pub moved: &'a Moved,
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
    /// Today's media store can't give the bytes of a medium the session names, so the copy
    /// couldn't hold them: `not_movable {media_missing}`.
    MediaMissing(MediaMissing),
    /// The conversion dropped or couldn't read part of the WAL, so the copy wouldn't be exact.
    Lossy(String),
    /// Writing the staged session failed.
    Failed(String),
}

/// The media of a session that today's media store can't give back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaMissing {
    /// The first such medium's digest, in digest order.
    pub digest: String,
    /// Whether that medium's object is `absent` or `corrupt` (its bytes don't match its digest).
    pub state: &'static str,
    /// How many of the session's media can't be read.
    pub missing: usize,
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotMovable(reason) => write!(f, "not_movable {{{reason}}}"),
            Self::MediaMissing(missing) => write!(
                f,
                "not_movable {{media_missing}}: {} of the session's media can't be read, the \
                 first {} ({})",
                missing.missing, missing.digest, missing.state
            ),
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
    /// The bytes of every medium the session names, by digest, read from today's media store
    /// and checked against their references.
    pub media: BTreeMap<String, Vec<u8>>,
}

/// Where a converted record names a medium the new storage keeps an object for: a tool result's
/// media part, or a medium in a message an input, a queued send or a steer holds.
enum MediumAt {
    /// Part `part` of the `ToolReply`'s result.
    Reply { part: usize },
    /// The medium at content block `block` (and part `part` of a tool result there) of message
    /// `message` the record holds; an `Input` holds one message.
    Message {
        message: u32,
        block: u32,
        part: Option<u32>,
    },
}

/// Every medium `record` names where the new storage keeps an object for it.
fn media_at(record: &Record) -> Vec<(MediumAt, &MediaRef)> {
    let mut found = Vec::new();
    let messages: Vec<&broca_protocol::content::Message> = match record {
        Record::ToolReply(reply) => {
            if let ToolOutput::Parts { parts } = &reply.result.output {
                for (part, block) in parts.iter().enumerate() {
                    if let ToolOutputPart::Media { media } = block {
                        found.push((MediumAt::Reply { part }, media));
                    }
                }
            }
            return found;
        }
        Record::Input(input) => vec![&input.message],
        other => match broca_session_store::session::sent_messages(other) {
            Some((messages, _)) => messages.iter().collect(),
            None => return found,
        },
    };
    for (message, content) in (0u32..).zip(messages) {
        for (block, part, media) in broca_session_store::session::message_media(content) {
            found.push((
                MediumAt::Message {
                    message,
                    block,
                    part,
                },
                media,
            ));
        }
    }
    found
}

/// The bytes of every medium the converted `records` name, read from `media`, today's media
/// store for the session (`None` where there is none to read). Every medium today's `wal`
/// names must be one the new storage keeps an object for, and every one must read back whole:
/// a move reproduces the session exactly, so a medium it can't copy refuses the move.
pub(crate) fn read_media(
    wal: &[FramedRecord],
    records: &[Record],
    media: Option<&dyn MediaBytes>,
) -> Result<BTreeMap<String, Vec<u8>>, StageError> {
    let mut references: BTreeMap<String, &MediaRef> = BTreeMap::new();
    for record in records {
        for (_, reference) in media_at(record) {
            references
                .entry(reference.sha256.clone())
                .or_insert(reference);
        }
    }
    if let Some(digest) = media_digests(wal)
        .into_iter()
        .find(|digest| !references.contains_key(digest))
    {
        return Err(StageError::Lossy(format!(
            "the medium {digest} is named where the new storage keeps no object for it"
        )));
    }
    let mut bytes = BTreeMap::new();
    let mut missing: Vec<(&str, &'static str)> = Vec::new();
    for (digest, reference) in &references {
        let Some(store) = media else {
            missing.push((digest, "absent"));
            continue;
        };
        match store.read_verified(reference) {
            Ok(read) => {
                bytes.insert(digest.clone(), read);
            }
            // No store to read it from, as when there is none at all.
            Err(MediaReadError::Missing(_) | MediaReadError::StoreMissing { .. }) => {
                missing.push((digest, "absent"))
            }
            Err(MediaReadError::Corrupt(_)) => missing.push((digest, "corrupt")),
            Err(MediaReadError::Io { digest, source }) => {
                return Err(StageError::Failed(format!(
                    "today's media store couldn't read {digest}: {source}"
                )))
            }
        }
    }
    if let Some((digest, state)) = missing.first() {
        return Err(StageError::MediaMissing(MediaMissing {
            digest: (*digest).to_owned(),
            state,
            missing: missing.len(),
        }));
    }
    Ok(bytes)
}

/// Converts the WAL `records`, refusing what the move can't carry exactly, and reads the bytes of
/// every medium the session names from `media`, today's media store for the session. Writes
/// nothing.
pub fn prepare(
    records: &[FramedRecord],
    lineage: Option<&str>,
    media: Option<&dyn MediaBytes>,
) -> Result<Prepared, StageError> {
    let converted = crate::convert(records, lineage).map_err(|error| StageError::Lossy(error.0))?;
    if !converted.lost.is_empty() {
        let lost: Vec<&str> = converted.lost.keys().map(String::as_str).collect();
        return Err(StageError::Lossy(format!("lost {}", lost.join(", "))));
    }
    let media = read_media(records, &converted.records, media)?;
    Ok(Prepared {
        converted,
        appends_in_conversation: !broca_core::is_mode3_log(
            records.iter().map(|frame| &frame.record),
        ),
        media,
    })
}

/// Prepares and writes the session `stage` names (see [`prepare`] and [`write`]).
pub async fn stage(stage: Stage<'_>) -> Result<Staged, StageError> {
    let prepared = prepare(stage.records, stage.lineage, stage.media)?;
    write(&stage, prepared).await
}

/// One append of a staged batch: records holding only tool results' media, or only messages'
/// media, since the new storage stores each kind through its own writer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Holds {
    Nothing,
    Replies,
    Messages,
}

fn holds(record: &Record) -> Holds {
    match media_at(record).first() {
        None => Holds::Nothing,
        Some((MediumAt::Reply { .. }, _)) => Holds::Replies,
        Some((MediumAt::Message { .. }, _)) => Holds::Messages,
    }
}

/// Appends `records` to `session` as one synced batch, storing the bytes `media` holds for each
/// medium they name. Every record names media of one kind (see [`Holds`]).
async fn append_with_media<B: SessionBackend>(
    session: &mut Session<B>,
    records: &[Record],
    media: &BTreeMap<String, Vec<u8>>,
) -> Result<(), StageError> {
    let failed = |error: broca_session_store::session::SessionError| {
        StageError::Failed(format!("append to the staged session: {error:?}"))
    };
    let bytes = |reference: &MediaRef| {
        media.get(&reference.sha256).cloned().ok_or_else(|| {
            StageError::Failed(format!(
                "the medium {} wasn't read before staging",
                reference.sha256
            ))
        })
    };
    let (mut replies, mut messages) = (Vec::new(), Vec::new());
    for (record, held) in records.iter().enumerate() {
        for (at, reference) in media_at(held) {
            match at {
                MediumAt::Reply { part } => replies.push(NewMedia {
                    record,
                    part,
                    bytes: bytes(reference)?,
                }),
                MediumAt::Message {
                    message,
                    block,
                    part,
                } => messages.push(NewInputMedia {
                    record,
                    message,
                    block,
                    part,
                    bytes: bytes(reference)?,
                }),
            }
        }
    }
    if !messages.is_empty() {
        session
            .append_input_media(records, messages, true)
            .await
            .map_err(failed)?;
    } else {
        session
            .append_media(records, replies, true)
            .await
            .map_err(failed)?;
    }
    Ok(())
}

/// Appends `batch` to `session`, storing the bytes `media` holds for each medium it names. A
/// batch holding both tool results' media and messages' media is appended in parts, each holding
/// one kind, as the run loop appends them.
pub(crate) async fn append_batch<B: SessionBackend>(
    session: &mut Session<B>,
    batch: &[Record],
    media: &BTreeMap<String, Vec<u8>>,
) -> Result<(), StageError> {
    let mut from = 0;
    let mut kind = Holds::Nothing;
    for (at, record) in batch.iter().enumerate() {
        let this = holds(record);
        if this != Holds::Nothing && kind != Holds::Nothing && this != kind {
            append_with_media(session, &batch[from..at], media).await?;
            from = at;
        }
        if this != Holds::Nothing {
            kind = this;
        }
    }
    append_with_media(session, &batch[from..], media).await
}

/// Writes the prepared session `stage` names into its staging folder. The folder must not hold
/// the session yet: the head is created there, as for any new session.
pub async fn write(stage: &Stage<'_>, prepared: Prepared) -> Result<Staged, StageError> {
    let Prepared {
        mut converted,
        appends_in_conversation,
        media,
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
    let mut session = Session::create_with_history_version(
        LocalBackend::new(stage.staging),
        SessionAddr(broca_core::session_addr(stage.session)),
        triple(stage.session),
        stage.deployment,
        appends_in_conversation,
        stage.source_history_version,
    )
    .await
    .map_err(failed("create the staged session"))?;
    converted.start_at(session.next_seq());
    // The records are appended as the run loop appends them: each group up to a barrier as one
    // synced batch, after the setup parts it names are stored. The move's own record goes in
    // the last batch.
    let records = &converted.records;
    let mut moved = stage.moved.clone();
    moved.source_lineage = stage.lineage.map(str::to_owned);
    let moved = Record::Moved(moved);
    if records.is_empty() {
        append_batch(&mut session, std::slice::from_ref(&moved), &media).await?;
    }
    let mut start = 0;
    for (index, record) in records.iter().enumerate() {
        let last = index + 1 == records.len();
        if !(crate::measure::barrier_after(record, records.get(index + 1)) || last) {
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
        if last {
            let mut batch = records[start..=index].to_vec();
            batch.push(moved.clone());
            append_batch(&mut session, &batch, &media).await?;
        } else {
            append_batch(&mut session, &records[start..=index], &media).await?;
        }
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
        media: media.len() as u64,
    })
}
