//! WAL-backed raw pages. SQL supplies coordinates, never message or title content.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};

use broca_core::projection_plan::{OffsetSeed, OffsetSource};
use broca_core::replay::{PositionedFrame, SeedFallback};
use broca_core::{SessionReadPage, StorePageSnapshot};
use broca_protocol::{BindIdentity, Message, Role, RunId};
use broca_wal::access_counter;
use broca_wal::file_wal::DurablePublication;
use broca_wal::framing::DecodeStep;
use broca_wal::{FramedRecord, WalRecord};
use rusqlite::{params, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use super::{index, DbError, LogLocation, OverhaulDb};

type ReadAt<'a> = dyn Fn(u64, u64) -> std::io::Result<Vec<u8>> + 'a;

#[derive(Debug, Clone, Copy, Default)]
pub struct RawPageRequest {
    pub from_ordinal: Option<u64>,
    pub limit: Option<u64>,
    pub include_tools: bool,
}

pub struct RawPageSnapshot {
    /// A full replay may answer a page without attesting the SQL tag coordinates.
    pub index_validated: bool,
    pub page: SessionReadPage,
    pub at_seq: u64,
    pub head: broca_core::head::HeadProjection,
    pub head_media: Option<RawHeadMedia>,
    pub last_title: Option<String>,
    pub last_write_ms: Option<u64>,
    pub retention_activity_ms: Option<u64>,
    pub tools: Option<(RunId, std::sync::Arc<Vec<broca_protocol::request::Tool>>)>,
    pub dispatches: BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
    pub native_tool_outputs: broca_core::display_facts::NativeToolOutputs,
}

/// Ephemeral media metadata from the raw page's authenticated log snapshot.
/// Media profiles are read from log records, never persisted in the database.
#[derive(Debug, Clone)]
pub struct RawHeadMedia {
    pub run_id: broca_protocol::RunId,
    pub media: Option<serde_json::Value>,
    pub interrupt_reason: Option<String>,
}

struct Session {
    seq: u64,
    off: u64,
    end: u64,
    identity: Vec<u8>,
    lineage: Option<String>,
    count: u64,
    locator: String,
    admission: String,
    projection: String,
    title: Option<u64>,
}

impl OverhaulDb {
    /// Content-free run discovery. Terminal metadata is hydrated from the WAL, not SQL.
    pub fn lookup_run(&self, run_id: &str) -> Result<Option<broca_core::RunIndexRow>, DbError> {
        self.read_snapshot(|tx| {
            let row = tx
                .query_row(
                    "SELECT s.project_root,s.harness,s.session_key,r.submission_id,
                        r.request_identity_digest,r.state
                 FROM run r JOIN session s ON s.sid=r.sid WHERE r.run_id=?1",
                    [run_id],
                    |r| {
                        Ok((
                            BindIdentity::new(
                                r.get::<_, String>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                            ),
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, Option<String>>(4)?,
                            r.get::<_, String>(5)?,
                        ))
                    },
                )
                .optional()?;
            row.map(|(session, submission_id, request_identity_digest, state)| {
                Ok(broca_core::RunIndexRow {
                    run_id: run_id.into(),
                    session,
                    submission_id,
                    request_identity_digest,
                    state: broca_core::RunIndexState::parse(&state)
                        .ok_or_else(|| invalid(format!("unknown run state: {state}")))?,
                    usage: None,
                    final_step_finish_reason: None,
                    retries_used: 0,
                    title: None,
                    withdrawals_owed: super::recovery::owed(tx, run_id)?,
                })
            })
            .transpose()
        })
    }

    /// Advisory orphan candidates; the caller must reconcile each against its durable WAL.
    pub fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, DbError> {
        self.run_candidates(false)
    }

    /// Include active rows because a durable restart pause may precede its index update.
    pub fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, DbError> {
        self.run_candidates(true)
    }

    fn run_candidates(&self, include_paused: bool) -> Result<Vec<(String, BindIdentity)>, DbError> {
        self.read_snapshot(|tx| {
            let mut stmt = tx.prepare(
                "SELECT r.run_id,s.project_root,s.harness,s.session_key
                 FROM run r JOIN session s ON s.sid=r.sid
                 WHERE r.state='active' OR (?1 AND r.state='paused') ORDER BY r.run_id",
            )?;
            let rows = stmt
                .query_map([include_paused], |r| {
                    Ok((
                        r.get(0)?,
                        BindIdentity::new(
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                        ),
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        })
    }

    pub fn read_raw_tag_location(
        &self,
        bind: &BindIdentity,
        session_ref: &str,
        location: LogLocation,
        publication: &DurablePublication,
        tag: u64,
    ) -> Result<broca_core::TagLocation, DbError> {
        // Authenticate and catch up before looking at coordinates: a Setup may
        // have moved a tag that the old SQL snapshot would have reported absent.
        let head = self.read_raw_page(
            bind,
            session_ref,
            location,
            publication,
            RawPageRequest {
                limit: Some(1),
                ..Default::default()
            },
        )?;
        if !head.index_validated {
            return Err(invalid(
                "raw tag lookup cannot attest index coordinates; reconcile the session",
            ));
        }
        let key = index::sid(bind)?;
        let (candidates, indexed_through) = self.read_snapshot(|tx| {
            let count = tx.query_row("SELECT head_ord FROM session WHERE sid=?1", [&key], |r| r.get::<_,u64>(0))?;
            Ok((tx.prepare("SELECT ord FROM message WHERE sid=?1 AND tag_base<?2 AND tag_high>=?2 ORDER BY ord")?
                .query_map(params![key,tag], |r| r.get::<_,u64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?, count))
        })?;
        for ordinal in candidates {
            let page = self.read_raw_page(
                bind,
                session_ref,
                location,
                publication,
                RawPageRequest {
                    from_ordinal: Some(ordinal),
                    limit: Some(1),
                    include_tools: false,
                },
            )?;
            if !page.index_validated {
                return Err(invalid(
                    "raw tag lookup cannot attest message coordinates; reconcile the session",
                ));
            }
            if let Some(row) = page.page.messages.first() {
                if let Some(part) = row
                    .message
                    .part_tags()
                    .and_then(|tags| tags.parts.iter().position(|n| *n == Some(tag)))
                {
                    return Ok(broca_core::TagLocation {
                        found: Some((ordinal, part)),
                        indexed_through,
                    });
                }
            }
        }
        Ok(broca_core::TagLocation {
            found: None,
            indexed_through,
        })
    }

    /// The writer's descriptor-synced publication bounds this service read.
    /// Catch-up precedes the SQL snapshot,
    /// including a durable Setup that may renumber tags in already indexed messages.
    /// Callers serialize publication and reads for a session, and exclude prefix rewrites.
    pub fn read_raw_page(
        &self,
        bind: &BindIdentity,
        session_ref: &str,
        location: LogLocation,
        publication: &DurablePublication,
        request: RawPageRequest,
    ) -> Result<RawPageSnapshot, DbError> {
        let RawPageRequest {
            from_ordinal: from,
            limit,
            include_tools,
        } = request;
        let key = index::sid(bind)?;
        let cursor = self.read_snapshot(|tx| {
            Ok(tx
                .query_row(
                    "SELECT indexed_seq,indexed_end,locator_seed FROM session WHERE sid=?1",
                    [&key],
                    |r| {
                        Ok((
                            r.get::<_, Option<u64>>(0)?.unwrap_or(0),
                            r.get::<_, u64>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?)
        })?;
        let stale_fold = publication.sequence() != 0
            && cursor.as_ref().is_some_and(|(_, _, locator)| {
                serde_json::from_str::<serde_json::Value>(locator)
                    .ok()
                    .and_then(|value| value.get(1)?.get("head_fold")?.get("version")?.as_u64())
                    != Some(1)
            });
        if stale_fold {
            self.raw_fallback_notice(
                &key,
                SeedFallback::new("raw head fold missing or unsupported").reason,
            );
            self.forget_session(bind)?;
        }
        if stale_fold
            || cursor.as_ref().is_none_or(|(seq, end, _)| {
                *seq != publication.sequence() || *end != publication.byte_end()
            })
        {
            self.publish(bind, session_ref, location, publication)?;
        }
        let file = RefCell::new(broca_wal::fs_ops::File::open(publication.path())?);
        let read = |off: u64, len: u64| {
            let mut file = file.borrow_mut();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            (&mut *file)
                .take(len.min(publication.byte_end().saturating_sub(off)))
                .read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        if publication.sequence() == 0 {
            let page = full_page(&read, publication, from, limit, include_tools, true)?;
            if page.page.lineage_id != Some(publication.lineage()) {
                return Err(invalid("empty raw page lineage mismatch"));
            }
            return Ok(page);
        }
        let result = self.read_snapshot(|tx| {
            page(
                tx,
                &key,
                &read,
                publication.lineage(),
                from,
                limit,
                include_tools,
            )
        });
        match result {
            Ok(page) => Ok(page),
            Err(DbError::Invalid(_)) => {
                self.raw_fallback_notice(
                    &key,
                    SeedFallback::new("raw page index or source invalid").reason,
                );
                full_page(&read, publication, from, limit, include_tools, false)
            }
            Err(DbError::SeedFallback(reason)) => {
                self.raw_fallback_notice(&key, reason.reason);
                full_page(
                    &read,
                    publication,
                    from,
                    limit,
                    include_tools,
                    reason.reason == "raw page historical timestamp source",
                )
            }
            Err(error) => Err(error),
        }
    }

    fn raw_fallback_notice(&self, session: &str, reason: &'static str) {
        if self
            .raw_fallback_notices
            .lock()
            .unwrap()
            .insert(session.to_owned())
        {
            tracing::warn!(target: "session", session, reason, "raw page is using a full WAL replay");
        }
    }
}

fn invalid(reason: impl Into<String>) -> DbError {
    DbError::Invalid(reason.into())
}

pub(super) fn optional_head_identity(
    cursor: broca_core::Cursor,
    qualify: impl FnOnce() -> Result<broca_core::Cursor, DbError>,
) -> Result<broca_core::Cursor, DbError> {
    match qualify() {
        Err(DbError::CursorIdentityUnavailable(reason)) => {
            static NOTICE: std::sync::Once = std::sync::Once::new();
            NOTICE.call_once(|| tracing::warn!(target: "session", wal_seq = cursor.wal_seq, reason, "head cursor identity unavailable; serving the position without resume identity"));
            Ok(cursor)
        }
        result => result,
    }
}

pub(super) fn cursor_identity_in_tool_turn(
    read: &ReadAt<'_>,
    seq: u64,
    lo_seq: u64,
    lo_off: u64,
    hi_off: u64,
    durable_end: u64,
) -> Result<broca_wal::framing::CursorFrameIdentity, DbError> {
    use broca_wal::framing::{
        cursor_frame_identity, decode_v4_header, HEADER_LEN, MAX_PAYLOAD_LEN,
    };
    let unavailable = |reason: &str| DbError::CursorIdentityUnavailable(reason.into());
    if lo_off >= hi_off || hi_off > durable_end || lo_seq > seq {
        return Err(unavailable(
            "stored head is outside its indexed tool-turn span",
        ));
    }
    // The stored-head reread is bounded by one tool turn's span: a message row
    // keeps only (lo_seq, lo_off, hi_off), not an offset for every frame in the turn.
    let mut off = lo_off;
    let mut expected = lo_seq;
    while off < hi_off {
        if hi_off - off < HEADER_LEN as u64 {
            return Err(unavailable("short tool-turn cursor header"));
        }
        let mut bytes = access_counter::read_at(read, off, HEADER_LEN as u64)?;
        if bytes.len() != HEADER_LEN {
            return Err(unavailable("short tool-turn cursor header"));
        }
        let payload = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64;
        if payload > MAX_PAYLOAD_LEN as u64 {
            return Err(unavailable("oversized tool-turn cursor frame"));
        }
        let v4 = bytes[4] == broca_wal::framing::V4_FRAMING_VERSION;
        let header_len = if v4 {
            83 + u16::from_le_bytes(bytes[45..47].try_into().unwrap()) as u64
        } else {
            HEADER_LEN as u64
        };
        let frame_len = header_len + payload;
        if frame_len > hi_off - off {
            return Err(unavailable("tool-turn cursor frame exceeds indexed span"));
        }
        let extra = if v4 {
            header_len - HEADER_LEN as u64
        } else {
            payload
        };
        bytes.extend(access_counter::read_at(
            read,
            off + HEADER_LEN as u64,
            extra,
        )?);
        if bytes.len() as u64 != HEADER_LEN as u64 + extra {
            return Err(unavailable("short tool-turn cursor frame"));
        }
        let frame_seq = if v4 {
            let header = decode_v4_header(&bytes)
                .map_err(|_| unavailable("invalid tool-turn cursor header"))?
                .ok_or_else(|| unavailable("short tool-turn cursor header"))?;
            header.metadata.seq
        } else {
            // Legacy v1/v3 headers carry no checksum of their own, so verifying
            // them needs their payloads too, still wholly within this turn's span.
            match access_counter::decode_next(&bytes) {
                DecodeStep::Record { framed, consumed } if consumed == bytes.len() => framed.seq,
                DecodeStep::Lineage { consumed, .. } if consumed == bytes.len() => {
                    off += frame_len;
                    continue;
                }
                _ => return Err(unavailable("invalid legacy tool-turn cursor frame")),
            }
        };
        if frame_seq != expected {
            return Err(unavailable("tool-turn cursor sequence gap"));
        }
        if frame_seq == seq {
            if v4 {
                bytes.extend(access_counter::read_at(read, off + header_len, payload)?);
            }
            match access_counter::decode_next(&bytes) {
                DecodeStep::Record { framed, consumed }
                    if consumed as u64 == frame_len
                        && framed.seq == seq
                        && matches!(framed.record, WalRecord::ToolBatchFinished { .. }) =>
                {
                    return cursor_frame_identity(&bytes)
                        .ok_or_else(|| unavailable("tool-turn cursor identity missing"));
                }
                _ => {
                    return Err(unavailable(
                        "stored tool-turn head does not name ToolBatchFinished",
                    ))
                }
            }
        }
        if frame_seq > seq {
            break;
        }
        expected += 1;
        off += frame_len;
    }
    Err(unavailable(
        "stored cursor frame is absent from indexed tool-turn span",
    ))
}

/// The cache is operation-local: repeated metadata/message locators share one checked read.
struct Sources<'a> {
    read: &'a ReadAt<'a>,
    end: u64,
    frames: BTreeMap<u64, (u64, FramedRecord)>,
    ranges: BTreeMap<(u64, u64), Vec<FramedRecord>>,
}

impl<'a> Sources<'a> {
    fn compaction_frame(
        &mut self,
        tx: &Transaction<'_>,
        key: &str,
        off: u64,
    ) -> Result<(), DbError> {
        let bad = || {
            DbError::SeedFallback(SeedFallback::new(
                "raw page requires compaction source hydration",
            ))
        };
        let auth: Option<(u64, u64, Vec<u8>)> = tx.query_row(
            "SELECT frame_end,seq,frame_identity FROM source_auth WHERE sid=?1 AND frame_off=?2",
            params![key, off], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        let (end, seq, identity) = auth.ok_or_else(bad)?;
        let len = end
            .checked_sub(off)
            .filter(|len| *len > 0 && end <= self.end)
            .ok_or_else(bad)?;
        let bytes = access_counter::read_at(self.read, off, len)?;
        match access_counter::decode_next(&bytes) {
            DecodeStep::Record { framed, consumed }
                if consumed == bytes.len()
                    && framed.seq == seq
                    && Sha256::digest(&bytes).as_slice() == identity
                    && matches!(
                        framed.record,
                        WalRecord::CompactionMessage { .. } | WalRecord::CompactionAnswer { .. }
                    ) =>
            {
                self.frames.insert(off, (end, *framed));
                Ok(())
            }
            _ => Err(bad()),
        }
    }

    fn range(&mut self, lo: u64, hi: u64) -> Result<Vec<FramedRecord>, DbError> {
        if let Some(frames) = self.ranges.get(&(lo, hi)) {
            return Ok(frames.clone());
        }
        if lo >= hi || hi > self.end {
            return Err(invalid("raw source outside durable snapshot"));
        }
        let bytes = access_counter::read_at(self.read, lo, hi - lo)?;
        if bytes.len() as u64 != hi - lo {
            return Err(invalid("short raw source"));
        }
        let positioned = decode(&bytes, lo)?;
        let frames: Vec<_> = positioned.iter().map(|p| p.framed.clone()).collect();
        for (i, p) in positioned.iter().enumerate() {
            let end = positioned.get(i + 1).map_or(hi, |p| p.off);
            self.frames.insert(p.off, (end, p.framed.clone()));
        }
        self.ranges.insert((lo, hi), frames.clone());
        Ok(frames)
    }

    fn indexed_frame(
        &mut self,
        tx: &Transaction<'_>,
        key: &str,
        off: u64,
    ) -> Result<FramedRecord, DbError> {
        if let Some((_, frame)) = self.frames.get(&off) {
            return Ok(frame.clone());
        }
        let end: Option<u64> = tx
            .query_row(
                "SELECT frame_end FROM projection_boundary WHERE sid=?1 AND frame_off=?2",
                params![key, off],
                |r| r.get(0),
            )
            .optional()?;
        match end {
            Some(end) => self
                .range(off, end)?
                .into_iter()
                .next()
                .ok_or_else(|| invalid("raw boundary source missing")),
            None => self.frame(off),
        }
    }

    fn frame(&mut self, off: u64) -> Result<FramedRecord, DbError> {
        if let Some((_, frame)) = self.frames.get(&off) {
            return Ok(frame.clone());
        }
        if off >= self.end {
            return Err(invalid("raw metadata outside durable snapshot"));
        }
        let header =
            access_counter::read_at(self.read, off, broca_wal::framing::HEADER_LEN as u64)?;
        if header.len() != broca_wal::framing::HEADER_LEN {
            return Err(invalid("short raw metadata header"));
        }
        let payload = u32::from_le_bytes(header[..4].try_into().unwrap()) as u64;
        // Both envelopes start with payload length. Version four adds a variable
        // feature array; the subsequent decode authenticates the entire envelope.
        let envelope = if header[4] == 4 {
            83 + u16::from_le_bytes(header[45..47].try_into().unwrap()) as u64
        } else {
            broca_wal::framing::HEADER_LEN as u64
        };
        let len = envelope + payload;
        let frames = self.range(
            off,
            off.checked_add(len)
                .ok_or_else(|| invalid("raw source overflow"))?,
        )?;
        if frames.len() != 1 {
            return Err(invalid("raw metadata is not one frame"));
        }
        Ok(frames.into_iter().next().unwrap())
    }
}

fn decode(bytes: &[u8], start: u64) -> Result<Vec<PositionedFrame>, DbError> {
    let mut at = 0;
    let mut out: Vec<PositionedFrame> = Vec::new();
    while at < bytes.len() {
        match access_counter::decode_next(&bytes[at..]) {
            DecodeStep::Record { framed, consumed } => {
                if out
                    .last()
                    .is_some_and(|p| p.framed.seq.checked_add(1) != Some(framed.seq))
                {
                    return Err(invalid("raw source sequence gap"));
                }
                out.push(PositionedFrame {
                    off: start + at as u64,
                    framed: *framed,
                });
                at += consumed;
            }
            DecodeStep::Lineage { consumed, .. } => at += consumed,
            _ => return Err(invalid("invalid or incomplete durable raw source")),
        }
    }
    Ok(out)
}

fn page(
    tx: &Transaction<'_>,
    key: &str,
    read: &ReadAt<'_>,
    lineage: broca_wal::WalLineageId,
    from: Option<u64>,
    limit: Option<u64>,
    include_tools: bool,
) -> Result<RawPageSnapshot, DbError> {
    let session = tx.query_row("SELECT indexed_seq,indexed_off,indexed_end,indexed_identity,lineage_id,head_ord,locator_seed,admission_seed,title_off,tail_seed FROM session WHERE sid=?1", [key], |r| Ok(Session {
        seq: r.get(0)?, off: r.get(1)?, end: r.get(2)?, identity: r.get(3)?, lineage: r.get(4)?, count: r.get(5)?, locator: r.get(6)?, admission: r.get(7)?, title: r.get(8)?, projection: r.get(9)?,
    }))?;
    let cursor_bytes = access_counter::read_at(
        read,
        session.off,
        session
            .end
            .checked_sub(session.off)
            .ok_or_else(|| invalid("raw cursor outside log"))?,
    )?;
    let cursor_frame = match access_counter::decode_next(&cursor_bytes) {
        DecodeStep::Record { framed, consumed }
            if framed.seq == session.seq
                && Sha256::digest(&cursor_bytes[..consumed]).as_slice() == session.identity =>
        {
            (session.off + consumed as u64, *framed)
        }
        _ => return Err(invalid("raw row cursor identity mismatch")),
    };
    if cursor_bytes.len() as u64 != session.end - session.off {
        return Err(invalid("short raw cursor"));
    }
    let continuation = broca_core::admission::AdmissionContinuation::decode(&session.admission)
        .map_err(DbError::SeedFallback)?;
    let boundary = continuation.boundary();
    let projection =
        broca_core::projection_plan::ProjectionContinuation::decode(&session.projection)
            .map_err(DbError::SeedFallback)?;
    if projection.boundary() != boundary {
        return Err(invalid("raw continuation boundary disagreement"));
    }
    let (ordinal_base, tag_seed) = projection.raw_read_position();
    if boundary.lineage_id != session.lineage
        || session.lineage.as_deref() != Some(lineage.to_string().as_str())
    {
        return Err(invalid("raw snapshot lineage mismatch"));
    }
    let tail_bytes = access_counter::read_at(
        read,
        boundary.frame_end,
        session
            .end
            .checked_sub(boundary.frame_end)
            .ok_or_else(|| invalid("raw seed ahead of row cursor"))?,
    )?;
    let tail = decode(&tail_bytes, boundary.frame_end)?;
    let mut sources = Sources {
        read,
        end: session.end,
        frames: BTreeMap::from([(session.off, cursor_frame)]),
        ranges: BTreeMap::new(),
    };
    let current_off: Option<u64> = tx.query_row("SELECT run_started_off FROM episode WHERE sid=?1 AND run_started_off IS NOT NULL ORDER BY n DESC LIMIT 1", [key], |r| r.get(0)).optional()?.flatten();
    let first_off: Option<u64> = tx.query_row("SELECT run_started_off FROM episode WHERE sid=?1 AND run_started_off IS NOT NULL ORDER BY n LIMIT 1", [key], |r| r.get(0)).optional()?.flatten();
    let current = current_off
        .map(|off| sources.indexed_frame(tx, key, off))
        .transpose()?;
    if current
        .as_ref()
        .is_some_and(|f| !matches!(f.record, WalRecord::RunStarted { .. }))
    {
        return Err(invalid("raw current episode source is not RunStarted"));
    }
    let first = first_off
        .map(|off| sources.indexed_frame(tx, key, off))
        .transpose()?;
    if first
        .as_ref()
        .is_some_and(|f| !matches!(f.record, WalRecord::RunStarted { .. }))
    {
        return Err(invalid("raw first episode source is not RunStarted"));
    }
    let budget = current.as_ref().and_then(|f| match &f.record {
        WalRecord::RunStarted { config, .. } => config.budget.clone(),
        _ => None,
    });
    let seed = OffsetSeed::decode(&session.locator, budget.clone())
        .ok_or_else(|| invalid("invalid raw locator seed"))?;
    // This is the only seeded replay entry. Even fully indexed pages authenticate
    // their qualified boundary and contiguous durable tail before exposing SQL rows.
    let (replayed, boundary_identity) =
        broca_core::replay::replay_seeded_at_validated_boundary_with_identity(
            read,
            &boundary,
            &tail,
            current.as_ref().and_then(|f| match &f.record {
                WalRecord::RunStarted { config, .. } => Some(config.model.clone()),
                _ => None,
            }),
            current.as_ref().and_then(|f| match &f.record {
                WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
                _ => None,
            }),
            budget,
            None,
            ordinal_base,
            false,
            tag_seed,
            Vec::new(),
        )
        .map_err(DbError::SeedFallback)?;
    let compaction_sources: u64 = tx.query_row(
        "SELECT count(*) FROM source_auth WHERE sid=?1",
        [key],
        |r| r.get(0),
    )?;
    if compaction_sources != seed.admission.compaction.len() as u64 {
        return Err(DbError::SeedFallback(SeedFallback::new(
            "raw page requires compaction source hydration",
        )));
    }
    for off in &seed.admission.compaction {
        sources.compaction_frame(tx, key, *off)?;
    }
    let mut ownership = continuation
        .raw_read_ownership(read)
        .map_err(DbError::SeedFallback)?;
    let tail_frames: Vec<_> = tail.iter().map(|p| p.framed.clone()).collect();
    if broca_core::replay::project_deferred_calls(
        None,
        &tail_frames,
        session.lineage.as_deref(),
        None,
    )
    .is_some_and(|view| !view.calls.is_empty())
    {
        return Err(DbError::SeedFallback(SeedFallback::new(
            "raw page deferred tail state",
        )));
    }
    ownership.fold_owner_records(&tail_frames);
    let mut head = broca_core::head::HeadProjection::from_read_fold(
        seed.head_fold
            .as_ref()
            .ok_or_else(|| invalid("raw head fold missing"))?,
        first.as_ref().map(|f| &f.record),
        current.as_ref().map(|f| &f.record),
        ownership,
    )
    .ok_or_else(|| invalid("raw head fold unsupported"))?;
    continuation
        .restore_head_sender_admission(read, &mut head)
        .map_err(DbError::SeedFallback)?;
    for frame in &tail {
        if matches!(
            frame.framed.record,
            WalRecord::SessionRefreshAccepted { .. }
                | WalRecord::SessionRefreshPolicyChanged { .. }
                | WalRecord::SessionPrefixFlushed { .. }
        ) {
            head.observe(&frame.framed.record);
        }
    }
    for off in seed.admission.queued.values() {
        head.observe(&sources.frame(*off)?.record);
    }
    let limit = limit
        .unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT)
        .clamp(1, broca_core::SESSION_READ_MAX_LIMIT);
    let count = session.count + u64::from(replayed.open_tool_turn.is_some());
    let start = from
        .unwrap_or_else(|| count.saturating_sub(limit))
        .min(count);
    let end = start.saturating_add(limit).min(count);
    let rows = tx.prepare("SELECT ord,lo_seq,lo_off,hi_off,input_start,input_end,tag_base,tag_n,tag_high,flags FROM message WHERE sid=?1 AND ord>=?2 AND ord<?3 ORDER BY ord")?
        .query_map(params![key,start,end], |r| Ok((r.get::<_,u64>(0)?, OffsetSource {
            lo_seq: r.get(1)?, lo_off: r.get(2)?, hi_off: r.get(3)?, input_start: r.get(4)?, input_end: r.get(5)?, tag_base: r.get(6)?, tag_n: r.get(7)?, tag_high: r.get(8)?, flags: r.get(9)?,
        })))?.collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() as u64 != end.min(session.count).saturating_sub(start) {
        return Err(invalid("raw page ordinal gap"));
    }
    let mode3 = head
        .mode3
        .unwrap_or_else(|| broca_core::is_mode3_log(first.iter().map(|frame| &frame.record)));
    let mut messages = Vec::new();
    let mut dispatches = BTreeMap::new();
    let mut native_tool_outputs = BTreeMap::new();
    for (ordinal, source) in rows {
        if source.input_start.checked_add(1) != Some(source.input_end) {
            return Err(invalid("raw message sub-index range invalid"));
        }
        let frames = if let Some((hi, frame)) = sources
            .frames
            .get(&source.lo_off)
            .filter(|(hi, _)| *hi == source.hi_off)
        {
            let _ = hi;
            vec![frame.clone()]
        } else {
            sources.range(source.lo_off, source.hi_off)?
        };
        if frames.first().is_none_or(|f| f.seq != source.lo_seq) {
            return Err(invalid("raw message source sequence mismatch"));
        }
        let episode: (Option<String>,bool,Option<u64>,u64,Option<u64>) = tx.query_row(
            "SELECT run_id,attributable,run_started_off,end_ord,terminal_off FROM episode WHERE sid=?1 AND first_ord<=?2 AND end_ord>?2 ORDER BY n DESC LIMIT 1", params![key,ordinal],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        )?;
        let started = episode
            .2
            .map(|off| sources.indexed_frame(tx, key, off))
            .transpose()?;
        let (mut message, written_at, completed_at, created_at, turn) =
            hydrate_message(&frames, &source, started.as_ref(), mode3)?;
        // The disabled broca-state/2 indexed reader omits SendDelivered after ToolBatchFinished,
        // so delivered_as is missing and deliveries without a ride envelope remain unjoined.
        if message.role == Role::Tool {
            if let Some(outputs) =
                broca_core::display_facts::native_tool_outputs(&frames, &lineage.to_string())
                    .map_err(invalid)?
                    .remove(&source.input_start)
            {
                native_tool_outputs.insert(ordinal, outputs);
            }
        }
        if source.flags & 2 != 0 {
            let mut high = source
                .tag_base
                .ok_or_else(|| invalid("computed tag base missing"))?;
            broca_protocol::tags::number_message(
                &mut message,
                broca_protocol::tags::ComputedRules {
                    render: source.flags & 4 != 0,
                    clean: source.flags & 8 != 0,
                },
                &mut high,
            )
            .map_err(|_| invalid("raw tag numbering overflow"))?;
            if high != source.tag_high {
                return Err(invalid("raw computed tag count mismatch"));
            }
        }
        message.ordinal = Some(ordinal);
        let run = episode
            .0
            .filter(|_| episode.1)
            .map(|run_id| broca_core::RunAttribution {
                episode: run_id.clone(),
                run_id,
                is_final: episode.4.is_some() && ordinal + 1 == episode.3,
                delivery: message.tags.delivery().cloned(),
            });
        if let Some(turn) = turn {
            dispatches.insert(
                ordinal,
                broca_core::dispatches_of_turns(&frames, &[(0, turn)], ordinal)
                    .remove(&ordinal)
                    .unwrap_or_default(),
            );
        }
        messages.push(broca_core::SessionReadMessage {
            created_at,
            completed_at,
            ordinal,
            written_at,
            message,
            run,
        });
    }
    if session.count >= start && session.count < end {
        if let Some((mut message, step)) = replayed.open_tool_turn {
            message.ordinal = Some(session.count);
            let times = if mode3 {
                broca_core::message_times::mode3_written_times(
                    tail_frames.iter().map(|f| &f.record),
                    broca_core::placement::Placement::new(ordinal_base),
                    Default::default(),
                    Default::default(),
                )
                .map(|fold| fold.times)
            } else {
                broca_core::message_times::written_times(
                    tail_frames.iter().map(|f| &f.record),
                    broca_core::placement::Placement::new(ordinal_base),
                )
            }
            .map_err(invalid)?;
            let creations = broca_core::creation_times::project(
                tail_frames.iter().map(|frame| &frame.record),
                broca_core::placement::Placement::new(ordinal_base),
                head.queued_times().clone(),
                head.creation_times.clone().unwrap_or_default(),
            )
            .map_err(invalid)?;
            dispatches.extend(broca_core::dispatches_of_turns(
                &tail_frames,
                &[(0, step)],
                session.count,
            ));
            messages.push(broca_core::SessionReadMessage {
                created_at: creations.times.get(&session.count).copied(),
                completed_at: None,
                ordinal: session.count,
                message,
                written_at: times.get(&session.count).copied(),
                run: replayed.open_tool_run,
            });
        }
    }
    let terminal = seed
        .episode
        .as_ref()
        .and_then(|e| e.terminal_off)
        .map(|off| sources.frame(off))
        .transpose()?;
    let mut head_media = current.as_ref().and_then(|frame| match &frame.record {
        WalRecord::RunStarted { run_id, config, .. } => Some(RawHeadMedia {
            run_id: run_id.clone(),
            media: config.frozen_render_config.get("media").cloned(),
            interrupt_reason: None,
        }),
        _ => None,
    });
    if let Some(off) = seed.admission.refreeze {
        let frame = sources.frame(off)?;
        if current
            .as_ref()
            .is_none_or(|current| frame.seq <= current.seq)
        {
            return Err(invalid("raw head media refreeze precedes episode"));
        }
        match (&mut head_media, frame.record) {
            (
                Some(media),
                WalRecord::RunConfigRefrozen {
                    frozen_render_config,
                    ..
                },
            ) => {
                media.media = frozen_render_config.get("media").cloned();
            }
            _ => return Err(invalid("raw head media refreeze mismatch")),
        }
    }
    if let (Some(media), Some(frame)) = (&mut head_media, &terminal) {
        match &frame.record {
            WalRecord::RunFinished {
                reason,
                interrupt_reason,
                ..
            } => {
                media.interrupt_reason = (*reason == broca_protocol::RunFinishReason::Interrupted)
                    .then(|| interrupt_reason.clone())
                    .flatten();
            }
            _ => return Err(invalid("raw head media terminal mismatch")),
        }
    }
    let mut state_snapshot = StorePageSnapshot::empty();
    state_snapshot.last_run_id = current.as_ref().and_then(|f| match &f.record {
        WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
        _ => None,
    });
    state_snapshot.completed_steps = u64::from(
        seed.episode
            .as_ref()
            .is_some_and(|e| e.last_step_off.is_some()),
    );
    // The latest step and terminal supply the current episode's result. Keep all
    // authenticated tail controls as well, including pauses and admission changes.
    let last_step = seed
        .episode
        .as_ref()
        .and_then(|e| e.last_step_off)
        .map(|off| sources.indexed_frame(tx, key, off))
        .transpose()?;
    let mut state_frames = last_step
        .into_iter()
        .chain(terminal)
        .chain(tail_frames.iter().cloned())
        .collect::<Vec<_>>();
    state_frames.sort_by_key(|f| f.seq);
    state_frames.dedup_by_key(|f| f.seq);
    let state_page =
        broca_core::read_session_page(&state_snapshot, &state_frames, Some(0), Some(1))
            .map_err(|e| invalid(e.to_string()))?;
    let lineage_id = Some(lineage);
    let last_title = session
        .title
        .map(|off| sources.frame(off))
        .transpose()?
        .and_then(|f| f.record.title().map(str::to_owned));
    let tools = if include_tools {
        current.and_then(|f| match f.record {
            WalRecord::RunStarted { run_id, config, .. } => Some((run_id, config.tools)),
            _ => None,
        })
    } else {
        None
    };
    let control_head = broca_core::last_control_cursor_through(&tail_frames, None).or(seed
        .qualified_control_head
        .map(|(seq, sub)| broca_core::Cursor::new(seq, sub)));
    let control_head = control_head.map(|cursor| optional_head_identity(cursor, || {
        let identity = if cursor.wal_seq == boundary.seq {
            boundary_identity
        } else if let Some(frame) = tail.iter().find(|frame| frame.framed.seq == cursor.wal_seq) {
            let offset = (frame.off - boundary.frame_end) as usize;
            broca_wal::framing::cursor_frame_identity(&tail_bytes[offset..]).ok_or_else(|| DbError::CursorIdentityUnavailable("verified tail frame has no identity".into()))?
        } else {
            // TurnFinished adds no control unit. Its stored head names an earlier
            // ToolBatchFinished within the last tool turn's indexed source span.
            let span: Option<(u64, u64, u64)> = tx.query_row(
                "SELECT lo_seq,lo_off,hi_off FROM message WHERE sid=?1 AND lo_seq<=?2 ORDER BY ord DESC LIMIT 1",
                params![key, cursor.wal_seq], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).optional()?;
            let (lo_seq, lo_off, hi_off) = span.ok_or_else(|| DbError::CursorIdentityUnavailable("stored head has no indexed tool-turn span".into()))?;
            cursor_identity_in_tool_turn(read, cursor.wal_seq, lo_seq, lo_off, hi_off, session.end)?
        };
        Ok::<_, DbError>(cursor.with_frame_identity(lineage, identity))
    })).transpose()?;
    Ok(RawPageSnapshot {
        index_validated: true,
        page: SessionReadPage {
            messages,
            next_from_ordinal: (end < count).then_some(end),
            head: control_head,
            lineage_id,
            lineage_state: state_page.lineage_state,
            prompt_len: count,
            deferred_calls: None,
            late_approvals: Vec::new(),
            withdrawals_owed: false,
            tags_await_setup: state_page.tags_await_setup,
        },
        at_seq: session.seq,
        head,
        head_media,
        last_title,
        last_write_ms: seed.last_write_ms,
        retention_activity_ms: seed.frozen_policy.and(seed.last_activity_ms),
        tools,
        dispatches,
        native_tool_outputs,
    })
}

type HydratedMessage = (
    Message,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    Option<usize>,
);

fn hydrate_message(
    frames: &[FramedRecord],
    source: &OffsetSource,
    started: Option<&FramedRecord>,
    mode3: bool,
) -> Result<HydratedMessage, DbError> {
    let first = frames
        .first()
        .ok_or_else(|| invalid("empty raw message source"))?;
    if mode3 && matches!(first.record, WalRecord::BudgetFired { .. }) {
        return Err(DbError::SeedFallback(SeedFallback::new(
            "mode3 budget timestamp source",
        )));
    }
    let creations = broca_core::creation_times::project(
        frames.iter().map(|frame| &frame.record),
        broca_core::placement::Placement::default(),
        Default::default(),
        Default::default(),
    )
    .map_err(invalid)?;
    let created_at = creations.times.get(&source.input_start).copied();
    let input = match &first.record {
        WalRecord::RunStarted { input, .. }
        | WalRecord::Steered { input, .. }
        | WalRecord::SendDelivered {
            input: Some(input), ..
        } => Some(input),
        _ => None,
    };
    if let Some(input) = input {
        if matches!(
            &first.record,
            WalRecord::RunStarted {
                written_at_ms: None,
                submission_id: Some(_),
                ..
            } | WalRecord::SendDelivered {
                written_at_ms: None,
                ..
            }
        ) {
            return Err(DbError::SeedFallback(SeedFallback::new(
                "raw page historical timestamp source",
            )));
        }
        let mut message = input
            .get(source.input_start as usize)
            .cloned()
            .ok_or_else(|| invalid("raw input sub-index invalid"))?;
        broca_core::tags::attach(
            &mut message,
            first.record.runner_tags().get(source.input_start as usize),
        );
        let time = match &first.record {
            WalRecord::RunStarted {
                written_at_ms,
                ts_ms,
                ..
            }
            | WalRecord::Steered {
                written_at_ms,
                ts_ms,
                ..
            } => written_at_ms.or(*ts_ms),
            WalRecord::SendDelivered { written_at_ms, .. } => *written_at_ms,
            _ => None,
        }
        .filter(|time| *time != 0);
        if mode3 && time.is_none() {
            return Err(DbError::SeedFallback(SeedFallback::new(
                "mode3 historical timestamp source",
            )));
        }
        return Ok((message, time, None, created_at, None));
    }
    if frames.iter().any(|f| {
        matches!(
            f.record,
            WalRecord::ModelStepFinished {
                written_at_ms: None,
                ..
            }
        )
    }) {
        return Err(DbError::SeedFallback(SeedFallback::new(
            "raw page historical timestamp source",
        )));
    }
    let records: Vec<_> = frames.iter().map(|f| f.record.clone()).collect();
    let replay = broca_core::replay(&records).map_err(|e| invalid(e.to_string()))?;
    let mut message = replay
        .prompt
        .get(source.input_start as usize)
        .cloned()
        .ok_or_else(|| invalid("raw turn sub-index invalid"))?;
    if message.role == Role::Assistant && message.origin.is_none() {
        message.origin = started.and_then(|f| match &f.record {
            WalRecord::RunStarted { config, .. } => Some(config.model.clone()),
            _ => None,
        });
    }
    // The turn range includes all result/no-dispatch frames, so timestamps and
    // dispatch ordering use the same folds as a full transcript read.
    let time_facts = broca_core::message_times::fold(
        frames.iter().map(|f| &f.record),
        broca_core::placement::Placement::default(),
        Default::default(),
        mode3,
        Default::default(),
    )
    .map_err(invalid)?;
    let completions = broca_core::compaction_v2::entries::completion_times(
        &replay.prompt,
        0,
        &time_facts.times,
        &time_facts.native_assistants,
    );
    let turn = (source.input_start == 0)
        .then(|| replay.committed_tool_turns.first().map(|(_, index)| *index))
        .flatten();
    Ok((
        message,
        time_facts.times.get(&source.input_start).copied(),
        completions
            .get(source.input_start as usize)
            .copied()
            .flatten(),
        created_at,
        turn,
    ))
}

fn full_page(
    read: &ReadAt<'_>,
    publication: &DurablePublication,
    from: Option<u64>,
    limit: Option<u64>,
    include_tools: bool,
    index_validated: bool,
) -> Result<RawPageSnapshot, DbError> {
    let bytes = access_counter::read_at(read, 0, publication.byte_end())?;
    let positioned = decode(&bytes, 0)?;
    let offsets: Vec<_> = positioned
        .iter()
        .map(|frame| (frame.framed.seq, frame.off))
        .collect();
    let frames: Vec<_> = positioned.into_iter().map(|p| p.framed).collect();
    let (mut page, dispatches) = broca_core::read_session_page_owned_with_dispatches(
        StorePageSnapshot::empty(),
        &frames,
        from,
        limit,
    )
    .map_err(|e| invalid(e.to_string()))?;
    page.lineage_id = match access_counter::decode_next(&bytes) {
        DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id),
        _ => None,
    };
    if let (Some(cursor), Some(lineage)) = (page.head, page.lineage_id) {
        let off = offsets
            .iter()
            .find(|(seq, _)| *seq == cursor.wal_seq)
            .expect("projected cursor frame was decoded")
            .1;
        let identity = broca_wal::framing::cursor_frame_identity(&bytes[off as usize..])
            .expect("verified record frame");
        page.head = Some(cursor.with_frame_identity(lineage, identity));
    }
    let mut head = broca_core::head::HeadProjection::default();
    for frame in &frames {
        head.observe(&frame.record);
    }
    let mut head_media = None;
    for frame in &frames {
        match &frame.record {
            WalRecord::RunStarted { run_id, config, .. } => {
                head_media = Some(RawHeadMedia {
                    run_id: run_id.clone(),
                    media: config.frozen_render_config.get("media").cloned(),
                    interrupt_reason: None,
                });
            }
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => {
                if let Some(media) = &mut head_media {
                    media.media = frozen_render_config.get("media").cloned();
                }
            }
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                interrupt_reason,
                ..
            } => {
                if let Some(media) = &mut head_media {
                    media.interrupt_reason = interrupt_reason.clone();
                }
            }
            _ => {}
        }
    }
    let last_title = frames
        .iter()
        .rev()
        .find_map(|f| f.record.title())
        .map(str::to_owned);
    let last_write_ms = frames
        .iter()
        .rev()
        .find_map(|f| broca_core::record_time_ms(&f.record));
    let tools = include_tools
        .then(|| {
            frames.iter().rev().find_map(|f| match &f.record {
                WalRecord::RunStarted { run_id, config, .. } => {
                    Some((run_id.clone(), config.tools.clone()))
                }
                _ => None,
            })
        })
        .flatten();
    Ok(RawPageSnapshot {
        index_validated,
        native_tool_outputs: broca_core::display_facts::native_tool_outputs(
            &frames,
            &publication.lineage().to_string(),
        )
        .map_err(invalid)?,
        page,
        dispatches,
        at_seq: publication.sequence(),
        head,
        head_media,
        last_title,
        last_write_ms,
        retention_activity_ms: None,
        tools,
    })
}

#[cfg(test)]
mod cursor_span_tests {
    use super::*;

    #[test]
    fn a_head_survives_an_unavailable_cursor_identity_but_not_other_read_errors() {
        let cursor = broca_core::Cursor::new(12, 0);
        let head = optional_head_identity(cursor, || {
            Err(DbError::CursorIdentityUnavailable("missing span".into()))
        })
        .unwrap();
        assert_eq!(head, cursor);
        assert!(head.frame_identity().is_none());
        assert!(matches!(
            optional_head_identity(cursor, || Err(DbError::Invalid("corrupt page".into()))),
            Err(DbError::Invalid(_))
        ));
    }

    fn fixture(
        prefix: usize,
        v4: bool,
    ) -> (Vec<u8>, u64, u64, broca_wal::framing::CursorFrameIdentity) {
        let records = [
            serde_json::from_value(serde_json::json!({"type":"tool_batch_finished","batch_id":"earlier"})).unwrap(),
            serde_json::from_value(serde_json::json!({"type":"tool_result","batch_id":"b","outcome":"completed","result":{"tool_call_id":"c","output":{"kind":"text","text":"R".repeat(128 * 1024)},"is_error":false}})).unwrap(),
            serde_json::from_value(serde_json::json!({"type":"tool_batch_finished","batch_id":"b"})).unwrap(),
        ];
        let mut bytes = vec![0x99; prefix];
        let lo = bytes.len() as u64;
        let mut identity = None;
        for (i, record) in records.iter().enumerate() {
            let seq = 10 + i as u64;
            let encoded = if v4 {
                broca_wal::framing::encode_v4(
                    broca_wal::framing::V4FrameMetadata {
                        seq,
                        fence: 1,
                        appended_ms: 1,
                        writer_id: [1; 16],
                    },
                    &[],
                    record,
                )
                .unwrap()
            } else {
                broca_wal::framing::encode(seq, 1, record).unwrap()
            };
            if seq == 12 {
                identity = broca_wal::framing::cursor_frame_identity(&encoded);
            }
            bytes.extend(encoded);
        }
        let hi = bytes.len() as u64;
        bytes.extend(vec![0x77; 4096]);
        (bytes, lo, hi, identity.unwrap())
    }

    fn span_cost(v4: bool) {
        let mut costs = Vec::new();
        let mut lengths = Vec::new();
        for prefix in [16 * 1024, 4 * 1024 * 1024] {
            let (bytes, lo, hi, expected) = fixture(prefix, v4);
            lengths.push(bytes.len());
            let total = std::cell::Cell::new(0);
            let read = |off: u64, len: u64| {
                assert!(
                    off >= lo && off + len <= hi,
                    "cursor walk escaped indexed turn: {off}+{len}, span {lo}..{hi}"
                );
                total.set(total.get() + len);
                Ok(bytes[off as usize..(off + len) as usize].to_vec())
            };
            let identity =
                cursor_identity_in_tool_turn(&read, 12, 10, lo, hi, bytes.len() as u64).unwrap();
            assert_eq!(
                identity, expected,
                "the digest must come from ToolBatchFinished, not the next boundary"
            );
            if v4 {
                assert!(
                    total.get() < (hi - lo) / 100,
                    "v4 walk read the large result payload"
                );
            } else {
                assert_eq!(
                    total.get(),
                    hi - lo,
                    "legacy verification must read exactly the turn span"
                );
            }
            costs.push(total.get());
        }
        assert!(lengths[1] >= lengths[0] * 10);
        assert_eq!(costs[0], costs[1]);
    }

    #[test]
    fn stored_head_v4_walk_skips_large_result_and_reads_only_indexed_turn_span() {
        span_cost(true);
    }

    #[test]
    fn stored_head_legacy_walk_verifies_payloads_only_within_indexed_turn_span() {
        span_cost(false);
    }

    #[test]
    fn stored_head_walk_refuses_missing_target_and_corrupt_header() {
        let (mut bytes, lo, hi, _) = fixture(512, true);
        let read = |off: u64, len: u64| Ok(bytes[off as usize..(off + len) as usize].to_vec());
        let error =
            cursor_identity_in_tool_turn(&read, 13, 10, lo, hi, bytes.len() as u64).unwrap_err();
        assert!(error
            .to_string()
            .contains("cursor_identity_unavailable: stored cursor frame is absent"));
        bytes[lo as usize + 20] ^= 1;
        let read = |off: u64, len: u64| Ok(bytes[off as usize..(off + len) as usize].to_vec());
        let error =
            cursor_identity_in_tool_turn(&read, 12, 10, lo, hi, bytes.len() as u64).unwrap_err();
        assert!(error
            .to_string()
            .contains("cursor_identity_unavailable: invalid tool-turn cursor header"));
    }
}
