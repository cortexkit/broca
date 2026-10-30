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
    pub last_title: Option<String>,
    pub last_write_ms: Option<u64>,
    pub retention_activity_ms: Option<u64>,
    pub tools: Option<(RunId, std::sync::Arc<Vec<broca_protocol::request::Tool>>)>,
    pub dispatches: BTreeMap<u64, Vec<broca_core::ToolCallDispatch>>,
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
                            r.get::<_, u64>(0)?,
                            r.get::<_, u64>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?)
        })?;
        let stale_fold = cursor.as_ref().is_some_and(|(_, _, locator)| {
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

/// The cache is operation-local: repeated metadata/message locators share one checked read.
struct Sources<'a> {
    read: &'a ReadAt<'a>,
    end: u64,
    frames: BTreeMap<u64, (u64, FramedRecord)>,
    ranges: BTreeMap<(u64, u64), Vec<FramedRecord>>,
}

impl<'a> Sources<'a> {
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
    let replayed = broca_core::replay::replay_seeded_at_validated_boundary(
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
    let mut ownership = continuation
        .raw_read_ownership()
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
    let mut messages = Vec::new();
    let mut dispatches = BTreeMap::new();
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
        let (mut message, written_at, turn) = hydrate_message(&frames, &source, started.as_ref())?;
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
            ordinal,
            written_at,
            message,
            run,
        });
    }
    if session.count >= start && session.count < end {
        if let Some((mut message, step)) = replayed.open_tool_turn {
            message.ordinal = Some(session.count);
            let times = broca_core::message_times::written_times(
                tail_frames.iter().map(|f| &f.record),
                broca_core::placement::Placement::new(ordinal_base),
            )
            .map_err(invalid)?;
            dispatches.extend(broca_core::dispatches_of_turns(
                &tail_frames,
                &[(0, step)],
                session.count,
            ));
            messages.push(broca_core::SessionReadMessage {
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
    Ok(RawPageSnapshot {
        index_validated: true,
        page: SessionReadPage {
            messages,
            next_from_ordinal: (end < count).then_some(end),
            head: broca_core::last_control_cursor_through(&tail_frames, None).or(seed
                .qualified_control_head
                .map(|(seq, sub)| broca_core::Cursor::new(seq, sub))),
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
        last_title,
        last_write_ms: seed.last_write_ms,
        retention_activity_ms: seed.frozen_policy.and(seed.last_activity_ms),
        tools,
        dispatches,
    })
}

fn hydrate_message(
    frames: &[FramedRecord],
    source: &OffsetSource,
    started: Option<&FramedRecord>,
) -> Result<(Message, Option<u64>, Option<usize>), DbError> {
    let first = frames
        .first()
        .ok_or_else(|| invalid("empty raw message source"))?;
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
        return Ok((message, time, None));
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
    let times = broca_core::message_times::written_times(
        frames.iter().map(|f| &f.record),
        broca_core::placement::Placement::default(),
    )
    .map_err(invalid)?;
    let turn = (source.input_start == 0)
        .then(|| replay.committed_tool_turns.first().map(|(_, index)| *index))
        .flatten();
    Ok((message, times.get(&source.input_start).copied(), turn))
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
    let mut head = broca_core::head::HeadProjection::default();
    for frame in &frames {
        head.observe(&frame.record);
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
        page,
        dispatches,
        at_seq: publication.sequence(),
        head,
        last_title,
        last_write_ms,
        retention_activity_ms: None,
        tools,
    })
}
