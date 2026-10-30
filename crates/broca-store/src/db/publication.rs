//! Durable event consumption. Row and continuation cursors deliberately advance
//! independently: an open run is discoverable before it can seed projection replay.

use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};

use broca_core::admission::AdmissionContinuation;
use broca_core::projection_plan::{u64_to_i64, OffsetSeed, ProjectionContinuation};
use broca_core::replay::{PositionedFrame, SeedBoundary, SeedFallback};
use broca_protocol::BindIdentity;
use broca_wal::access_counter::{self, ValidationRead};
use broca_wal::file_wal::DurablePublication;
use broca_wal::framing::{DecodeStep, LINEAGE_FRAME_LEN};
use rusqlite::{params, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use super::index::{self, IndexedFrame};
use super::{DbError, IndexOutcome, LogLocation, OverhaulDb};

pub(super) type ReadAt<'a> = dyn Fn(u64, u64) -> std::io::Result<Vec<u8>> + 'a;

/// A descriptor-synced prefix whose lineage and sequence have been checked.
/// Boot recovery and writer publication supply this only after their own sync.
pub(super) struct CheckedEnd<'a> {
    pub lineage: &'a str,
    pub end: u64,
    pub seq: u64,
}

pub(super) struct PreparedCursor {
    cp: Option<RowCursor>,
    seed: Option<OffsetSeed>,
    rebuilt: bool,
}

impl PreparedCursor {
    pub fn start(&self) -> (u64, Option<u64>) {
        self.cp.as_ref().map_or((0, None), |cp| (cp.end, cp.seq))
    }
}

struct RowCursor {
    seq: Option<u64>,
    off: u64,
    end: u64,
    identity: Option<Vec<u8>>,
    lineage: Option<String>,
    locator: String,
    lineage_off: Option<u64>,
}

/// Only the validator can construct index input. Every frame is complete,
/// contiguous with the checked row cursor and bounded by the synced witness.
struct ValidatedRange {
    frames: Vec<IndexedFrame>,
    end: u64,
    seq: Option<u64>,
    lineage_off: Option<u64>,
}

impl ValidatedRange {
    fn read(
        read: &ReadAt<'_>,
        start: u64,
        end: u64,
        after: Option<u64>,
        seq: u64,
        lineage: &str,
    ) -> Result<Self, DbError> {
        let bytes = access_counter::read_at(read, start, end - start)?;
        if bytes.len() as u64 != end - start {
            return Err(DbError::Invalid(
                "durable publication is shorter than its witness".into(),
            ));
        }
        let mut at = 0;
        let mut last = after;
        let mut frames = Vec::new();
        let mut lineage_off = None;
        while at < bytes.len() {
            let (framed, consumed) = match access_counter::decode_next(&bytes[at..]) {
                DecodeStep::Record { framed, consumed } => (*framed, consumed),
                DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } if lineage_id.to_string() == lineage => {
                    lineage_off = Some(start + at as u64);
                    at += consumed;
                    continue;
                }
                _ => {
                    return Err(DbError::Invalid(
                        "durable publication contains an incomplete or invalid frame".into(),
                    ))
                }
            };
            if last.map_or(framed.seq != 1, |last: u64| {
                last.checked_add(1) != Some(framed.seq)
            }) {
                return Err(DbError::Invalid(
                    "durable publication sequence is not contiguous".into(),
                ));
            }
            frames.push(IndexedFrame {
                off: start + at as u64,
                end: start + (at + consumed) as u64,
                identity: Sha256::digest(&bytes[at..at + consumed]).to_vec(),
                framed,
            });
            last = Some(frames.last().unwrap().framed.seq);
            at += consumed;
        }
        if last.unwrap_or(0) != seq {
            return Err(DbError::Invalid(
                "durable publication sequence differs from its witness".into(),
            ));
        }
        Ok(Self {
            frames,
            end,
            seq: last,
            lineage_off,
        })
    }
}

impl OverhaulDb {
    /// Consume the latest writer event; skipping intermediate events is safe.
    /// All reads, including seed authentication and hydration, are capped at the
    /// witness end, even if the live writer has already appended another batch.
    /// The caller must serialize events for each session in publication order.
    /// Committed bytes within a lineage are immutable; a caller rewriting an
    /// earlier prefix must first use [`Self::forget_session`].
    pub fn publish(
        &self,
        bind: &BindIdentity,
        session_ref: &str,
        location: LogLocation,
        publication: &DurablePublication,
    ) -> Result<IndexOutcome, DbError> {
        let file = RefCell::new(broca_wal::fs_ops::File::open(publication.path())?);
        let read = |off: u64, len: u64| {
            let len = len.min(publication.byte_end().saturating_sub(off));
            let mut file = file.borrow_mut();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            (&mut *file).take(len).read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        self.transaction(false, |tx| {
            publish_in_tx(tx, bind, session_ref, location, publication, &read)
        })
    }
}

fn load_locator(
    tx: &Transaction<'_>,
    key: &str,
    encoded: &str,
    read: &ReadAt<'_>,
) -> Result<Option<OffsetSeed>, DbError> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(encoded) else {
        return Ok(None);
    };
    let budget = match value
        .get(1)
        .and_then(|v| v.get("admission"))
        .and_then(|v| v.get("run_started"))
        .and_then(|v| v.as_u64())
    {
        Some(off) => {
            let end: Option<u64> = tx
                .query_row(
                    "SELECT frame_end FROM projection_boundary WHERE sid=?1 AND frame_off=?2",
                    params![key, off],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(end) = end.filter(|end| *end > off) else {
                return Ok(None);
            };
            let bytes = access_counter::read_at(read, off, end - off)?;
            match access_counter::decode_next(&bytes) {
                DecodeStep::Record { framed, consumed } if consumed == bytes.len() => {
                    match framed.record {
                        broca_wal::WalRecord::RunStarted { config, .. } => config.budget,
                        _ => return Ok(None),
                    }
                }
                _ => return Ok(None),
            }
        }
        None => None,
    };
    Ok(OffsetSeed::decode(encoded, budget))
}

fn valid_cursor(
    cp: &RowCursor,
    lineage: &str,
    end: u64,
    read: &ReadAt<'_>,
) -> Result<bool, DbError> {
    if cp.lineage.as_deref() != Some(lineage) || cp.end > end || cp.off > cp.end {
        return Ok(false);
    }
    let Some(seq) = cp.seq else {
        return Ok(cp.end == LINEAGE_FRAME_LEN as u64 && cp.identity.is_none());
    };
    let bytes = access_counter::read_validation_at(
        ValidationRead::BoundaryFrame,
        read,
        cp.off,
        cp.end - cp.off,
    )?;
    if bytes.len() as u64 != cp.end - cp.off {
        return Ok(false);
    }
    let consumed = match access_counter::decode_next(&bytes) {
        DecodeStep::Record { framed, consumed }
            if framed.seq == seq
                && cp.identity.as_deref()
                    == Some(Sha256::digest(&bytes[..consumed]).as_slice()) =>
        {
            consumed
        }
        _ => return Ok(false),
    };
    let mut at = consumed;
    while at < bytes.len() {
        match access_counter::decode_next(&bytes[at..]) {
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } if lineage_id.to_string() == lineage => at += consumed,
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn publish_in_tx(
    tx: &Transaction<'_>,
    bind: &BindIdentity,
    session_ref: &str,
    location: LogLocation,
    publication: &DurablePublication,
    read: &ReadAt<'_>,
) -> Result<IndexOutcome, DbError> {
    let key = index::sid(bind)?;
    let header = access_counter::read_at(read, 0, LINEAGE_FRAME_LEN as u64)?;
    let lineage = match access_counter::decode_next(&header) {
        DecodeStep::Lineage {
            lineage_id,
            consumed,
        } if consumed == header.len() && lineage_id == publication.lineage() => {
            lineage_id.to_string()
        }
        _ => {
            return Err(DbError::Invalid(
                "durable publication lineage differs from its witness".into(),
            ))
        }
    };
    let checked = CheckedEnd {
        lineage: &lineage,
        end: publication.byte_end(),
        seq: publication.sequence(),
    };
    let prepared = prepare_cursor(tx, &key, &lineage, checked.end, read, None, false)?;
    publish_checked_in_tx(tx, bind, session_ref, location, &checked, read, prepared)
}

pub(super) fn prepare_cursor(
    tx: &Transaction<'_>,
    key: &str,
    lineage: &str,
    end: u64,
    read: &ReadAt<'_>,
    rebuild_reason: Option<&'static str>,
    force: bool,
) -> Result<PreparedCursor, DbError> {
    let cp = tx.query_row("SELECT indexed_seq,indexed_off,indexed_end,indexed_identity,lineage_id,locator_seed,lineage_off FROM session WHERE sid=?1", [key], |r| Ok(RowCursor {
        seq: r.get(0)?, off: r.get(1)?, end: r.get(2)?, identity: r.get(3)?, lineage: r.get(4)?, locator: r.get(5)?, lineage_off: r.get(6)?,
    })).optional()?;
    let mut seed = None;
    let mut rebuilt = force;
    if let Some(cp) = &cp {
        if !force && valid_cursor(cp, lineage, end, read)? {
            seed = load_locator(tx, key, &cp.locator, read)?;
        }
        rebuilt = seed.is_none();
    }
    if cp.is_none() || rebuilt {
        SeedFallback::new(rebuild_reason.unwrap_or(if cp.is_none() {
            "publication index missing"
        } else {
            "publication row cursor invalid"
        }));
        index::clear_session(tx, key)?;
    }
    let cp = if rebuilt { None } else { cp };
    Ok(PreparedCursor { cp, seed, rebuilt })
}

pub(super) fn publish_checked_in_tx(
    tx: &Transaction<'_>,
    bind: &BindIdentity,
    session_ref: &str,
    location: LogLocation,
    checked: &CheckedEnd<'_>,
    read: &ReadAt<'_>,
    prepared: PreparedCursor,
) -> Result<IndexOutcome, DbError> {
    let key = index::sid(bind)?;
    let PreparedCursor { cp, seed, rebuilt } = prepared;
    let lineage = checked.lineage;
    let start = cp.as_ref().map_or(LINEAGE_FRAME_LEN as u64, |cp| cp.end);
    let range = ValidatedRange::read(
        read,
        start,
        checked.end,
        cp.as_ref().and_then(|cp| cp.seq),
        checked.seq,
        lineage,
    )?;
    let mut seed = seed.unwrap_or_default();
    tx.execute("INSERT INTO session (sid,project_root,harness,session_key,session_ref,lineage_id,location,indexed_off,indexed_end,head_ord,tail_seed,admission_seed)
        VALUES (?1,?2,?3,?4,?5,?6,?7,0,0,0,'','') ON CONFLICT(sid) DO UPDATE SET session_ref=excluded.session_ref,location=excluded.location",
        params![key,bind.project_root.display().to_string(),bind.harness,bind.session,session_ref,lineage,location.as_str()])?;
    for frame in &range.frames {
        if let broca_wal::WalRecord::RunStarted { session, .. } = &frame.framed.record {
            if session != bind {
                return Err(DbError::Invalid(
                    "WAL bind identity differs from indexed session".into(),
                ));
            }
        }
        index::apply_frame(tx, &key, Some(lineage), &mut seed, frame)?;
    }
    let off = range
        .frames
        .last()
        .map_or_else(|| cp.as_ref().map_or(0, |cp| cp.off), |frame| frame.off);
    let identity = range
        .frames
        .last()
        .map(|frame| frame.identity.clone())
        .or_else(|| cp.as_ref().and_then(|cp| cp.identity.clone()));
    let lineage_off = range
        .lineage_off
        .or_else(|| cp.as_ref().and_then(|cp| cp.lineage_off))
        .unwrap_or(0);
    tx.execute("UPDATE session SET lineage_id=?2,indexed_seq=?3,indexed_off=?4,indexed_end=?5,indexed_identity=?6,head_ord=?7,locator_seed=?8,frozen_policy=?9,last_activity_ms=?10,last_write_ms=?11,title_off=?12,lineage_off=?13 WHERE sid=?1",
        params![key,lineage,range.seq.map(u64_to_i64).transpose()?,u64_to_i64(off)?,u64_to_i64(range.end)?,identity,u64_to_i64(seed.head_ord())?,seed.encode()?,seed.frozen_policy.map(u64_to_i64).transpose()?,seed.last_activity_ms.map(u64_to_i64).transpose()?,seed.last_write_ms.map(u64_to_i64).transpose()?,seed.title_off.map(u64_to_i64).transpose()?,u64_to_i64(lineage_off)?])?;
    publish_seeds(tx, &key, read)?;
    Ok(IndexOutcome {
        rebuilt,
        indexed_seq: range.seq,
        indexed_end: range.end,
        head_ord: seed.head_ord(),
    })
}

fn positioned(read: &ReadAt<'_>, start: u64, end: u64) -> Result<Vec<PositionedFrame>, DbError> {
    broca_core::projection::decode_positioned_frames(read, start, end, None)
        .map_err(DbError::Invalid)
}

fn publish_seeds(tx: &Transaction<'_>, key: &str, read: &ReadAt<'_>) -> Result<(), DbError> {
    let boundary = tx.query_row(
        "SELECT lineage_id,seq,frame_off,frame_end,frame_identity,qualified FROM projection_boundary WHERE sid=?1 AND qualified=1 ORDER BY seq DESC LIMIT 1",
        [key], |r| Ok(SeedBoundary { lineage_id: r.get(0)?, seq: r.get(1)?, frame_off: r.get(2)?, frame_end: r.get(3)?, frame_identity: r.get(4)?, qualified: r.get(5)? }),
    ).optional()?;
    let Some(boundary) = boundary else {
        tx.execute(
            "UPDATE session SET admission_seed=?2,tail_seed=?2 WHERE sid=?1",
            params![key, "{\"version\":1,\"continuation\":null}"],
        )?;
        return Ok(());
    };
    let (tail, admission): (String, String) = tx.query_row(
        "SELECT tail_seed,admission_seed FROM session WHERE sid=?1",
        [key],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let stored = if tail.is_empty() || tail.contains("\"continuation\":null") {
        None
    } else {
        ProjectionContinuation::decode(&tail)
            .and_then(|seed| {
                let admission = AdmissionContinuation::decode(&admission)?;
                if admission.encode().ok() != seed.admission().encode().ok() {
                    return Err(SeedFallback::new(
                        "stored admission and projection continuations disagree",
                    ));
                }
                Ok(seed)
            })
            .ok()
    };
    let next = if let Some(stored) = stored {
        let prior = stored.boundary();
        if prior == boundary {
            return Ok(());
        }
        if prior.seq < boundary.seq && prior.frame_end <= boundary.frame_off {
            // The row cursor may be ahead of the seed after an unqualified event.
            // Seed advancement must consume that entire interval again.
            let tail = positioned(read, prior.frame_end, boundary.frame_end)?;
            stored.advance(read, &boundary, &tail).ok()
        } else {
            SeedFallback::new("stored continuation ahead of projection boundary");
            None
        }
    } else {
        None
    };
    let next = match next {
        Some(next) => next,
        None => {
            SeedFallback::new("publication continuation rebuild");
            let prefix = positioned(read, 0, boundary.frame_end)?;
            match ProjectionContinuation::capture(read, &boundary, &prefix) {
                Ok(next) => next,
                Err(reason) => {
                    let unavailable = serde_json::json!({"version":1,"continuation":null,"fallback":reason.reason}).to_string();
                    tx.execute(
                        "UPDATE session SET tail_seed=?2,admission_seed=?2 WHERE sid=?1",
                        params![key, unavailable],
                    )?;
                    return Ok(());
                }
            }
        }
    };
    tx.execute(
        "UPDATE session SET tail_seed=?2,admission_seed=?3 WHERE sid=?1",
        params![
            key,
            next.encode().map_err(|e| DbError::Invalid(e.to_string()))?,
            next.admission()
                .encode()
                .map_err(|e| DbError::Invalid(e.to_string()))?
        ],
    )?;
    Ok(())
}
