//! Session-local, rebuildable continuation forms. Row indexing and seed advancement have
//! independent cursors. Rows follow every publication. Seeds stop at a completed tool
//! turn, or at a no-tool step/zero-step run whose episode has a durable RunFinished.

use broca_core::replay::{PositionedFrame, SeedBoundary, SeedFallback};
use broca_core::{admission::AdmissionContinuation, projection_plan::ProjectionContinuation};
use broca_wal::framing::DecodeStep;
use rusqlite::{params, OptionalExtension, Transaction};

use super::DbError;

fn positioned(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    start: u64,
    end: u64,
) -> Result<Vec<PositionedFrame>, DbError> {
    let bytes = broca_wal::access_counter::read_at(read_at, start, end - start)?;
    let mut at = 0;
    let mut frames = Vec::new();
    while at < bytes.len() {
        match broca_wal::access_counter::decode_next(&bytes[at..]) {
            DecodeStep::Record { framed, consumed } => {
                frames.push(PositionedFrame {
                    off: start + at as u64,
                    framed: *framed,
                });
                at += consumed;
            }
            DecodeStep::Lineage { consumed, .. } => at += consumed,
            _ => {
                return Err(DbError::Invalid(
                    "continuation source range is not a complete WAL prefix".into(),
                ))
            }
        }
    }
    Ok(frames)
}

pub(super) fn publish(tx: &Transaction<'_>, key: &str, bytes: &[u8]) -> Result<(), DbError> {
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
    let read_at = |off: u64, len: u64| {
        let start = usize::try_from(off).unwrap_or(usize::MAX).min(bytes.len());
        let end = off.saturating_add(len).min(bytes.len() as u64) as usize;
        Ok(bytes[start..end.max(start)].to_vec())
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
            let tail = positioned(&read_at, prior.frame_end, boundary.frame_end)?;
            stored.advance(&read_at, &boundary, &tail).ok()
        } else {
            SeedFallback::new("stored continuation ahead of projection boundary");
            None
        }
    } else {
        None
    };
    let next = match next {
        Some(seed) => seed,
        None => {
            let prefix = positioned(&read_at, 0, boundary.frame_end)?;
            match ProjectionContinuation::capture(&read_at, &boundary, &prefix) {
                Ok(seed) => seed,
                Err(reason) => {
                    // Compaction replacements, pending steers and open tool turns cannot use
                    // this continuation. Keep a named full-WAL-replay cache miss without
                    // discarding indexed rows or touching authoritative state.
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
