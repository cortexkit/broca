//! Session-local, rebuildable continuation forms. Row indexing and seed advancement have
//! independent cursors. Rows follow every publication. Seeds stop at a completed tool
//! turn, or at a no-tool step/zero-step run whose episode has a durable RunFinished.

use broca_core::replay::{PositionedFrame, SeedBoundary, SeedFallback};
use broca_core::{admission::AdmissionContinuation, projection_plan::ProjectionContinuation};
use broca_wal::framing::DecodeStep;
use rusqlite::{params, OptionalExtension, Transaction};

use super::{DbError, OverhaulDb};

impl OverhaulDb {
    /// Build an opt-in admission capability around this database and the writer's synced
    /// publication witness. Boot composition continues to select the legacy reader.
    pub fn admission_reader(
        self: &std::sync::Arc<Self>,
        publication: impl Fn(
                &broca_protocol::BindIdentity,
            ) -> Result<broca_wal::file_wal::DurablePublication, String>
            + Send
            + Sync
            + 'static,
    ) -> std::sync::Arc<dyn broca_core::context::AdmissionReader> {
        std::sync::Arc::new(DatabaseAdmissionReader {
            db: self.clone(),
            publication: Box::new(publication),
        })
    }

    fn admission_continuation(
        &self,
        bind: &broca_protocol::BindIdentity,
        publication: &broca_wal::file_wal::DurablePublication,
    ) -> Result<AdmissionContinuation, DbError> {
        self.read_snapshot(|tx| {
            let row: Option<(String, Option<String>, u64)> = tx.query_row(
                "SELECT admission_seed,lineage_id,indexed_seq FROM session WHERE sid=?1",
                [super::index::sid(bind)?],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            ).optional()?;
            let Some((encoded, lineage, head)) = row else {
                return Err(DbError::SeedFallback(SeedFallback::new("admission session index missing")));
            };
            let seed = AdmissionContinuation::decode(&encoded).map_err(DbError::SeedFallback)?;
            let boundary = seed.boundary();
            let stored = tx.query_row(
                "SELECT lineage_id,seq,frame_off,frame_end,frame_identity,qualified FROM projection_boundary WHERE sid=?1 AND seq=?2",
                params![super::index::sid(bind)?, boundary.seq],
                |r| Ok(SeedBoundary { lineage_id: r.get(0)?, seq: r.get(1)?, frame_off: r.get(2)?, frame_end: r.get(3)?, frame_identity: r.get(4)?, qualified: r.get(5)? }),
            ).optional()?;
            if stored.as_ref() != Some(&boundary) || !boundary.qualified
                || boundary.seq > head || head > publication.sequence()
                || boundary.frame_end > publication.byte_end()
                || lineage.as_deref() != Some(publication.lineage().to_string().as_str())
                || boundary.lineage_id != lineage
            {
                return Err(DbError::SeedFallback(SeedFallback::new("admission seed and published boundary mismatch")));
            }
            Ok(seed)
        })
    }
}

type PublicationSource = dyn Fn(&broca_protocol::BindIdentity) -> Result<broca_wal::file_wal::DurablePublication, String>
    + Send
    + Sync;

struct DatabaseAdmissionReader {
    db: std::sync::Arc<OverhaulDb>,
    publication: Box<PublicationSource>,
}

impl DatabaseAdmissionReader {
    fn read<T>(
        &self,
        session: &broca_protocol::BindIdentity,
        safe_head: u64,
        work: impl FnOnce(
            &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
            Result<AdmissionContinuation, DbError>,
            u64,
            &str,
        ) -> Result<T, String>,
    ) -> Result<T, String> {
        use std::io::{Read, Seek, SeekFrom};
        let publication = (self.publication)(session)?;
        if publication.sequence() != safe_head {
            return Err(format!(
                "admission publication does not name durable head {safe_head}"
            ));
        }
        let file = std::cell::RefCell::new(
            broca_wal::fs_ops::File::open(publication.path()).map_err(|e| e.to_string())?,
        );
        let read_at = |off: u64, len: u64| {
            let mut file = file.borrow_mut();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            (&mut *file)
                .take(len.min(publication.byte_end().saturating_sub(off)))
                .read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        let seed = self.db.admission_continuation(session, &publication);
        work(
            &read_at,
            seed,
            publication.byte_end(),
            &publication.lineage().to_string(),
        )
    }
}

fn admission_tail(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    seed: &AdmissionContinuation,
    head: u64,
    end: u64,
) -> Result<Vec<PositionedFrame>, DbError> {
    if seed.boundary().seq == head {
        Ok(Vec::new())
    } else {
        positioned(
            read_at,
            seed.boundary().frame_end,
            end,
            seed.boundary().lineage_id.as_deref(),
        )
    }
}

fn full_admission(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    head: u64,
    end: u64,
    lineage: &str,
) -> Result<broca_core::run::PriorWalSnapshot, String> {
    let frames: Vec<_> = positioned(read_at, 0, end, Some(lineage))
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|p| p.framed)
        .collect();
    if frames.last().map_or(0, |f| f.seq) != head
        || frames
            .iter()
            .enumerate()
            .any(|(i, frame)| frame.seq != i as u64 + 1)
    {
        return Err(format!(
            "admission repair did not reach durable head {head}"
        ));
    }
    broca_core::replay::replay(&frames.iter().map(|f| f.record.clone()).collect::<Vec<_>>())
        .map_err(|e| e.to_string())?;
    let checkpoint_at_head =
        broca_core::AdmissionCheckpointV1::from_frames(&frames).map_err(|e| e.to_string())?;
    Ok(broca_core::run::PriorWalSnapshot {
        base_watermark: 0,
        head_seq: head,
        tail: std::sync::Arc::new(frames),
        checkpoint_at_base: None,
        checkpoint_at_head,
    })
}

fn count_admission_error(error: &DbError) {
    if !matches!(error, DbError::SeedFallback(_)) {
        SeedFallback::new("admission database or tail unavailable");
    }
}

impl broca_core::context::AdmissionReader for DatabaseAdmissionReader {
    fn prior_snapshot(
        &self,
        session: &broca_protocol::BindIdentity,
        safe_head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        self.read(session, safe_head, |read_at, seed, end, lineage| {
            let seeded = seed.and_then(|seed| {
                let tail = admission_tail(read_at, &seed, safe_head, end)?;
                Ok((seed, tail))
            });
            match seeded {
                Ok((seed, tail)) => broca_core::run::load_prior_wal_snapshot_from_seed(
                    read_at,
                    &seed,
                    &tail,
                    safe_head,
                    || {
                        Ok(full_admission(read_at, safe_head, end, lineage)?
                            .tail
                            .as_ref()
                            .clone())
                    },
                ),
                Err(error) => {
                    count_admission_error(&error);
                    full_admission(read_at, safe_head, end, lineage)
                }
            }
        })
    }

    fn paused_episode(
        &self,
        session: &broca_protocol::BindIdentity,
        safe_head: u64,
    ) -> Result<Option<broca_core::admission::LastEpisodeV1>, String> {
        self.read(session, safe_head, |read_at, seed, end, lineage| {
            let seeded = seed.and_then(|seed| {
                let tail = admission_tail(read_at, &seed, safe_head, end)?;
                seed.paused_episode(read_at, &tail, safe_head)
                    .map_err(DbError::SeedFallback)
            });
            match seeded {
                Ok(episode) => Ok(episode),
                Err(error) => {
                    count_admission_error(&error);
                    Ok(full_admission(read_at, safe_head, end, lineage)?
                        .checkpoint_at_head
                        .last_episode
                        .filter(|e| {
                            e.disposition == broca_core::admission::EpisodeDisposition::Paused
                        }))
                }
            }
        })
    }
}

fn positioned(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    start: u64,
    end: u64,
    lineage: Option<&str>,
) -> Result<Vec<PositionedFrame>, DbError> {
    if start > end {
        return Err(DbError::Invalid(
            "admission tail is ahead of durable end".into(),
        ));
    }
    let bytes = broca_wal::access_counter::read_at(read_at, start, end - start)?;
    if bytes.len() as u64 != end - start {
        return Err(DbError::Invalid(
            "admission tail is shorter than durable end".into(),
        ));
    }
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
            DecodeStep::Lineage {
                consumed,
                lineage_id,
            } if lineage.is_none_or(|expected| lineage_id.to_string() == expected) => {
                at += consumed
            }
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
            let tail = positioned(&read_at, prior.frame_end, boundary.frame_end, None)?;
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
            let prefix = positioned(&read_at, 0, boundary.frame_end, None)?;
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
