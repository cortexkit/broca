use std::sync::{atomic::Ordering, Arc};

use broca_protocol::BindIdentity;
use broca_wal::{WalByteAnchor, WalLineageId, WalRangeRecords};

use super::{DurableMediaSeed, MediaCheckpoint};
use crate::{placement::Placement, run::PriorWalSnapshot, wal_writer::WalStore, WalHandle};

#[derive(Debug)]
pub struct InitializedMedia {
    pub checkpoint: MediaCheckpoint,
    pub anchor: Option<WalByteAnchor>,
}

fn forward_head(writer: &WalHandle, selected: u64) -> Result<bool, String> {
    let next = selected
        .checked_add(1)
        .ok_or("media initialization sequence exhausted")?;
    let durable = writer.durable_head().load(Ordering::Acquire);
    let reserved = writer.next_record_seq();
    if durable < selected || reserved < next {
        return Err("media initialization head moved backwards".into());
    }
    Ok(durable > selected || reserved > next)
}

fn check_lineage(writer: &WalHandle, expected: Option<WalLineageId>) -> Result<(), String> {
    if expected.is_some() && writer.lineage_id() != expected {
        return Err("media initialization lineage changed".into());
    }
    Ok(())
}

/// Reuse a coherent admission seed, or rebuild a verified forward prefix on the same
/// writer lineage. The byte anchor always comes from the prefix selected for that seed;
/// later appends are consumed by the ordinary strictly anchored media-tail reader.
pub async fn initialize_media_checkpoint<W: WalStore + ?Sized + 'static>(
    writer: &WalHandle,
    wal_store: Arc<W>,
    session: &BindIdentity,
    snapshot: &PriorWalSnapshot,
) -> Result<InitializedMedia, String> {
    let expected_lineage = writer.lineage_id();
    if !forward_head(writer, snapshot.head_seq)? {
        let anchor = wal_store
            .file_identity(session)
            .ok()
            .flatten()
            .zip(wal_store.live_wal_bytes(session).ok().flatten())
            .map(|(file_id, byte_end)| WalByteAnchor {
                file_id,
                byte_end,
                seq: snapshot.head_seq,
            });
        let cached = match &snapshot.checkpoint_at_head.media {
            Some(DurableMediaSeed::Ready(seed)) if seed.at_seq == snapshot.head_seq => writer
                .validate_media_seed_head(snapshot.head_seq)
                .map(|()| (**seed).clone()),
            Some(DurableMediaSeed::Ready(_)) => {
                Err("media seed watermark does not match admission".into())
            }
            Some(DurableMediaSeed::Unreadable(_)) => {
                Err("media_seed_undecodable after admission replay".into())
            }
            None => writer.empty_media_checkpoint(snapshot.head_seq),
        };
        check_lineage(writer, expected_lineage)?;
        match cached {
            Ok(checkpoint) => return Ok(InitializedMedia { checkpoint, anchor }),
            Err(error) if !forward_head(writer, snapshot.head_seq)? => return Err(error),
            Err(_) => {}
        }
    }
    check_lineage(writer, expected_lineage)?;
    let through = writer
        .sync()
        .await
        .map_err(|error| format!("media initialization barrier: {error}"))?;
    forward_head(writer, snapshot.head_seq)?;
    if through <= snapshot.head_seq {
        return Err("media initialization advance did not move forwards".into());
    }
    check_lineage(writer, expected_lineage)?;
    let lineage = match expected_lineage {
        Some(lineage) => lineage,
        None if snapshot.head_seq == 0 => writer
            .lineage_id()
            .ok_or("media initialization advance has no known lineage")?,
        None => return Err("media initialization advance has no known lineage".into()),
    };
    let session = session.clone();
    let initialized = tokio::task::spawn_blocking(move || -> Result<InitializedMedia, String> {
        let range = wal_store
            .read_records_range(&session, 0, Some(through), None)
            .map_err(|error| format!("media initialization advance is unreadable: {error}"))?;
        if range.lineage_id != Some(lineage) {
            return Err("media initialization advance changed lineage".into());
        }
        let anchor = range
            .next_anchor
            .ok_or("media initialization advance has no byte anchor")?;
        if anchor.seq != through {
            return Err("media initialization anchor does not match its head".into());
        }
        let records = match range.into_records() {
            WalRangeRecords::Complete(records) => records,
            _ => return Err("media initialization advance is not a complete prefix".into()),
        };
        if records.last().map(|frame| frame.seq) != Some(through)
            || records
                .iter()
                .enumerate()
                .any(|(index, frame)| frame.seq != index as u64 + 1)
        {
            return Err("media initialization advance does not reach its contiguous head".into());
        }
        let admission = crate::AdmissionCheckpointV1::from_frames(&records)
            .map_err(|error| format!("media initialization replay failed: {error}"))?;
        let checkpoint = match admission.media {
            Some(DurableMediaSeed::Ready(seed)) if seed.at_seq == through => *seed,
            Some(_) => return Err("media initialization replay has an unreadable seed".into()),
            None => {
                let mut placement = Placement::default();
                for frame in &records {
                    placement
                        .observe(&frame.record)
                        .map_err(|error| format!("media initialization placement: {error}"))?;
                }
                MediaCheckpoint::empty_at(placement, through)
            }
        };
        Ok(InitializedMedia {
            checkpoint,
            anchor: Some(anchor),
        })
    })
    .await
    .map_err(|error| format!("media initialization replay task failed: {error}"))??;
    check_lineage(writer, Some(lineage))?;
    forward_head(writer, through)?;
    Ok(initialized)
}

#[cfg(test)]
#[path = "initialization_tests.rs"]
mod tests;
