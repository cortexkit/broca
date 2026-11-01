use std::sync::{atomic::Ordering, Arc};

use broca_protocol::BindIdentity;
use broca_wal::{WalByteAnchor, WalRangeRecords};

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

fn check_lineage(writer: &WalHandle, expected: Option<&str>) -> Result<(), String> {
    if expected.is_some()
        && writer
            .lineage_id()
            .map(|lineage| lineage.to_string())
            .as_deref()
            != expected
    {
        return Err("media initialization lineage changed".into());
    }
    Ok(())
}

/// Reuse a coherent admission seed, or fold a verified same-lineage suffix into it.
/// Only an admission without a media seed needs every WAL record through the selected
/// durable head. The seed and byte anchor cover that same head; later appends belong to the tail reader.
pub async fn initialize_media_checkpoint<W: WalStore + ?Sized + 'static>(
    writer: &WalHandle,
    wal_store: Arc<W>,
    session: &BindIdentity,
    snapshot: &PriorWalSnapshot,
) -> Result<InitializedMedia, String> {
    let expected_lineage = snapshot.lineage_id.clone();
    check_lineage(writer, expected_lineage.as_deref())?;
    if !forward_head(writer, snapshot.head_seq)? {
        let anchor = snapshot.anchor.or_else(|| {
            wal_store
                .file_identity(session)
                .ok()
                .flatten()
                .zip(wal_store.live_wal_bytes(session).ok().flatten())
                .map(|(file_id, byte_end)| WalByteAnchor {
                    file_id,
                    byte_end,
                    seq: snapshot.head_seq,
                })
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
        check_lineage(writer, expected_lineage.as_deref())?;
        match cached {
            Ok(checkpoint) => return Ok(InitializedMedia { checkpoint, anchor }),
            Err(error) if !forward_head(writer, snapshot.head_seq)? => return Err(error),
            Err(_) => {}
        }
    }
    check_lineage(writer, expected_lineage.as_deref())?;
    let through = writer
        .sync()
        .await
        .map_err(|error| format!("media initialization barrier: {error}"))?;
    forward_head(writer, snapshot.head_seq)?;
    if through <= snapshot.head_seq {
        return Err("media initialization advance did not move forwards".into());
    }
    check_lineage(writer, expected_lineage.as_deref())?;
    let lineage = match expected_lineage {
        Some(lineage) => lineage,
        None if snapshot.head_seq == 0 => writer
            .lineage_id()
            .map(|lineage| lineage.to_string())
            .ok_or("media initialization advance has no known lineage")?,
        None => return Err("media initialization advance has no known lineage".into()),
    };
    let session = session.clone();
    let ready = match &snapshot.checkpoint_at_head.media {
        Some(DurableMediaSeed::Ready(seed)) if seed.at_seq == snapshot.head_seq => {
            let mut seed = (**seed).clone();
            seed.resume_budget(
                snapshot
                    .checkpoint_at_head
                    .last_episode
                    .as_ref()
                    .and_then(|episode| episode.config.budget.clone()),
            );
            Some(seed)
        }
        Some(_) => {
            return Err(
                "media initialization admission seed is unreadable or has the wrong watermark"
                    .into(),
            )
        }
        None => None,
    };
    let after = ready.as_ref().map_or(0, |seed| seed.at_seq);
    let prior_anchor = snapshot.anchor;
    let verified_lineage = lineage.clone();
    let initialized = tokio::task::spawn_blocking(move || -> Result<InitializedMedia, String> {
        let range = if ready.is_some() {
            wal_store.read_media_seed_suffix(&session, after, through, prior_anchor.as_ref())
        } else {
            wal_store.read_records_range(&session, 0, Some(through), None)
        }
        .map_err(|error| format!("media initialization advance is unreadable: {error}"))?;
        if range
            .lineage_id
            .map(|lineage| lineage.to_string())
            .as_deref()
            != Some(lineage.as_str())
        {
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
            WalRangeRecords::Partial(records) if ready.is_some() => records,
            _ => return Err("media initialization advance is not a complete prefix".into()),
        };
        if records.last().map(|frame| frame.seq) != Some(through)
            || records
                .iter()
                .enumerate()
                .any(|(index, frame)| frame.seq != after + index as u64 + 1)
        {
            return Err("media initialization advance does not reach its contiguous head".into());
        }
        if let Some(mut checkpoint) = ready {
            for frame in &records {
                checkpoint.fold(frame.seq, &frame.record)?;
            }
            return Ok(InitializedMedia {
                checkpoint,
                anchor: Some(anchor),
            });
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
    check_lineage(writer, Some(&verified_lineage))?;
    forward_head(writer, through)?;
    Ok(initialized)
}

#[cfg(test)]
#[path = "initialization_tests.rs"]
mod tests;
