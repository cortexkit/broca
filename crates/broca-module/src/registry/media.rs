use super::*;
use broca_core::AdmissionCheckpointV1;
use broca_protocol::MediaRef;

fn replay_required(reason: &str) -> Option<AdmissionCheckpointV1> {
    broca_core::media_request::count_seed_fallback(reason);
    None
}

impl LineageRegistry {
    /// Resolve capture provenance without decoding the materialized transcript on every chunk.
    pub(crate) async fn captured_media_reference(
        &self,
        key: &LineageKey,
        sha256: &str,
    ) -> Result<Option<MediaRef>, OpenError> {
        self.check_expired(key, None).await?;
        let checkpoint = match self.media_checkpoint_at_head(key).await? {
            Some(checkpoint) => checkpoint,
            None => {
                let (frames, _) = self.read_durable_wal(key).await?;
                AdmissionCheckpointV1::from_frames(&frames).map_err(|error| {
                    OpenError::Open(format!("read session media history: {error}"))
                })?
            }
        };
        Ok(checkpoint.captured_media_reference(sha256).cloned())
    }

    async fn media_checkpoint_at_head(
        &self,
        key: &LineageKey,
    ) -> Result<Option<AdmissionCheckpointV1>, OpenError> {
        let reader =
            self.inner.reader.as_ref().ok_or_else(|| {
                OpenError::Open("session.media_read reader is not configured".into())
            })?;
        let Some(store) = reader.store.clone() else {
            return Ok(replay_required("media_checkpoint_missing"));
        };
        let session = key.clone();
        let snapshot = spawn_blocking(JobKind::StoreRead, move || {
            store.read_admission_snapshot(&session)
        })
        .await;
        let snapshot = match snapshot {
            Ok(Ok(Some(snapshot))) => snapshot,
            Ok(Ok(None)) => return Ok(replay_required("media_checkpoint_missing")),
            _ => return Ok(replay_required("media_checkpoint_unavailable")),
        };
        let Some(mut checkpoint) = snapshot.checkpoint else {
            return Ok(replay_required("media_checkpoint_missing"));
        };
        if checkpoint.at_seq != snapshot.watermark_seq {
            return Ok(replay_required("media_seed_checkpoint_watermark_mismatch"));
        }
        if !checkpoint.media_seed_complete() {
            // The shared seed validator already records the precise missing/invalid-seed reason.
            return Ok(None);
        }
        if checkpoint.compaction.is_none() || !checkpoint.compaction_complete() {
            return Ok(replay_required("media_seed_capture_index_incomplete"));
        }
        let live_head = match self.inner.slots.lock().unwrap().get(key) {
            Some(Entry::Open { handle, .. }) => Some(handle.durable_head.load(Ordering::Acquire)),
            _ => None,
        };
        if live_head.is_some_and(|head| snapshot.watermark_seq > head) {
            return Ok(replay_required("media_seed_checkpoint_ahead"));
        }
        if snapshot.anchor.is_none() {
            return Ok(replay_required("media_seed_anchor_missing"));
        }
        let range = self
            .read_durable_range_for(
                reader,
                key,
                ReadRangeRequest {
                    after_seq: snapshot.watermark_seq,
                    anchor: snapshot.anchor,
                    live_head,
                    through_seq: None,
                    purpose: ReadPurpose::Media,
                },
            )
            .await?;
        let records = match crate::durable_read::DurableRead::classify(
            &range,
            snapshot.watermark_seq,
            live_head,
        )
        .records()
        {
            Ok(records) => records,
            Err(_) => return Ok(replay_required("media_seed_tail_unproven")),
        };
        if snapshot.lineage_id.as_deref() != range.lineage_id.map(|id| id.to_string()).as_deref()
            || snapshot.lineage_id.is_none()
        {
            return Ok(replay_required("media_seed_lineage_mismatch"));
        }
        if checkpoint.fold_frames(records).is_err() {
            return Ok(replay_required("media_seed_tail_invalid"));
        }
        if !checkpoint.media_seed_complete() {
            return Ok(None);
        }
        if !checkpoint.compaction_complete() {
            return Ok(replay_required("media_seed_capture_index_incomplete"));
        }
        Ok(Some(checkpoint))
    }
}
