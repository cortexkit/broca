use super::*;
use crate::context::{HeldEpisode, HeldPrefix, HeldRowsRequest, StorePageSnapshot};

/// The store answers only held-prefix and bounded page reads. A full-prefix fallback is a bug
/// in this fixture: the original image is deliberately outside the held set.
struct MediaResumeStore {
    replay: crate::ReplayState,
    snapshot: crate::AdmissionStoreSnapshot,
    episodes: Vec<crate::context::EpisodeRenderProjection>,
    held_reads: AtomicUsize,
}

impl MediaResumeStore {
    fn new(shared: &Shared, watermark: usize) -> Self {
        let frames = shared.frames();
        let records = shared.records();
        let checkpoint = crate::AdmissionCheckpointV1::from_frames(&frames[..watermark]).unwrap();
        // Round-trip the actual projection bytes, rather than sharing an in-memory index.
        let checkpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();
        Self {
            replay: crate::replay(&records[..watermark]).unwrap(),
            snapshot: crate::AdmissionStoreSnapshot {
                watermark_seq: watermark as u64,
                lineage_id: Some(fixture_lineage_id().to_string()),
                anchor: None,
                checkpoint: Some(checkpoint),
            },
            episodes: PagedStore::from_wal(&records[..watermark]).episodes,
            held_reads: AtomicUsize::new(0),
        }
    }
}

impl crate::MaterializedStore for MediaResumeStore {
    fn read_admission_snapshot(
        &self,
        _: &BindIdentity,
    ) -> Result<Option<crate::AdmissionStoreSnapshot>, String> {
        Ok(Some(self.snapshot.clone()))
    }

    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<crate::StorePrefix, String> {
        Err("full-prefix reads are forbidden in the sparse media fixture".into())
    }

    fn read_held_prefix(
        &self,
        _: &BindIdentity,
        request: &HeldRowsRequest,
    ) -> Result<Option<HeldPrefix>, String> {
        self.held_reads.fetch_add(1, Ordering::SeqCst);
        let rows = &self.replay.prompt;
        let last = rows.last().and_then(|message| message.ordinal);
        let held: Vec<_> = rows
            .iter()
            .filter(|message| {
                message.role == Role::System
                    || message
                        .ordinal
                        .is_some_and(|ordinal| request.holds(ordinal) || Some(ordinal) == last)
            })
            .cloned()
            .collect();
        assert!(
            held.iter()
                .all(|message| super::super::super::media_refs(&message.content)
                    .next()
                    .is_none()),
            "the original image must not be returned in the held rows"
        );
        Ok(Some(HeldPrefix {
            lineage_id: self.snapshot.lineage_id.clone(),
            watermark_seq: self.snapshot.watermark_seq,
            prefix_len: self.replay.extent,
            context_epoch: 0,
            completed_steps: self.replay.completed_steps,
            usage: self.replay.usage,
            retries_used: self.replay.retries_used,
            last_step_usage: self.replay.last_step_usage,
            last_step_finish_reason: self.replay.last_step_finish_reason,
            rows: held,
            trailing_origin: rows.iter().rev().find_map(|message| message.origin.clone()),
            tag_seed: crate::tags::TagSeed::from_prefix(rows),
            episodes: Some(
                self.episodes
                    .iter()
                    .cloned()
                    .map(|projection| HeldEpisode {
                        projection,
                        next_start: None,
                    })
                    .collect(),
            ),
        }))
    }

    fn read_page_with_watermark(
        &self,
        _: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        _: crate::context::ReadView,
    ) -> Result<StorePageSnapshot, String> {
        let from = from.unwrap_or(0);
        Ok(StorePageSnapshot {
            lineage_id: self.snapshot.lineage_id.clone(),
            watermark_seq: self.snapshot.watermark_seq,
            prefix_len: self.replay.extent,
            rows: self
                .replay
                .prompt
                .iter()
                .filter(|message| {
                    message
                        .ordinal
                        .is_some_and(|ordinal| from <= ordinal && ordinal < from + limit)
                })
                .map(|message| crate::context::StorePageRow {
                    ordinal: message.ordinal.unwrap(),
                    written_at: None,
                    message: message.clone(),
                    run: None,
                })
                .collect(),
            ..StorePageSnapshot::empty()
        })
    }
}

impl crate::Persister for MediaResumeStore {
    fn project(&self, _: &BindIdentity, _: &[FramedRecord]) -> Result<u64, String> {
        Ok(self.snapshot.watermark_seq)
    }
}

#[tokio::test]
async fn captured_media_survives_every_checkpoint_cut_and_sparse_resume() {
    let scenario = || EpisodeScenario {
        media: true,
        budget: false,
        ..EpisodeScenario::fresh(Later::Same)
    };
    let live = Shared::default();
    let (expected, _) = run_configured(&live, false, false, scripts(0), None, scenario()).await;
    assert!(expected
        .iter()
        .all(|request| request
            .prompt
            .iter()
            .any(|message| crate::compaction::media_refs(&message.content)
                .next()
                .is_some())));
    let records = live.records();
    let watermark = records
        .iter()
        .position(|record| matches!(record, WalRecord::TurnFinished { .. }))
        .unwrap()
        + 1;
    let mut sparse_sends = 0;
    // Keep the store prefix at a complete turn, and crash after each later WAL record,
    // including tool calls whose completion is not yet known.
    for cut in watermark..=records.len() {
        let durable = records[..cut].to_vec();
        let from = crate::replay(&durable).unwrap().completed_steps;
        let full = Shared::with_records(durable.clone());
        let (canonical, _) =
            run_configured(&full, true, false, scripts(from), None, scenario()).await;
        let resumed = Shared::with_records(durable);
        let store = Arc::new(MediaResumeStore::new(&resumed, watermark));
        let (calls, _) = run_configured(
            &resumed,
            false,
            false,
            scripts(from),
            Some(store.clone()),
            scenario(),
        )
        .await;
        assert_eq!(calls.len(), canonical.len(), "WAL cut {cut}");
        if !calls.is_empty() {
            assert!(
                store.held_reads.load(Ordering::SeqCst) > 0,
                "WAL cut {cut} never loaded held rows"
            );
            sparse_sends += calls.len();
        }
        for (actual, canonical) in calls.iter().zip(&canonical) {
            assert_request_bytes(actual, canonical, cut);
            let index = actual.step_context.as_ref().unwrap().step_index as usize - 1;
            assert_request_bytes(actual, &expected[index], cut);
        }
    }
    assert!(sparse_sends > 0);
    println!(
        "{} WAL record cuts, {sparse_sends} sparse resumed sends",
        records.len() - watermark + 1
    );
}

fn assert_request_bytes(actual: &CallOptions, expected: &CallOptions, cut: usize) {
    let actual = crate::canonical_request_bytes(actual);
    let expected = crate::canonical_request_bytes(expected);
    assert!(
        actual == expected,
        "WAL cut {cut}\nactual:\n{}\nexpected:\n{}",
        serde_json::to_string_pretty(&serde_json::from_slice::<Value>(&actual).unwrap()).unwrap(),
        serde_json::to_string_pretty(&serde_json::from_slice::<Value>(&expected).unwrap()).unwrap()
    );
}
