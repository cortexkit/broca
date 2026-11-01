use super::*;
use broca_core::display_facts::{DisplayFacts, DisplayFactsCache};
use std::collections::BTreeMap;

impl LineageRegistry {
    /// Only an opt-in read calls this path. The first request backfills one lineage; later
    /// requests load their rows and fold only records beyond the atomically committed seed.
    pub(crate) async fn display_facts_for_page(
        &self,
        session: &LineageKey,
        lineage: &str,
        at_seq: u64,
        ordinals: Vec<u64>,
    ) -> Result<BTreeMap<u64, DisplayFacts>, OpenError> {
        let reader = self.inner.reader.as_ref().ok_or_else(|| {
            OpenError::Open("display facts have no durable history source".into())
        })?;
        let store = reader.store.clone();
        let mut cache = if let Some(store) = &store {
            let store = store.clone();
            let session = session.clone();
            let lineage = lineage.to_owned();
            let ordinals = ordinals.clone();
            spawn_blocking(JobKind::StoreRead, move || {
                store.read_display_facts_cache(&session, &lineage, at_seq, &ordinals)
            })
            .await
            .map_err(|e| OpenError::Open(e.to_string()))?
            .map_err(OpenError::Open)?
            .unwrap_or_default()
        } else {
            DisplayFactsCache::default()
        };
        if cache
            .seed
            .as_ref()
            .is_some_and(|seed| seed.through_seq >= at_seq)
        {
            return Ok(cache.rows);
        }
        let expected = cache.seed.as_ref().map(|seed| seed.through_seq);
        let mut seed = cache.seed.take().unwrap_or_default();
        let range = self
            .read_durable_range_for(
                reader,
                session,
                ReadRangeRequest {
                    after_seq: seed.through_seq,
                    anchor: seed
                        .anchor
                        .map(|(file_id, seq, byte_end)| broca_wal::WalByteAnchor {
                            file_id,
                            seq,
                            byte_end,
                        }),
                    live_head: Some(at_seq),
                    through_seq: Some(at_seq),
                    purpose: ReadPurpose::Session,
                },
            )
            .await?;
        if range.lineage_id.map(|id| id.to_string()).as_deref() != Some(lineage) {
            return Err(OpenError::Open(
                "display facts lineage changed during the read".into(),
            ));
        }
        let frames =
            crate::durable_read::DurableRead::classify(&range, seed.through_seq, Some(at_seq))
                .records()
                .map_err(|reason| OpenError::Classified {
                    reason,
                    class: broca_protocol::ErrorClass::Transient,
                })?
                .to_vec();
        let anchor = range
            .next_anchor
            .map(|anchor| (anchor.file_id, anchor.seq, anchor.byte_end));
        let source_lineage = lineage.to_owned();
        let (seed, versions) = spawn_blocking(JobKind::StoreRead, move || {
            let versions = seed.fold(&frames, &source_lineage)?;
            seed.anchor = anchor;
            Ok::<_, String>((seed, versions))
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        if seed.through_seq < at_seq {
            return Err(OpenError::Open(
                "display facts source does not cover the read snapshot".into(),
            ));
        }
        for version in &versions {
            if ordinals.contains(&version.ordinal) {
                cache.rows.insert(version.ordinal, version.facts.clone());
            }
        }
        if let Some(store) = store {
            let session = session.clone();
            let lineage = lineage.to_owned();
            spawn_blocking(JobKind::StoreRead, move || {
                store.publish_display_facts(&session, &lineage, expected, &seed, &versions)
            })
            .await
            .map_err(|e| OpenError::Open(e.to_string()))?
            .map_err(OpenError::Open)?;
        }
        Ok(cache.rows)
    }
}
