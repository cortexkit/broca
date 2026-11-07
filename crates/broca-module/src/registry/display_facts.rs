use super::*;
use broca_core::display_facts::{DisplayFacts, DisplayFactsCache};
use std::collections::BTreeMap;

impl LineageRegistry {
    /// Application reads use this sidecar without changing model-facing rows. The first request backfills one lineage; later
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
        if cache.seed.as_ref().is_some_and(|seed| {
            seed.version == broca_core::display_facts::DISPLAY_FACTS_VERSION
                && seed.through_seq >= at_seq
        }) {
            resolve_page_notices(&mut cache.rows, &cache.seed.as_ref().unwrap().send_keys)?;
            return Ok(cache.rows);
        }
        let expected = cache.seed.as_ref().map(|seed| seed.through_seq);
        let expected_epoch = cache.publication_epoch;
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
        let whole_source = seed.through_seq == 0;
        let (seed, versions, dispatches) = spawn_blocking(JobKind::StoreRead, move || {
            let versions = seed.fold(&frames, &source_lineage)?;
            seed.anchor = anchor;
            // Reuse this proven full decode instead of rereading it for default call attribution.
            let dispatches = if whole_source {
                let facts =
                    broca_core::tool_call_dispatches(&frames).map_err(|error| error.to_string())?;
                // Display facts can mention an uncommitted call; only committed turns prove coverage.
                let covered = facts
                    .last_key_value()
                    .map_or(0, |(ordinal, _)| ordinal.saturating_add(1));
                Some((covered, facts))
            } else {
                None
            };
            Ok::<_, String>((seed, versions, dispatches))
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))?
        .map_err(OpenError::Open)?;
        if seed.through_seq < at_seq {
            return Err(OpenError::Open(
                "display facts source does not cover the read snapshot".into(),
            ));
        }
        if let Some((covered, dispatches)) = dispatches {
            self.cache_tool_dispatches(session, lineage, covered, dispatches);
        }
        for version in &versions {
            if ordinals.contains(&version.ordinal) {
                cache.rows.insert(version.ordinal, version.facts.clone());
            }
        }
        resolve_page_notices(&mut cache.rows, &seed.send_keys)?;
        if let Some(store) = store {
            let session = session.clone();
            let lineage = lineage.to_owned();
            spawn_blocking(JobKind::StoreRead, move || {
                store.publish_display_facts(
                    &session,
                    &lineage,
                    expected,
                    expected_epoch,
                    &seed,
                    &versions,
                )
            })
            .await
            .map_err(|e| OpenError::Open(e.to_string()))?
            .map_err(OpenError::Open)?;
        }
        Ok(cache.rows)
    }
}

fn resolve_page_notices(
    rows: &mut BTreeMap<u64, DisplayFacts>,
    keys: &broca_core::notice_keys::NoticeSendKeys,
) -> Result<(), OpenError> {
    for facts in rows.values_mut() {
        facts
            .resolve_notices(keys)
            .map_err(|reason| OpenError::Classified {
                reason,
                class: broca_protocol::ErrorClass::Transient,
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn a_notice_hash_collision_refuses_the_page_with_a_named_transient_error() {
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&("sender", "send")).unwrap())
        );
        let keys = serde_json::from_value(serde_json::json!({
            "through": 1, "complete": true, "ordered_senders": {}, "submissions": {},
            "client_keys": {(hash): ["other", "different"]}
        }))
        .unwrap();
        let mut rows = BTreeMap::from([(
            0,
            DisplayFacts {
                notices: vec![broca_core::projection::ProjectedNotice {
                    submission_id: "send".into(),
                    fact: cortexkit_role_llm_runner::read::NoticeFact::new(
                        0, "send", "sender", "notice",
                    )
                    .with_call_key("call"),
                }],
                ..Default::default()
            },
        )]);
        assert!(matches!(
            resolve_page_notices(&mut rows, &keys),
            Err(OpenError::Classified { reason, class: broca_protocol::ErrorClass::Transient })
                if reason.starts_with("notice_send_key_hash_collision:")
        ));
    }
}
